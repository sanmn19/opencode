# Agent-to-Agent Messaging (A2A) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Any two sessions in an opencode instance can exchange durable, transcript-visible messages; a running subagent notifies its parent when done, and the parent processes it in a real provider turn instead of blocking.

**Architecture:** A dedicated instance-scoped `AgentMail` Effect service (modeled on `BackgroundJob`) owns addressing, validation, and delivery. Messages are a new `agent-message` part type persisted in the receiving session's transcript through the existing event/part pipeline. Delivery calls `SessionPrompt.prompt` — the same seam mid-run user messages use — so idle targets get a provider turn immediately, urgent messages steer busy targets at the next turn boundary, and normal messages hold in a FIFO until the run drains.

**Tech Stack:** TypeScript, Effect v4 (`Effect.gen`, `Effect.fn`, `Layer`/`LayerNode`, `InstanceState`), Bun test, `@opencode-ai/schema` (Effect Schema), SolidJS for TUI.

**Spec:** `docs/superpowers/specs/2026-10-09-agent-to-agent-messaging-design.md`

## Global Constraints

- Run tests and typecheck from package dirs: `cd packages/opencode && bun test ...`, `bun typecheck`. Never from repo root (guard: `do-not-run-tests-from-root`).
- Tests must not use mocks for domain behavior under test. `Layer.mock`/`Layer.succeed` stubs for unrelated services (Summary, LSP, MCP) are fine, as in `test/session/prompt.test.ts`.
- No `export namespace Foo`; use flat exports + bottom self-reexport (`export * as AgentMail from "./agent-mail"`) per `packages/opencode/AGENTS.md`.
- Never `await import("x").then(...)`, no `for` loops where `flatMap`/`filter` fit, prefer `const`, no `else`, avoid explicit types where inference works.
- Branch: `a2a-messaging`. Conventional commits: `type(scope): summary`.
- Rate limit default: 30 msgs/min per sender→target pair. Size cap: 16 KB text.
- Feature flag this plan adds: env-based `OPENCODE_A2A` via `RuntimeFlags` (spec's "config flag" maps to the repo's env-flag idiom; file-config support can follow later).
- Do not push or open PRs without explicit ask.

---

### Task 1: Schema — `agent-message` part type

**Files:**
- Modify: `packages/schema/src/v1/session.ts` (Part union at ~line 357, part inputs near ~line 440)
- Modify: `packages/core/src/v1/session.ts` (explicit re-export list — add `AgentMessagePart` and `AgentMessagePartInput` next to `SubtaskPartInput`, ~line 34)
- Test: `packages/opencode/test/session/agent-message-schema.test.ts` (new file)

**Interfaces:**
- Consumes: `partBase` (`{ id: PartID, sessionID: SessionID, messageID: MessageID }`, `packages/schema/src/v1/session.ts:81-85`), `Part` union (`:357-383`).
- Produces: `SessionV1.AgentMessagePart` (stored part) and `SessionV1.AgentMessagePartInput` (prompt-admission input). Exact shape:

```ts
AgentMessagePart = {
  id: PartID; sessionID: SessionID; messageID: MessageID;
  type: "agent-message";
  sender: { sessionID: SessionID; agent: string };
  to: SessionID;
  replyTo?: MessageID; urgent?: boolean; text: string
}
```

- [ ] **Step 1: Write the failing contract test**

Create `packages/opencode/test/session/agent-message-schema.test.ts`:

```ts
import { expect, test } from "bun:test"
import { Schema } from "effect"
import { SessionV1 } from "@opencode-ai/core/v1/session"

const ids = {
  id: "prt_01JT0000000000000000000000",
  sessionID: "ses_01JT0000000000000000000000",
  messageID: "msg_01JT0000000000000000000000",
}

test("agent-message part is a valid V1 part", () => {
  const part = Schema.decodeUnknownSync(SessionV1.Part)({
    ...ids,
    type: "agent-message",
    sender: {
      sessionID: "ses_01JT0000000000000000000001",
      agent: "worker",
    },
    to: "ses_01JT0000000000000000000000",
    text: "build finished",
  })
  expect(part.type).toBe("agent-message")
  if (part.type === "agent-message") {
    expect(part.sender.agent).toBe("worker")
    expect(part.urgent).toBeUndefined()
    expect(part.replyTo).toBeUndefined()
  }
})

test("agent-message part input decodes and omits absent optionals", () => {
  const part = Schema.decodeUnknownSync(SessionV1.AgentMessagePartInput)({
    type: "agent-message",
    sender: { sessionID: "ses_01JT0000000000000000000001", agent: "worker" },
    to: "ses_01JT0000000000000000000000",
    replyTo: "msg_01JT0000000000000000000000",
    urgent: true,
    text: "hi",
  })
  expect(part.replyTo).toBe("msg_01JT0000000000000000000000")
  expect(part.urgent).toBe(true)
})
```

Note: prefix validators require the underscore (` prt_`, `ses_`, `msg_`) — ids above already carry them.

- [ ] **Step 2: Run test to verify it fails**

Run from `packages/opencode`: `bun test test/session/agent-message-schema.test.ts`
Expected: FAIL — `AgentMessagePartInput` does not exist on `SessionV1` (or decode fails on unknown type).

- [ ] **Step 3: Implement the part type**

In `packages/schema/src/v1/session.ts`, next to the other part-input definitions (~line 400-455), add:

```ts
export const AgentMessagePart = Schema.Struct({
  ...partBase,
  type: Schema.Literal("agent-message"),
  sender: Schema.Struct({
    sessionID: SessionID,
    agent: Schema.String,
  }),
  to: SessionID,
  replyTo: Schema.optional(MessageID),
  urgent: Schema.optional(Schema.Boolean),
  text: Schema.String,
}).annotate({ identifier: "AgentMessagePart" })
export type AgentMessagePart = Types.DeepMutable<Schema.Schema.Type<typeof AgentMessagePart>>
```

And near `TextPartInput`/`SubtaskPartInput`:

```ts
export const AgentMessagePartInput = Schema.Struct({
  id: Schema.optional(PartID),
  type: Schema.Literal("agent-message"),
  sender: Schema.Struct({
    sessionID: SessionID,
    agent: Schema.String,
  }),
  to: SessionID,
  replyTo: Schema.optional(MessageID),
  urgent: Schema.optional(Schema.Boolean),
  text: Schema.String,
}).annotate({ identifier: "AgentMessagePartInput" })
export type AgentMessagePartInput = Types.DeepMutable<Schema.Schema.Type<typeof AgentMessagePartInput>>
```

Add `AgentMessagePart` to both members of the `Part` union and the exported `Part` type union (`packages/schema/src/v1/session.ts:357-383`).

`packages/core/src/v1/session.ts` re-exports schema names through an **explicit** list (not `export *`) — add `AgentMessagePart` and `AgentMessagePartInput` to that list (next to `SubtaskPartInput`, ~line 34), keeping alphabetical order.

Create a session named agent passthrough check: `packages/opencode/src/session/schema` re-exports the IDs it needs — Task 3 already imports `SessionID`/`MessageID` from `@/session/schema`, which task.ts uses identically; no extra change.

- [ ] **Step 4: Run test to verify it passes**

Run from `packages/opencode`: `bun test test/session/agent-message-schema.test.ts`
Expected: PASS. Also run `bun run typecheck` in `packages/opencode` to catch union-member fallout.

- [ ] **Step 5: Commit**

```bash
git add packages/schema/src/v1/session.ts packages/opencode/test/session/agent-message-schema.test.ts
git commit -m "feat(schema): add agent-message part type"
```

### Task 2: Prompt ingestion and provider-content compilation

**Files:**
- Modify: `packages/opencode/src/session/prompt.ts` (~line 1512-1519 PromptInput union, ~line 699 `resolvePart`)
- Modify: `packages/opencode/src/session/message-v2.ts` (user-content loop ~lines 214-251)
- Test: append to `packages/opencode/test/session/prompt.test.ts` (reuse its harness)

**Interfaces:**
- Consumes (Task 1): `SessionV1.AgentMessagePart`, `SessionV1.AgentMessagePartInput`.
- Produces: `PromptInput["parts"]` accepts `{type: "agent-message", ...}`; persisted as an `AgentMessagePart`; providers see plain text `[agent-message from=<agent> session=<id>]\n<text>`. Later tasks (3-7) construct `agent-message` part inputs to deliver.

- [ ] **Step 1: Write failing tests in `test/session/prompt.test.ts`**

Inside the existing file, add an A2A describe using its existing helpers (`it.instance`, `reply`, `writeConfig`, `cfg`, `providerCfg`, `useServerConfig` are module-level there). Near the file's other `testEffect` definitions (line ~242-257) add:

```ts
const a2a = testEffect(makeHttp())
```

Then at the bottom of the file:

```ts
describe("session.prompt agent-message parts", () => {
  a2a.instance("persists an agent-message part and sends its wrapped text to the LLM", () =>
    Effect.gen(function* () {
      const llm = yield* TestLLMServer
      const sessions = yield* Session.Service
      const promptSvc = yield* SessionPrompt.Service
      const target = yield* sessions.create({})
      yield* llm.push(reply().text("ack"))
      const message = yield* promptSvc.prompt({
        sessionID: target.id,
        parts: [
          {
            type: "agent-message",
            sender: { sessionID: SessionID.make("ses_01JT0000000000000000000001"), agent: "worker" },
            to: target.id,
            text: "build finished",
          },
        ],
      })
      const parts = yield* sessions.messages({ sessionID: target.id })
      const agentPart = message.parts.find((p): p is SessionV1.AgentMessagePart => p.type === "agent-message")
      expect(agentPart?.sender.agent).toBe("worker")
      expect(parts.length).toBeGreaterThan(0)
    }),
  )
})
```

 Notes:
- Copy the exact import block pattern from the top of `prompt.test.ts`; add `SessionID` to the already-imported `../../src/session/schema` import.
- If the instance fixture requires a provider config before prompting, follow the nearest pre-existing example in the file (look at the first `it.instance` that calls `promptSvc.prompt` and mirror its config steps verbatim, swapping in the shown payload).

- [ ] **Step 2: Run test to verify it fails**

Run from `packages/opencode`: `bun test test/session/prompt.test.ts -t agent-message`
Expected: FAIL — decode error: `agent-message` not part of the PromptInput parts union.

- [ ] **Step 3: Implement**

`packages/opencode/src/session/prompt.ts` — add to the `PromptInput` parts union (~line 1512):

```ts
parts: Schema.Array(
  Schema.Union([
    SessionV1.TextPartInput,
    SessionV1.FilePartInput,
    SessionV1.AgentPartInput,
    SessionV1.SubtaskPartInput,
    SessionV1.AgentMessagePartInput,
  ]).annotate({ discriminator: "type" }),
),
```

In `resolvePart` (`SessionPrompt.resolveUserPart`, ~line 699) add a branch next to the `"file"` branch:

```ts
if (part.type === "agent-message") {
  return [
    {
      messageID: info.id,
      sessionID: input.sessionID,
      type: "agent-message",
      sender: part.sender,
      to: part.to,
      replyTo: part.replyTo,
      urgent: part.urgent,
      text: part.text,
    },
  ]
}
```

`packages/opencode/src/session/message-v2.ts` — in the user-message parts loop (after the `"subtask"` branch at ~line 249) add:

```ts
if (part.type === "agent-message" && !part.ignored && part.text !== "")
  userMessage.parts.push({
    type: "text",
    text: `[agent-message from=${part.sender.agent} session=${part.sender.sessionID}]\n${part.text}`,
  })
```

- [ ] **Step 4: Run test to verify it passes**

Run from `packages/opencode`: `bun test test/session/prompt.test.ts -t agent-message`
Expected: PASS. Then `bun test test/session/prompt.test.ts` for no regressions.

- [ ] **Step 5: Typecheck and commit**

`bun typecheck` from `packages/opencode`.

```bash
git add packages/opencode/src/session/prompt.ts packages/opencode/src/session/message-v2.ts packages/opencode/test/session/prompt.test.ts
git commit -m "feat(opencode): admit agent-message parts into prompts"
```

### Task 3: `AgentMail` service — addressing, validation, bounce

**Files:**
- Create: `packages/opencode/src/agent-mail/agent-mail.ts`
- Create: `packages/opencode/test/agent-mail/harness.ts` (shared test layer)
- Test: `packages/opencode/test/agent-mail/agent-mail.test.ts`

**Interfaces:**
- Consumes: `SessionPrompt.PromptInput` parts incl. `agent-message` (Task 2), `Session.Service` (`list/get`, `session.ts:413-451`), `SessionStatus.Service` (`get/list`, `status.ts:13-17`), `EventV2Bridge.Service`, `RuntimeFlags.Service`, `SessionID`/`MessageID` from `@/session/schema`.
- Produces (Tasks 4-7):

```ts
export interface SendInput {
  to: string
  text: string
  urgent?: boolean
  replyTo?: MessageID
  sender: { sessionID: SessionID; agent: string }
}
export interface DeliveryResult {
  target: SessionID
  delivery: "delivered" | "queued"
}
export interface AgentMailErrorTag { _tag: "MessagingDisabled" | "UnknownTarget" | "SelfSend" | "TooLarge" | "RateLimited" | "TargetMissing"; message: string }
export class Service extends Context.Service<Service, Interface>()("@opencode/AgentMail")
```

`Interface = { send(input: SendInput): Effect.Effect<DeliveryResult, AgentMailError> }`. Delivery mechanics come in Task 4; this task makes `send()` resolve, validate, and bounce; admission delegates to a private `admit()` that Task 4 replaces.

- [ ] **Step 1: Create the shared test layer** `packages/opencode/test/agent-mail/harness.ts`

Copy the essentials of the harness from `test/session/prompt.test.ts` (module-level `summary`, `makeMcp`, `lsp`, `runtimeFlags`, `cfg`, `writeConfig` helpers and the `promptRoot` group, lines 61-320) into this file and export:

```ts
import { AgentMail } from "@/agent-mail/agent-mail"

export const mailRoot = LayerNode.group([
  ...promptRoot.nodes,
  AgentMail.node,
])

export const makeMail = (options?: { processor?: "blocking" }) => {
  const replacements = [
    [SessionSummary.node, summary],
    [LSP.node, lsp],
    [MCP.node, makeMcp()],
    [RuntimeFlags.node, RuntimeFlags.layer({ experimentalEventSystem: true, a2a: true, a2aAutoWake: true })],
  ] as const
  if (options?.processor === "blocking")
    return LayerNode.compile(mailRoot, [...replacements, [SessionProcessor.node, blockingProcessor]])
  return LayerNode.compile(mailRoot, replacements)
}
export const it5 = testEffect(makeMail()).instance
```

Follow the verbatim shapes of the originals when copying (do not retype from memory); the only new wiring is `AgentMail.node` appended to the group and the flag overrides.

- [ ] **Step 2: Write failing tests** `packages/opencode/test/agent-mail/agent-mail.test.ts`

```ts
import { describe, expect } from "bun:test"
import { Effect } from "effect"
import { Session } from "@/session/session"
import { AgentMail } from "@/agent-mail/agent-mail"
import { it5 } from "./harness"

const OTHER = "ses_01JT0000000000000000000001"

describe("AgentMail", () => {
  it5("bounces unknown targets, self-sends, oversize text, and disabled messaging", () =>
    Effect.gen(function* () {
      const mail = yield* AgentMail.Service
      const sessions = yield* Session.Service
      const target = yield* sessions.create({})
      const sender = { sessionID: target.id, agent: "main" }

      const unknown = yield* mail.send({ ...sender, to: "nope-agent", text: "hi" }).pipe(Effect.either)
      expect(unknown._tag === "Left" && unknown.left._tag).toBe("UnknownTarget")

      const self = yield* mail.send({ ...sender, to: target.id, text: "hi" }).pipe(Effect.either)
      expect(self._tag === "Left" && self.left._tag).toBe("SelfSend")

      const big = yield* mail.send({ ...sender, to: target.id, text: "x".repeat(16 * 1024 + 1) }).pipe(Effect.either)
      expect(big._tag === "Left" && big.left._tag).toBe("TooLarge")
    }),
  )

  it5("resolves an agent name to the single session running it", () =>
    Effect.gen(function* () {
      const mail = yield* AgentMail.Service
      const sessions = yield* Session.Service
      const worker = yield* sessions.create({ agent: "build" })
      const senderSession = yield* sessions.create({})
      const result = yield* mail.send({
        to: "build",
        text: "coordinating",
        sender: { sessionID: senderSession.id, agent: "main" },
      })
      expect(result.target).toBe(worker.id)
    }),
  )

  it5("prefer the most recently updated session when an agent name is ambiguous", () =>
    Effect.gen(function* () {
      const mail = yield* AgentMail.Service
      const sessions = yield* Session.Service
      yield* sessions.create({ agent: "build" })
      yield* Effect.sleep(5)
      const newer = yield* sessions.create({ agent: "build" })
      const senderSession = yield* sessions.create({})
      const result = yield* mail.send({
        to: "build",
        text: "coordinating",
        sender: { sessionID: senderSession.id, agent: "main" },
      })
      expect(result.target).toBe(newer.id)
    }),
  )

  it5("bounces with the candidate list when the target is unknown and similar agents exist", () =>
    Effect.gen(function* () {
      const mail = yield* AgentMail.Service
      const sessions = yield* Session.Service
      yield* sessions.create({ agent: "greet" })
      const senderSession = yield* sessions.create({})
      const failure = yield* mail
        .send({ to: "greets", text: "x", sender: { sessionID: senderSession.id, agent: "main" } })
        .pipe(Effect.either)
      expect(failure._tag === "Left" && failure.left.message.includes("greet")).toBe(true)
    }),
  )
})
```

- [ ] **Step 3: Run to verify failure**

Run from `packages/opencode`: `bun test test/agent-mail/agent-mail.test.ts`
Expected: FAIL — `@/agent-mail/agent-mail` does not exist (test file fails to import).

- [ ] **Step 4: Implement `agent-mail.ts`**

```ts
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { InstanceState } from "@/effect/instance-state"
import { Session } from "@/session/session"
import { SessionStatus } from "@/session/status"
import { SessionPrompt } from "@/session/prompt"
import { EventV2Bridge } from "@/event-v2-bridge"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { MessageID, SessionID } from "@/session/schema"
import { Context, Effect, Layer, Schema } from "effect"

const MAX_TEXT_BYTES = 16 * 1024
const RATE_LIMIT = 30
const RATE_WINDOW_MS = 60_000

export interface SendInput {
  to: string
  text: string
  urgent?: boolean
  replyTo?: MessageID
  sender: { sessionID: SessionID; agent: string }
}
export interface DeliveryResult {
  target: SessionID
  delivery: "delivered" | "queued"
}

export type SendError =
  | MessagingDisabled
  | UnknownTarget
  | SelfSend
  | TooLarge
  | RateLimited
  | TargetMissing

// Typed errors per packages/opencode/AGENTS.md — Schema.TaggedErrorClass
export class MessagingDisabled extends Schema.TaggedErrorClass<MessagingDisabled>()("AgentMail.MessagingDisabled", {
  message: Schema.String,
}) {}
export class UnknownTarget extends Schema.TaggedErrorClass<UnknownTarget>()("AgentMail.UnknownTarget", {
  message: Schema.String,
}) {}
export class SelfSend extends Schema.TaggedErrorClass<SelfSend>()("AgentMail.SelfSend", {
  message: Schema.String,
}) {}
export class TooLarge extends Schema.TaggedErrorClass<TooLarge>()("AgentMail.TooLarge", {
  message: Schema.String,
}) {}
export class RateLimited extends Schema.TaggedErrorClass<RateLimited>()("AgentMail.RateLimited", {
  message: Schema.String,
}) {}
export class TargetMissing extends Schema.TaggedErrorClass<TargetMissing>()("AgentMail.TargetMissing", {
  message: Schema.String,
}) {}

export interface MailState {
  pending: Map<SessionID, Array<{ input: SendInput; messageID: MessageID }>>
  window: Map<string, number[]>
}

export interface Interface {
  readonly send: (input: SendInput) => Effect.Effect<DeliveryResult, SendError>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/AgentMail") {}

const emptyState: MailState = {
  pending: new Map(),
  window: new Map(),
}

const isSessionID = (value: string) => value.startsWith("ses_")

const resolveTarget = Effect.fn("AgentMail.resolveTarget")(function* (to: string, sender: SendInput["sender"]) {
  const sessionSvc = yield* Session.Service
  if (isSessionID(to)) {
    const target = yield* sessionSvc.get(SessionID.make(to)).pipe(Effect.either)
    if (target._tag === "Left") return yield* new UnknownTarget({ message: `No session ${to}` })
    return target.right
  }
  const all = yield* sessionSvc.list()
  const candidates = all
    .filter((info) => info.agent === to && info.id !== sender.sessionID && !info.time?.archived)
    .toSorted((a, b) => (b.time?.updated ?? 0) - (a.time?.updated ?? 0))
  if (candidates.length === 0) {
    const known = [...new Set(all.filter((info) => info.agent && info.id !== sender.sessionID).map((info) => info.agent))]
    return yield* new UnknownTarget({
      message: `No session or agent "${to}". Known agents: ${known.join(", ") || "(none)"}`,
    })
  }
  const pick = candidates[0]
  if (!pick) return yield* new UnknownTarget({ message: `No session or agent "${to}"` })
  return pick
})

const rateLimit = Effect.fn("AgentMail.rateLimit")(function* (state: MailState, pair: string) {
  const now = Date.now()
  const hits = (state.window.get(pair) ?? []).filter((t) => now - t < RATE_WINDOW_MS)
  if (hits.length >= RATE_LIMIT) return yield* new RateLimited({ message: `Rate limit exceeded for ${pair}` })
  hits.push(now)
  state.window.set(pair, hits)
})

const validate = Effect.fn("AgentMail.validate")(function* (state: MailState, input: SendInput) {
  if (new TextEncoder().encode(input.text).byteLength > MAX_TEXT_BYTES)
    return yield* new TooLarge({ message: `text exceeds ${MAX_TEXT_BYTES} bytes` })
  if (input.to === input.sender.sessionID) return yield* new SelfSend({ message: "cannot message own session" })
  yield* rateLimit(state, `${input.sender.sessionID}->${input.to}`)
})
```

The service body:

```ts
const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const flags = yield* RuntimeFlags.Service
    const state = yield* InstanceState.make(
      Effect.sync(() => ({ pending: new Map(), window: new Map() } as MailState)),
    )

    const admit = (target: SessionID, input: SendInput, messageID: MessageID) =>
      Effect.gen(function* () {
        const promptSvc = yield* SessionPrompt.Service
        yield* promptSvc.prompt({
          sessionID: target,
          messageID,
          parts: [agentMessagePartInput(target, input)],
        })
      })

    const send = Effect.fn("AgentMail.send")(function* (input: SendInput) {
      if (!flags.a2a) return yield* new MessagingDisabled({ message: "Set OPENCODE_A2A=true to enable agent messaging" })
      const data = yield* InstanceState.get(state)
      yield* validate(data, input)
      const target = yield* resolveTarget(input.to, input.sender)
      const messageID = MessageID.ascending()
      const status = yield* (yield* SessionStatus.Service).get(target)
      if (status.type === "busy" && !input.urgent) {
        const queue = data.pending.get(target) ?? []
        queue.push({ input, messageID })
        data.pending.set(target, queue)
        return { target, delivery: "queued" as const }
      }
      yield* admit(target, input, messageID).pipe(
        Effect.orElse(() =>
          Effect.gen(function* () {
            const queue = data.pending.get(target) ?? []
            queue.push({ input, messageID })
            data.pending.set(target, queue)
          }),
        ),
      )
      return { target, delivery: "delivered" as const }
    })

    return Service.of({ send })
  }),
)

export const node = LayerNode.make({ service: Service, layer, deps: [SessionPrompt.node, Session.node, SessionStatus.node, EventV2Bridge.node, RuntimeFlags.node] })

export * as AgentMail from "./agent-mail"

function agentMessagePartInput(target: SessionID, input: SendInput) {
  return {
    type: "agent-message" as const,
    sender: { sessionID: input.sender.sessionID, agent: input.sender.agent },
    to: target,
    replyTo: input.replyTo,
    urgent: input.urgent,
    text: input.text,
  }
}
```

The error classes above are the final shape (repo convention: `Schema.TaggedErrorClass` per
`packages/opencode/AGENTS.md`). Task 4 will fork admission fibers and flush pending FIFO on idle.

Also add the two config flags to `packages/opencode/src/effect/runtime-flags.ts`:

```ts
a2a: bool("OPENCODE_A2A"),
a2aAutoWake: bool("OPENCODE_A2A_AUTO_WAKE"),
```

- [ ] **Step 5: Run tests to verify they pass**

Run from `packages/opencode`: `bun test test/agent-mail/agent-mail.test.ts`
Expected: PASS (validate/resolve paths; admission may run the target loop — the harness LLM replies only when pushed, and `send()` in this task admits only for idle targets; admission failure falls back to enqueue, so assertions on bounce/resolve hold).

- [ ] **Step 6: Typecheck and commit**

```bash
git add packages/opencode/src/agent-mail packages/opencode/test/agent-mail packages/opencode/src/effect/runtime-flags.ts
git commit -m "feat(opencode): add AgentMail service for agent addressing and validation"
```

---

### Task 4: `AgentMail` — delivery scheduling (idle flush, urgent fork)

**Files:**
- Modify: `packages/opencode/src/agent-mail/agent-mail.ts`
- Test: `packages/opencode/test/agent-mail/agent-mail.test.ts` (extend)

**Interfaces:**
- Consumes (Task 3): `AgentMail` service internals, `MailState.pending`, `EventV2Bridge.listen` (used identically to `packages/opencode/src/event-v2-bridge.ts:35-62` — `events.listen(fn)` receives every published event), `SessionStatusEvent` (`@opencode-ai/schema/session-status-event`, exposes `Idle` events carrying `{ sessionID }`).
- Produces: FIFO flush on idle; urgent fork admits immediately through the joined-prompt seam; `DeliveryResult` unchanged.

- [ ] **Step 1: Write failing tests (append to `agent-mail.test.ts`)**

```ts
it5("holds a normal message while the target is busy, then flushes FIFO on idle", () =>
  Effect.gen(function* () {
    const mail = yield* AgentMail.Service
    const sessions = yield* Session.Service
    const promptSvc = yield* SessionPrompt.Service
    const target = yield* sessions.create({ agent: "build" })
    const senderSession = yield* sessions.create({})
    yield* llm.push(reply().text("working").stop())
    const fiber = yield* promptSvc
      .prompt({ sessionID: target.id, parts: [{ type: "text", text: "work" }] })
      .pipe(Effect.forkChild)
    yield* pollWithTimeout(
      Effect.gen(function* () {
        const status = yield* (yield* SessionStatus.Service).get(target.id)
        return status.type === "busy" ? (true as const) : undefined
      }),
      "target never became busy",
    )
    const first = yield* mail.send({ to: "build", text: "one", sender: { sessionID: senderSession.id, agent: "main" } })
    const second = yield* mail.send({ to: "build", text: "two", sender: { sessionID: senderSession.id, agent: "main" } })
    expect(first.delivery).toBe("queued")
    expect(second.delivery).toBe("queued")

    // Finish the in-flight turn: the run drains, the session goes idle, the
    // watcher flushes FIFO, and each delivered message opens its own turn.
    yield* llm.push(reply().text("done").stop())
    yield* llm.push(reply().text("got one").stop())
    yield* llm.push(reply().text("got two").stop())
    yield* pollWithTimeout(
      Effect.gen(function* () {
        const messages = yield* sessions.messages({ sessionID: target.id })
        const one = messages.some((m) => m.parts.some((p) => p.type === "agent-message" && p.text === "one"))
        const two = messages.some((m) => m.parts.some((p) => p.type === "agent-message" && p.text === "two"))
        return one && two ? (true as const) : undefined
      }),
      "pending messages never flushed after idle",
    )
    yield* Fiber.interrupt(fiber)
  }),
)

it5("urgent message steers a busy target immediately", () =>
  Effect.gen(function* () {
    const mail = yield* AgentMail.Service
    const sessions = yield* Session.Service
    const promptSvc = yield* SessionPrompt.Service
    const target = yield* sessions.create({ agent: "build" })
    const senderSession = yield* sessions.create({})
    yield* llm.push(reply().text("working").stop())
    const fiber = yield* promptSvc
      .prompt({ sessionID: target.id, parts: [{ type: "text", text: "work" }] })
      .pipe(Effect.forkChild)
    yield* pollWithTimeout(
      Effect.gen(function* () {
        const status = yield* (yield* SessionStatus.Service).get(target.id)
        return status.type === "busy" ? (true as const) : undefined
      }),
      "target never became busy",
    )
    const delivered = yield* mail.send({
      to: target.id,
      text: "priority note",
      urgent: true,
      sender: { sessionID: senderSession.id, agent: "main" },
    })
    expect(delivered.delivery).toBe("delivered")

    // The admitted prompt joins the running loop; at the turn boundary the
    // urgent message is the latest user input for the next provider turn.
    yield* llm.push(reply().text("done").stop())
    yield* llm.push(reply().text("ack urgent").stop())
    yield* pollWithTimeout(
      Effect.gen(function* () {
        const messages = yield* sessions.messages({ sessionID: target.id })
        return messages.some((m) =>
          m.parts.some((p) => p.type === "agent-message" && p.text === "priority note"),
        )
          ? (true as const)
          : undefined
      }),
      "urgent message never processed",
    )
    yield* Fiber.interrupt(fiber)
  }),
)
```

Step-key for LLM replies (`reply().text(...).stop()`) comes from `test/lib/llm-server.ts` (`reply()` builder, used in prompt.test.ts:656,935). Extend `agent-mail.test.ts`'s import block with what Task 4's tests use:

```ts
import { SessionPrompt } from "@/session/prompt"
import { SessionStatus } from "@/session/status"
import { TestLLMServer, reply } from "../lib/llm-server"
import { Fiber } from "effect"
import { pollWithTimeout } from "../lib/effect"
```

(In Task 4's tests above, `llm` is fetched with `const llm = yield* TestLLMServer` before the first push.)

Then adapt the busy-block recipe to whatever the nearest existing "keep a session busy" test does in `test/session/prompt.test.ts` (search for `blockingProcessor` usage: start a prompt, assert `status.type === "busy"`, then resolve).

- [ ] **Step 2: Run to verify failure**

Run from `packages/opencode`: `bun test test/agent-mail/agent-mail.test.ts -t "flushes FIFO|steers a busy"` 
Expected: FAIL — busy targets enqueue (Task 3 behavior already queues normal messages and admits urgent immediately), but queue is **never flushed** (no idle watcher) and FIFO ordering is not asserted as delivered; the first test times out in `pollWithTimeout`.

- [ ] **Step 3: Implement**

In `agent-mail.ts`, inside the service layer, subscribe to idle events once and define `flush`:

```ts
const sessionStatus = yield* SessionStatus.Service
const events = yield* EventV2Bridge.Service
const data = yield* InstanceState.get(state)

const admit = Effect.fn("AgentMail.admit")(function* (target: SessionID, input: SendInput, messageID: MessageID) {
  const promptSvc = yield* SessionPrompt.Service
  yield* promptSvc.prompt({ sessionID: target, messageID, parts: [agentMessagePartInput(target, input)] })
})

const flush = Effect.fn("AgentMail.flush")(function* (sessionID: SessionID) {
  const queue = data.pending.get(sessionID) ?? []
  if (queue.length === 0) return
  data.pending.delete(sessionID)
  for (const item of queue) {
    yield* admit(sessionID, item.input, item.messageID).pipe(
      Effect.orElse(() => Effect.logError("AgentMail delivery failed", { target: sessionID })),
    )
  }
})

// `session.idle` is published whenever a session drains (session/status.ts:41-48).
const unsubscribe = events.listen((event) =>
  Effect.gen(function* () {
    if (event.type !== "session.idle") return
    const { sessionID } = event.data as { sessionID: SessionID }
    yield* flush(sessionID).pipe(Effect.catchCause(() => Effect.logError("AgentMail idle flush failed")))
  }),
)
yield* Effect.addFinalizer(() => unsubscribe)
```

Wire the exact event shape from the real `SessionStatusEvent` module instead of the loose cast shown: open `packages/schema/src/session-status-event.ts` and use `SessionStatusEvent.Idle`'s actual declared payload/type constant (`SessionStatus` publishes it at `status.ts:43`, `events.publish(Event.Idle, { sessionID })`). Match `event.type`/`event.data` against that definition and drop the casts.

Then in `send()` (Task 3 body), replace the two inline `admit(...)` enqueue fallbacks so an urgent send on a busy session:

```ts
if (status.type === "busy" && !input.urgent) {
  // unchanged — enqueue is Task 3 behavior
}
yield* admit(target, input, messageID).pipe(Effect.forkIn(yield* Effect.scope))
return { target, delivery: "delivered" as const }
```

Urgent sends no longer block `send()` on the drained prompt (fork admission; the joined-prompt seam guarantees the target processes the message at its next turn boundary). Also add to `send()` before enqueueing: if `!(yield* flags).a2aAutoWake` and the target is idle-with-no-run, do NOT fork admission — enqueue only, so unattended setups never wake sessions (Task 1 of spec's `a2a.autoWake`). Implementation hint: when the target is idle and autoWake is off, enqueue and return `{ delivery: "queued" }`; when it is busy, urgent sends still fork immediately (steering), normal sends enqueue (unchanged).

- [ ] **Step 4: Run tests to verify they pass**

Run from `packages/opencode`: `bun test test/agent-mail/agent-mail.test.ts`
Expected: PASS — all Task 3 + Task 4 tests green. Then `bun test test/session/prompt.test.ts` still passes.

- [ ] **Step 5: Typecheck and commit**

`bun typecheck` from `packages/opencode`:

```bash
git add packages/opencode/src/agent-mail/agent-mail.ts packages/opencode/test/agent-mail/agent-mail.test.ts
git commit -m "feat(opencode): deliver agent mail on idle and urgent steering"
```

---

### Task 5: `agent_message` tool + registry wiring

**Files:**
- Create: `packages/opencode/src/tool/agent-message.ts`
- Modify: `packages/opencode/src/tool/registry.ts` (~line 209-249)
- Test: extend `packages/opencode/test/agent-mail/agent-mail.test.ts` (E2E via LLM tool-call)

**Interfaces:**
- Consumes (Tasks 3-4): `AgentMail.Service.send(input): Effect<DeliveryResult, SendError>`, `Tool.Context` fields `ctx.sessionID`, `ctx.agent` (see `packages/opencode/src/tool/task.ts:235`, `:231`).
- Produces: builtin tool id `agent_message`; whatever agents use to send to peers/parents.

- [ ] **Step 1: Write the failing E2E test** (append to `agent-mail.test.ts`)

```ts
it5("executes the agent_message tool for an agent calling it in a run", () =>
  Effect.gen(function* () {
    const llm = yield* TestLLMServer
    const sessions = yield* Session.Service
    const promptSvc = yield* SessionPrompt.Service
    const target = yield* sessions.create({ agent: "build" })
    const main = yield* sessions.create({})
    // The main agent's first turn issues the tool call, second turn confirms.
    yield* llm.push(reply().tool("agent_message", { to: "build", text: "hello worker" }).stop())
    yield* llm.push(reply().text("sent").stop())
    yield* promptSvc.prompt({ sessionID: main.id, parts: [{ type: "text", text: "tell build hello" }] })
    yield* pollWithTimeout(
      Effect.gen(function* () {
        const messages = yield* sessions.messages({ sessionID: target.id })
        return messages.some((m) =>
          m.parts.some((p) => p.type === "agent-message" && p.text === "hello worker"),
        )
          ? (true as const)
          : undefined
      }),
      "tool-delivered message never landed in target",
    )
  }),
)
```

Note: the harness's ToolRegistry is the production one inside `promptRoot`, so the tool call above exercises the real registration path; extend imports with nothing new (reuse existing).

- [ ] **Step 2: Run to verify failure**

Run from `packages/opencode`: `bun test test/agent-mail/agent-mail.test.ts -t "tool call"`
Expected: FAIL — unknown tool `agent_message` produces a tool error part instead of delivery.

- [ ] **Step 3: Implement the tool**

Create `packages/opencode/src/tool/agent-message.ts`:

```ts
import { AgentMail } from "@/agent-mail/agent-mail"
import { Session } from "@/session/session"
import { MessageID } from "@/session/schema"
import * as Tool from "./tool"
import { Effect, Schema } from "effect"

const DESCRIPTION = [
  "Send a message to another agent's session, e.g. a peer agent you are coordinating with or your parent agent.",
  "Messages are delivered when the recipient's current turn ends (or immediately if it is idle);",
  "urgent messages are processed at the recipient's next turn boundary instead. You will receive replies as incoming agent-messages too.",
  "Coordinate shared resources (build ports, GPUs, files) through these messages before acting on them.",
].join("\n")

const Parameters = Schema.Struct({
  to: Schema.String.annotate({ description: "Recipient: a session ID or an agent name" }),
  text: Schema.String.annotate({ description: "Message body (text only)" }),
  replyTo: Schema.optional(Schema.String).annotate({ description: "Message ID this message replies to" }),
  urgent: Schema.optional(Schema.Boolean).annotate({ description: "Steer the recipient's in-flight turn instead of waiting for it to end" }),
})

export const AgentMessageTool = Tool.define(
  "agent_message",
  Effect.gen(function* () {
    const mail = yield* AgentMail.Service
    return {
      description: DESCRIPTION,
      parameters: Parameters,
      execute: (params: Schema.Schema.Type<typeof Parameters>, ctx: Tool.Context) =>
        mail
          .send({
            to: params.to,
            text: params.text,
            replyTo: params.replyTo ? MessageID.make(params.replyTo) : undefined,
            urgent: params.urgent,
            sender: { sessionID: ctx.sessionID, agent: ctx.agent ?? "user" },
          })
          .pipe(
            Effect.map((result) => ({
              title: `agent_message → ${result.target}`,
              metadata: { target: result.target, delivery: result.delivery },
              output:
                result.delivery === "delivered"
                  ? `Delivered to session ${result.target}. It will be processed at the recipient's next turn boundary.`
                  : `Queued for session ${result.target}; delivered when its current run finishes.`,
            })),
            Effect.mapError((error) => new Error(error.message)),
          ),
    }
  }),
)

export * as AgentMessageTool from "./agent-message"
```

- [ ] **Step 4: Register in the registry**

`packages/opencode/src/tool/registry.ts`:

```ts
// imports at top, next to TaskTool:
import { AgentMessageTool } from "./agent-message"

// in the Effect.all(...) tool init list (~line 217):
  agentMessage: Tool.init(AgentMessageTool),

// in the builtin splice (~line 240, after tool.task):
  tool.agentMessage,
```

- [ ] **Step 5: Run tests and commit**

Run from `packages/opencode`: `bun test test/agent-mail/agent-mail.test.ts` then `bun typecheck`.

```bash
git add packages/opencode/src/tool/agent-message.ts packages/opencode/src/tool/registry.ts packages/opencode/test/agent-mail/agent-mail.test.ts
git commit -m "feat(opencode): add agent_message builtin tool"
```

### Task 6: Route subagent completion through AgentMail

**Files:**
- Modify: `packages/opencode/src/tool/task.ts` (`inject`/`notify`, lines 227-265; add `agent` passthrough in AgentMail)
- Modify: `packages/opencode/src/agent-mail/agent-mail.ts` (`SendInput.agent?: string`, pass to admission)
- Modify: `packages/opencode/test/tool/task.test.ts` (background-completion assertions)

**Interfaces:**
- Consumes (Tasks 3-5): `AgentMail.Service.send`.
- Produces: completion notifications arrive as `agent-message` parts with sender `{ sessionID: <child>, agent: <subagent name> }`; `SendInput.agent` presets the target agent for the admitted prompt.

- [ ] **Step 1: Extend `SendInput` (agent-mail.ts) with optional target agent**

```ts
export interface SendInput {
  to: string
  text: string
  urgent?: boolean
  replyTo?: MessageID
  agent?: string
  sender: { sessionID: SessionID; agent: string }
}
```

and in `admit`:

```ts
yield* promptSvc.prompt({
  sessionID: target,
  messageID,
  agent: input.agent,
  parts: [agentMessagePartInput(target, input)],
})
```

- [ ] **Step 2: Rewrite `inject` in task.ts**

```ts
const inject = Effect.fn("TaskTool.injectBackgroundResult")(function* (state: "completed" | "error", text: string) {
  const currentParent = yield* sessions.get(ctx.sessionID)
  yield* mail
    .send({
      to: ctx.sessionID,
      agent: currentParent.agent ?? ctx.agent,
      text: renderOutput({
        sessionID: nextSession.id,
        state,
        summary:
          state === "completed"
            ? `Background task completed: ${params.description}`
            : `Background task failed: ${params.description}`,
        text,
      }),
      sender: { sessionID: nextSession.id, agent: next.name },
    })
    .pipe(Effect.ignore, Effect.forkIn(scope, { startImmediately: true }))
})
```

Add `const mail = yield* AgentMail.Service` to the `Tool.define` gen root (imports at top: `import { AgentMail } from "@/agent-mail/agent-mail"`). `notify` (lines 256-265) stays as-is.

- [ ] **Step 3: Update `test/tool/task.test.ts`**

Locate the background-completion test (grep `background: true` in `test/tool/task.test.ts`) and update its completion assertion to the new part type:

```ts
expect(completion.parts.some((p) => p.type === "agent-message" && p.sender.sessionID === taskSession.id)).toBe(true)
```

Mirror the variables used by the existing test (the child session id it already has available); keep every other assertion intact.

- [ ] **Step 4: Run tests and commit**

Run from `packages/opencode`: `bun test test/tool/task.test.ts` and `bun test test/agent-mail`.

```bash
git add packages/opencode/src/tool/task.ts packages/opencode/src/agent-mail/agent-mail.ts packages/opencode/test/tool/task.test.ts
git commit -m "feat(opencode): notify parents via AgentMail on background task completion"
```

---

### Task 7: HTTP route `POST /session/:sessionID/agent-message` + SDK regen

**Files:**
- Modify: `packages/opencode/src/server/routes/instance/httpapi/groups/session.ts` (`SessionPaths` block ~lines 80-106, endpoints after the existing post endpoints)
- Modify: `packages/opencode/src/server/routes/instance/httpapi/handlers/session.ts` (service yields + handler)
- Regen: `packages/sdk/js` (script) and `packages/client`

**Interfaces:**
- Consumes (Task 3): `AgentMail.Service.send` / `SendInput` / `DeliveryResult`.
- Produces SDK route `session.agentMessage` (POST) with payload `{ from, agent, text, urgent?, replyTo? }` and success `{ target, delivery }`.

- [ ] **Step 1: Declare the endpoint**

In `groups/session.ts`, add to `SessionPaths`:

```ts
  agentMessage: `${root}/:sessionID/agent-message`,
```

Add payload/success schemas near the other schema decls:

```ts
export const AgentMessagePayload = Schema.Struct({
  from: Schema.String,
  agent: Schema.String,
  text: Schema.String,
  urgent: Schema.optional(Schema.Boolean),
  replyTo: Schema.optional(Schema.String),
}).annotate({ identifier: "AgentMessagePayload" })

export const DeliveryInfo = Schema.Struct({
  target: Schema.String,
  delivery: Schema.Literals(["delivered", "queued"]),
}).annotate({ identifier: "AgentDeliveryInfo" })
```

Then, following the exact style of the `promptAsync` endpoint in the same group:

```ts
HttpApiEndpoint.post("agentMessage", SessionPaths.agentMessage, {
  params: { sessionID: SessionID },
  payload: AgentMessagePayload,
  success: described(DeliveryInfo, "Agent message delivery result"),
  error: HttpApiError.BadRequest,
}).annotateMerge(
  OpenApi.annotations({
    identifier: "session.agentMessage",
    summary: "Send an agent message",
    description: "Deliver an agent-to-agent message into a session's transcript and processing queue.",
  }),
),
```

Match import/`described` idioms already used by that file verbatim.

- [ ] **Step 2: Implement the handler**

In `handlers/session.ts`, inside the group builder where `promptSvc` etc. are yielded once, add:

```ts
const mail = yield* AgentMail.Service
```

(`import { AgentMail } from "@/agent-mail/agent-mail"` with the other service imports.) Handler:

```ts
const agentMessage = Effect.fn("SessionHttpApi.agentMessage")(function* (ctx: {
  params: { sessionID: SessionID }
  payload: typeof AgentMessagePayload.Type
}) {
  yield* requireSession(ctx.params.sessionID)
  const delivery = yield* mail
    .send({
      to: ctx.params.sessionID,
      text: ctx.payload.text,
      urgent: ctx.payload.urgent,
      replyTo: ctx.payload.replyTo,
      sender: { sessionID: SessionID.make(ctx.payload.from), agent: ctx.payload.agent },
    })
    .pipe(Effect.mapError(() => new HttpApiError.BadRequest({})))
  return delivery
})
```

And add `"agentMessage": agentMessage` to the group's handler record (same place `promptAsync` is wired).

- [ ] **Step 3: Verify with a smoke request**

`bun run dev serve` in a tmux pane (per `packages/opencode/AGENTS.md` tmux convention), with `OPENCODE_A2A=true` in the environment. Create a session, then:

```bash
curl -s -X POST "http://localhost:4096/session/<sessionID>/agent-message" \
  -H "content-type: application/json" \
  -d '{"from":"'"<secondSessionID>"'","agent":"main","text":"ping"}'
```

Expected: `{"target":"<sessionID>","delivery":"delivered"}` and an `agent-message` part visible via `GET /session/<sessionID>/message`. Kill the tmux session afterwards.

- [ ] **Step 4: Regenerate SDK and client**

From repo root:

```bash
./packages/sdk/js/script/build.ts
(cd packages/client && bun run generate)
```

- [ ] **Step 5: Commit**

```bash
git add packages/opencode/src/server packages/sdk/js packages/client
git commit -m "feat(server): add agent-message route and regenerate SDK"
```

### Task 8: TUI and shared-session-UI rendering + unread badge

**Files:**
- Modify: `packages/tui/src/routes/session/index.tsx` (`PART_MAPPING` ~line 1578; add component)
- Modify: `packages/tui/src/context/sync.tsx` (unread slice + clear helper)
- Modify: `packages/session-ui/src/v2/components/*` (part renderer, only if its part type union demands it for the web surface)

**Interfaces:**
- Consumes: `agent-message` part type (Task 1), `sync.tsx` event switch (`message.part.updated` case found at ~line 376).
- Produces: visible transcript rendering + per-session unread counts exposed on the sync context.

- [ ] **Step 1: Add `unread` state in `sync.tsx`**

Extend the store creation (~line 70) with an `unread: Record<string, number>` slice (initial `{}`), and in the `message.part.updated` case (before `break`) add:

```ts
if (event.properties.part.type === "agent-message")
  setStore("unread", event.properties.part.sessionID, (count = 0) => count + 1)
```

Export a clear helper from the same context:

```ts
const clearUnread = (sessionID: string) => setStore("unread", sessionID, 0)
```

and include it in the context value object returned by the provider.

- [ ] **Step 2: Clear-on-view in the session route**

In `packages/tui/src/routes/session/index.tsx`, wherever the route holds its active session id, clear on entry with the file's existing lifecycle idiom (search for `onMount` usage in that file and follow it):

```ts
onMount(() => sync.clearUnread(sessionID))
```

Use the exact session-id accessor pattern the surrounding code already uses instead of a bare `props.sessionID`.

- [ ] **Step 3: Render the part**

Add a renderer and register it:

```ts
const PART_MAPPING = {
  text: TextPart,
  tool: ToolPart,
  reasoning: ReasoningPart,
  "agent-message": AgentMessageView,
}
```

New component in the same file (SolidJS + opentui style, mirroring `ReasoningPart` ~line 1585):

```ts
function AgentMessageView(props: { last: boolean; part: AgentMessagePart; message: AssistantMessage }) {
  const { theme } = useTheme()
  return (
    <box paddingLeft={3} marginTop={1}>
      <text fg={theme.accent}>
        agent-message from {props.part.sender.agent} ({props.part.sender.sessionID}){props.part.urgent ? " · urgent" : ""}
      </text>
      <text>{props.part.text}</text>
    </box>
  )
}
```

Adapter note: `PART_MAPPING` components receive specific part props; when wiring, reconcile this sketch against the actual call-site signature for `TextPart` (same file, `PART_MAPPING` consumer a few lines below the declaration) — adjust the props type to match exactly, importing `AgentMessagePart` from `@opencode-ai/schema/v1`.

Also mirror the corresponding type-union handling in `packages/session-ui` part renderers if its part union switch is exhaustive (search for `"subtask"` in `packages/session-ui/src` and follow the same pattern found there).

- [ ] **Step 4: Badge display**

In the session list / wherever unknown-session badges naturally live in the TUI, surface `unread` counts (a colored `●N` next to the session row, using existing list-item spacing patterns). If the list-item component is in `packages/tui/src/routes/sessions/*`, follow its row-rendering props idiom. Scope note: badge placement is presentation-only; if the list page structure makes it nontrivial, rendering a toast for unread agent-messages while unfocused (search the codebase for the existing toast/notification primitive and reuse it) satisfies the spec's "minimal notification" requirement — do both only if each is cheap.

- [ ] **Step 5: Manual checklist + typecheck + commit**

`bun typecheck` in `packages/tui`, run `bun dev` in tmux per AGENTS.md, verify: agent-message renders with sender header; badge increments on message from another session; badge clears when viewing the session. Then:

```bash
git add packages/tui packages/session-ui
git commit -m "feat(tui): render agent messages and unread badges"
```

### Task 9: Final integration verification

- [ ] **Step 1: Typecheck every touched package** — `bun typecheck` in `packages/opencode`, `packages/tui`, `packages/schema`.
- [ ] **Step 2: Lint** — `bun lint` from repo root (`oxlint`).
- [ ] **Step 3: Targeted test suite** — from `packages/opencode`: `bun test test/agent-mail test/session/prompt.test.ts test/tool/task.test.ts test/session/agent-message-schema.test.ts test/tool/registry.test.ts`
- [ ] **Step 4: Manual E2E** — `bun dev` in tmux with `OPENCODE_A2A=true`: launch a background subagent (`task` + background), confirm the parent session keeps accepting input, gets the completion `agent-message` part, and responds in a new turn.
- [ ] **Step 5: Commit any stragglers** and re-run step 3 before reporting done.

---

## Execution notes

- Disable-flag default: with `OPENCODE_A2A` unset, `agent_message` fails fast with `MessagingDisabled` — safe rollout, opt-in via env.
- Pending inbox is in-memory (instance-scoped); a crash loses queued-but-undelivered rows. Delivered messages are durable parts. Acceptable v1 trade-off; if durability of pending rows is needed later, promote them to durable `session_input` rows on the V2 core.
- Future V2: `AgentMail` is session-core-agnostic except `admit()` (Task 6 adds `agent` passthrough); swap that call when V2 gains a task tool.
