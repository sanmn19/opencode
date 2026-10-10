import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { InstanceState } from "@/effect/instance-state"
import { Session } from "@/session/session"
import { SessionStatus } from "@/session/status"
import { SessionPrompt } from "@/session/prompt"
import { EventV2Bridge } from "@/event-v2-bridge"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { MessageID, SessionID } from "@/session/schema"
import { Cause, Context, Effect, Exit, Layer, Schema, Scope } from "effect"

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
  scope: Scope.Scope
}

export interface Interface {
  readonly send: (input: SendInput) => Effect.Effect<DeliveryResult, SendError>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/AgentMail") {}

const isSessionID = (value: string) => value.startsWith("ses_")

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

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const flags = yield* RuntimeFlags.Service
    const sessionSvc = yield* Session.Service
    const statuses = yield* SessionStatus.Service
    const promptSvc = yield* SessionPrompt.Service
    const events = yield* EventV2Bridge.Service

    const router = { flush: (_sessionID: SessionID) => Effect.void }

    const state = yield* InstanceState.make(() =>
      Effect.gen(function* () {
        // The idle watcher lives on the per-instance state so it re-attaches
        // whenever the instance is rebuilt and detaches with it (scope-close).
        const unsubscribe = yield* events.listen((event) =>
      Effect.gen(function* () {
        if (event.type !== "session.idle") return
        const { sessionID } = event.data as { sessionID: SessionID }
        yield* router.flush(sessionID)
      }),
        )
        yield* Effect.addFinalizer(() => unsubscribe)
        return { pending: new Map(), window: new Map(), scope: yield* Scope.Scope } as MailState
      }),
    )

    const enqueue = Effect.fn("AgentMail.enqueue")(function* (target: SessionID, input: SendInput, messageID: MessageID) {
      const data = yield* InstanceState.get(state)
      const queue = data.pending.get(target) ?? []
      queue.push({ input, messageID })
      data.pending.set(target, queue)
    })

    const resolveTarget = Effect.fn("AgentMail.resolveTarget")(function* (
      to: string,
      sender: SendInput["sender"],
    ) {
      if (isSessionID(to)) {
        const outcome = yield* sessionSvc.get(SessionID.make(to)).pipe(Effect.exit)
        if (!Exit.isSuccess(outcome)) return yield* new UnknownTarget({ message: `No session ${to}` })
        return outcome.value
      }
      const all = yield* sessionSvc.list()
      const candidates = all
        .filter((info) => info.agent === to && info.id !== sender.sessionID && !info.time?.archived)
        .toSorted((a, b) => (b.time?.updated ?? 0) - (a.time?.updated ?? 0))
      if (candidates.length === 0) {
        const known = [
          ...new Set(all.filter((info) => info.agent && info.id !== sender.sessionID).map((info) => info.agent)),
        ]
        return yield* new UnknownTarget({
          message: `No session or agent "${to}". Known agents: ${known.join(", ") || "(none)"}`,
        })
      }
      const pick = candidates[0]
      if (!pick) return yield* new UnknownTarget({ message: `No session or agent "${to}"` })
      return pick
    })

    const admit = (target: SessionID, input: SendInput, messageID: MessageID) =>
      Effect.gen(function* () {
        yield* promptSvc.prompt({
          sessionID: target,
          messageID,
          parts: [agentMessagePartInput(target, input)],
        })
      })

    const flush = Effect.fn("AgentMail.flush")(function* (sessionID: SessionID) {
      const data = yield* InstanceState.get(state)
      const queue = data.pending.get(sessionID) ?? []
      if (queue.length === 0) return
      data.pending.delete(sessionID)
      for (const item of queue) {
        const admitted = yield* admit(sessionID, item.input, item.messageID).pipe(Effect.exit)
        if (!Exit.isSuccess(admitted)) {
          yield* Effect.logError("AgentMail flushed delivery failed", { target: sessionID })
        }
      }
    })

    router.flush = Effect.fn("AgentMail.dispatchFlush")((sessionID: SessionID) =>
      flush(sessionID).pipe(
        Effect.catchCause((cause) => Effect.logError("AgentMail idle flush failed", { cause: Cause.pretty(cause) })),
      ),
    )

    const send = Effect.fn("AgentMail.send")(function* (input: SendInput) {
      if (!flags.a2a) {
        return yield* new MessagingDisabled({ message: "Set OPENCODE_A2A=true to enable agent messaging" })
      }
      const data = yield* InstanceState.get(state)
      const resolved = yield* resolveTarget(input.to, input.sender)
      const target = resolved.id
      yield* validate(data, input)
      const messageID = MessageID.ascending()
      const status = yield* statuses.get(target)
      if (status.type === "busy" && !input.urgent) {
        yield* enqueue(target, input, messageID)
        return { target, delivery: "queued" as const }
      }
      if (status.type !== "busy" && !flags.a2aAutoWake) {
        yield* enqueue(target, input, messageID)
        return { target, delivery: "queued" as const }
      }
      yield* admit(target, input, messageID)
        .pipe(
          Effect.exit,
          Effect.flatMap((outcome) => (Exit.isSuccess(outcome) ? Effect.void : enqueue(target, input, messageID))),
          Effect.forkIn(data.scope, { startImmediately: true }),
        )
      return { target, delivery: "delivered" as const }
    })

    return Service.of({ send })
  }),
)

export const node = LayerNode.make({
  service: Service,
  layer,
  deps: [SessionPrompt.node, Session.node, SessionStatus.node, EventV2Bridge.node, RuntimeFlags.node],
})

export * as AgentMail from "./agent-mail"

