import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { InstanceState } from "@/effect/instance-state"
import { Session } from "@/session/session"
import { SessionStatus } from "@/session/status"
import { EventV2Bridge } from "@/event-v2-bridge"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { MessageID, SessionID } from "@/session/schema"
import { Cause, Effect, Exit, Layer, Scope } from "effect"
import { AgentMail } from "./agent-mail"

const MAX_TEXT_BYTES = 16 * 1024
const RATE_LIMIT = 30
const RATE_WINDOW_MS = 60_000

const isSessionID = (value: string) => value.startsWith("ses_")

export const agentMessagePartInput = (target: SessionID, input: AgentMail.SendInput) => ({
  type: "agent-message" as const,
  sender: { sessionID: input.sender.sessionID, agent: input.sender.agent },
  to: target,
  replyTo: input.replyTo,
  urgent: input.urgent,
  text: input.text,
})

const validate = Effect.fn("AgentMail.validate")(function* (input: AgentMail.SendInput, state: MailState) {
  if (new TextEncoder().encode(input.text).byteLength > MAX_TEXT_BYTES)
    return yield* new AgentMail.TooLarge({ message: `text exceeds ${MAX_TEXT_BYTES} bytes` })
  if (input.to === input.sender.sessionID) return yield* new AgentMail.SelfSend({ message: "cannot message own session" })
  const now = Date.now()
  const pair = `${input.sender.sessionID}->${input.to}`
  const hits = (state.window.get(pair) ?? []).filter((t) => now - t < RATE_WINDOW_MS)
  if (hits.length >= RATE_LIMIT) return yield* new AgentMail.RateLimited({ message: `Rate limit exceeded for ${pair}` })
  hits.push(now)
  state.window.set(pair, hits)
})

interface MailState {
  pending: Map<SessionID, Array<{ input: AgentMail.SendInput; messageID: MessageID }>>
  window: Map<string, number[]>
  scope: Scope.Scope
}

const layer = Layer.effect(
  AgentMail.Service,
  Effect.gen(function* () {
    const flags = yield* RuntimeFlags.Service
    const sessionSvc = yield* Session.Service
    const statuses = yield* SessionStatus.Service
    const events = yield* EventV2Bridge.Service

    let dispatch: (sessionID: SessionID) => Effect.Effect<void> = () => Effect.void

    const state = yield* InstanceState.make(() =>
      Effect.gen(function* () {
        // The idle watcher lives on the per-instance state so it re-attaches
        // whenever the instance is rebuilt and detaches with it (scope-close).
        const unsubscribe = yield* events.listen((event) =>
          Effect.gen(function* () {
            if (event.type !== "session.idle") return
            const { sessionID } = event.data as { sessionID: SessionID }
            yield* dispatch(sessionID)
          }).pipe(
            Effect.catchCause((cause) =>
              Effect.logError("AgentMail idle flush failed", { cause: Cause.pretty(cause) }),
            ),
          ),
        )
        yield* Effect.addFinalizer(() => unsubscribe)
        return { pending: new Map(), window: new Map(), scope: yield* Scope.Scope } as MailState
      }),
    )

    const enqueue = Effect.fn("AgentMail.enqueue")(function* (target: SessionID, input: AgentMail.SendInput, messageID: MessageID) {
      const data = yield* InstanceState.get(state)
      const queue = data.pending.get(target) ?? []
      queue.push({ input, messageID })
      data.pending.set(target, queue)
    })

    const resolveTarget = Effect.fn("AgentMail.resolveTarget")(function* (to: string, sender: AgentMail.SendInput["sender"]) {
      if (isSessionID(to)) {
        const outcome = yield* sessionSvc.get(SessionID.make(to)).pipe(Effect.exit)
        if (!Exit.isSuccess(outcome)) return yield* new AgentMail.UnknownTarget({ message: `No session ${to}` })
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
        return yield* new AgentMail.UnknownTarget({
          message: `No session or agent "${to}". Known agents: ${known.join(", ") || "(none)"}`,
        })
      }
      const pick = candidates[0]
      if (!pick) return yield* new AgentMail.UnknownTarget({ message: `No session or agent "${to}"` })
      return pick
    })

    const box: { admitter?: AgentMail.Admitter } = {}

    const admit = (target: SessionID, input: AgentMail.SendInput, messageID: MessageID) =>
      Effect.gen(function* () {
        const admitter = box.admitter
        if (!admitter) return yield* new AgentMail.TargetMissing({ message: "session prompting is not available yet" })
        yield* admitter.prompt({
          sessionID: target,
          messageID,
          agent: input.agent,
          parts: [agentMessagePartInput(target, input)],
        })
      })

    const setAdmitter = (admitter: AgentMail.Admitter) =>
      Effect.sync(() => {
        box.admitter = admitter
      })

    const flushEffect = Effect.fn("AgentMail.flush")(function* (sessionID: SessionID) {
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

    dispatch = (sessionID: SessionID) =>
      flushEffect(sessionID).pipe(
        Effect.catchCause((cause) => Effect.logError("AgentMail idle flush failed", { cause: Cause.pretty(cause) })),
      )

    const send = Effect.fn("AgentMail.send")(function* (input: AgentMail.SendInput) {
      if (!flags.a2a) {
        return yield* new AgentMail.MessagingDisabled({ message: "Set OPENCODE_A2A=true to enable agent messaging" })
      }
      const data = yield* InstanceState.get(state)
      const resolved = yield* resolveTarget(input.to, input.sender)
      const target = resolved.id
      yield* validate(input, data)
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

    return AgentMail.Service.of({ send, setAdmitter })
  }),
)

export const node = LayerNode.make({
  service: AgentMail.Service,
  layer,
  deps: [Session.node, SessionStatus.node, EventV2Bridge.node, RuntimeFlags.node],
})

export * as AgentMailRuntime from "./delivery"
