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
  registry: Map<SessionID, AgentMail.RegistryEntry>
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

    const entryFromInfo = (info: Session.Info, status: AgentMail.RegistryEntry["status"]): AgentMail.RegistryEntry => ({
      sessionID: info.id,
      agent: info.agent ?? "",
      directory: info.directory,
      title: info.title,
      status,
      lastActive: info.time?.updated ?? info.time?.created ?? 0,
    })

    const state = yield* InstanceState.make(() =>
      Effect.gen(function* () {
        const registry = new Map<SessionID, AgentMail.RegistryEntry>()
        const busy = yield* statuses.list()
        const all = yield* sessionSvc.list()
        for (const info of all) {
          if (!info.agent || info.time?.archived) continue
          registry.set(info.id, entryFromInfo(info, busy.get(info.id)?.type === "busy" ? "busy" : "idle"))
        }
        // The idle watcher + registry live on the per-instance state so they
        // re-attach whenever the instance is rebuilt and detach with it (scope-close).
        const unsubscribe = yield* events.listen((event) =>
          Effect.gen(function* () {
            if (event.type === "session.idle") {
              const { sessionID } = event.data as { sessionID: SessionID }
              yield* dispatch(sessionID)
              return
            }
            if (event.type === "session.created" || event.type === "session.updated") {
              const { info } = event.data as { info: Session.Info }
              if (!info.agent || info.time?.archived) {
                registry.delete(info.id)
                return
              }
              const current = registry.get(info.id)
              registry.set(info.id, { ...entryFromInfo(info, current?.status ?? "idle"), ...(current && info.time?.updated === current.lastActive ? { lastActive: current.lastActive } : {}) })
              return
            }
            if (event.type === "session.status") {
              const { sessionID, status } = event.data as { sessionID: SessionID; status: { type: string } }
              const entry = registry.get(sessionID)
              if (entry && (status.type === "busy" || status.type === "idle" || status.type === "retry")) {
                registry.set(sessionID, { ...entry, status: status.type, lastActive: Date.now() })
              }
              return
            }
            if (event.type === "session.deleted") {
              const { sessionID } = event.data as { sessionID: SessionID }
              registry.delete(sessionID)
            }
          }).pipe(
            Effect.catchCause((cause) =>
              Effect.logError("AgentMail registry/flush event failed", { cause: Cause.pretty(cause) }),
            ),
          ),
        )
        yield* Effect.addFinalizer(() => unsubscribe)
        return { pending: new Map(), window: new Map(), registry, scope: yield* Scope.Scope } as MailState
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
      const data = yield* InstanceState.get(state)
      const registered = [...data.registry.values()]
        .filter((entry) => entry.agent === to && entry.sessionID !== sender.sessionID)
        .toSorted((a, b) => b.lastActive - a.lastActive)
      const registeredPick = registered[0]
      if (registeredPick) {
        const outcome = yield* sessionSvc.get(registeredPick.sessionID).pipe(Effect.exit)
        if (Exit.isSuccess(outcome)) return outcome.value
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

    const lookup = Effect.fn("AgentMail.lookup")(function* (query: AgentMail.RegistryQuery) {
      const data = yield* InstanceState.get(state)
      return [...data.registry.values()]
        .filter(
          (entry) =>
            (!query.agent || entry.agent === query.agent) &&
            (!query.directory || entry.directory === query.directory) &&
            (!query.status || entry.status === query.status),
        )
        .toSorted((a, b) => b.lastActive - a.lastActive)
    })

    return AgentMail.Service.of({ send, setAdmitter, lookup })
  }),
)

export const node = LayerNode.make({
  service: AgentMail.Service,
  layer,
  deps: [Session.node, SessionStatus.node, EventV2Bridge.node, RuntimeFlags.node],
})

export * as AgentMailRuntime from "./delivery"
