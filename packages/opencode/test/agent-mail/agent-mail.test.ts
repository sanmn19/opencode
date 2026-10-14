import { describe, expect } from "bun:test"
import { Cause, Effect, Exit, Fiber } from "effect"
import { Session } from "@/session/session"
import { SessionPrompt } from "@/session/prompt"
import { SessionStatus } from "@/session/status"
import { AgentMail } from "@/agent-mail/agent-mail"
import { SessionID } from "@/session/schema"
import { TestLLMServer, reply } from "../lib/llm-server"
import { pollWithTimeout } from "../lib/effect"
import { cfg5, it5, it6 } from "./harness"

function failureFrom(exit: unknown) {
  const cause = (exit as { cause?: { reasons?: Array<{ error?: unknown }>; failures?: Array<{ error?: unknown }> } })
    .cause ?? {}
  const reason = (cause.reasons ?? cause.failures ?? []).find((r) => r?.error)
  return reason?.error as { _tag: string; message: string }
}

describe("AgentMail", () => {
  it5("bounces unknown targets, self-sends, oversize text, and disabled messaging", () =>
    Effect.gen(function* () {
      const mail = yield* AgentMail.Service
      const sessions = yield* Session.Service
      const target = yield* sessions.create({})
      const sender = { sessionID: target.id, agent: "main" }

      const unknown = yield* mail.send({ sender, to: "nope-agent", text: "hi" }).pipe(Effect.exit)
      if (!Exit.isFailure(unknown)) throw new Error("expected UnknownTarget")
      expect(failureFrom(unknown)._tag).toBe("AgentMail.UnknownTarget")

      const self = yield* mail.send({ sender, to: target.id, text: "hi" }).pipe(Effect.exit)
      if (!Exit.isFailure(self)) throw new Error("expected SelfSend")
      expect(failureFrom(self)._tag).toBe("AgentMail.SelfSend")

      const big = yield* mail.send({ sender, to: target.id, text: "x".repeat(16 * 1024 + 1) }).pipe(Effect.exit)
      if (!Exit.isFailure(big)) throw new Error("expected TooLarge")
      expect(failureFrom(big)._tag).toBe("AgentMail.TooLarge")
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
        .pipe(Effect.exit)
      if (!Exit.isFailure(failure)) throw new Error("expected UnknownTarget")
      expect(failureFrom(failure).message.includes("greet")).toBe(true)
    }),
  )
})

describe("AgentMail delivery", () => {
  function makeBusy(sessionID: SessionID, command = "sleep 60") {
    return Effect.fn("test.busyShell")(function* () {
      const promptSvc = yield* SessionPrompt.Service
      const fiber = yield* promptSvc.shell({ sessionID, agent: "build", command }).pipe(Effect.forkChild)
      yield* pollWithTimeout(
        Effect.gen(function* () {
          const status = yield* (yield* SessionStatus.Service).get(sessionID)
          return status.type === "busy" ? (true as const) : undefined
        }),
        "target never became busy",
        "15 seconds",
      )
      return fiber
    })()
  }

  it5("holds normal messages while busy, then flushes FIFO on idle", () =>
    Effect.gen(function* () {
      const { llm } = yield* cfg5.useServerConfig(cfg5.providerCfg)
      const mail = yield* AgentMail.Service
      const promptSvc = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const target = yield* sessions.create({
        agent: "build",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })
      const senderSession = yield* sessions.create({})
      const fiber = yield* makeBusy(target.id)

      const one = yield* mail.send({ sender: { sessionID: senderSession.id, agent: "main" }, to: "build", text: "one" })
      const two = yield* mail.send({ sender: { sessionID: senderSession.id, agent: "main" }, to: "build", text: "two" })
      expect(one.delivery).toBe("queued")
      expect(two.delivery).toBe("queued")

      yield* promptSvc.cancel(target.id)
      yield* Fiber.await(fiber)
      yield* llm.push(reply().text("got one").stop())
      yield* llm.push(reply().text("got two").stop())
      yield* pollWithTimeout(
        Effect.gen(function* () {
          const messages = yield* sessions.messages({ sessionID: target.id })
          const gotOne = messages.some((m) => m.parts.some((p) => p.type === "agent-message" && p.text === "one"))
          const gotTwo = messages.some((m) => m.parts.some((p) => p.type === "agent-message" && p.text === "two"))
          return gotOne && gotTwo ? (true as const) : undefined
        }),
        "pending messages never flushed after idle",
        "20 seconds",
      )
    }),
  60_000,
  )

  it5("urgent message is admitted while the target is busy", () =>
    Effect.gen(function* () {
      const { llm } = yield* cfg5.useServerConfig(cfg5.providerCfg)
      const mail = yield* AgentMail.Service
      const promptSvc = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const target = yield* sessions.create({
        agent: "build",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })
      const senderSession = yield* sessions.create({})
      const fiber = yield* makeBusy(target.id)

      const delivered = yield* mail.send({
        sender: { sessionID: senderSession.id, agent: "main" },
        to: target.id,
        text: "priority note",
        urgent: true,
      })
      expect(delivered.delivery).toBe("delivered")
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
        "urgent message never admitted",
        "20 seconds",
      )
      yield* promptSvc.cancel(target.id)
      yield* Fiber.await(fiber)
    }),
  60_000,
  )

  it6("autoWake=false queues instead of waking an idle target", () =>
    Effect.gen(function* () {
      const mail = yield* AgentMail.Service
      const sessions = yield* Session.Service
      const target = yield* sessions.create({ agent: "build" })
      const senderSession = yield* sessions.create({})
      const queued = yield* mail.send({
        sender: { sessionID: senderSession.id, agent: "main" },
        to: "build",
        text: "hold on",
      })
      expect(queued.delivery).toBe("queued")
    }),
  )
})

describe("agent_message tool", () => {
  it5("routes a tool call made by the LLM to the target session", () =>
    Effect.gen(function* () {
      const { llm } = yield* cfg5.useServerConfig(cfg5.providerCfg)
      const promptSvc = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const target = yield* sessions.create({
        agent: "worker-two",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })
      const main = yield* sessions.create({})
      yield* llm.push(reply().tool("agent_message", { to: "worker-two", text: "hello worker" }).stop())
      yield* llm.push(reply().text("sent").stop())
      yield* promptSvc.prompt({
        sessionID: main.id,
        parts: [{ type: "text", text: "tell worker-two hello" }],
      })
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
        "20 seconds",
      )
    }),
  60_000,
  )
})

describe("AgentMail registry", () => {
  it5("indexes sessions by agent and resolves lookups with filters", () =>
    Effect.gen(function* () {
      const mail = yield* AgentMail.Service
      const sessions = yield* Session.Service
      const worker = yield* sessions.create({ agent: "registry-worker" })
      yield* sessions.create({})

      const entries = yield* mail.lookup({ agent: "registry-worker" })
      expect(entries.length).toBe(1)
      expect(entries[0]?.sessionID).toBe(worker.id)
      expect(entries[0]?.status).toBe("idle")
    }),
  )

  it5("deregisters sessions when they are deleted", () =>
    Effect.gen(function* () {
      const mail = yield* AgentMail.Service
      const sessions = yield* Session.Service
      const target = yield* sessions.create({ agent: "registry-busy" })
      const busy = yield* mail.lookup({ agent: "registry-busy" })
      expect(busy[0]?.status).toBe("idle")

      yield* sessions.remove(target.id)
      const after = yield* mail.lookup({ agent: "registry-busy" })
      expect(after.length).toBe(0)
    }),
  30_000,
  )

  it5("send() resolves names through the registry", () =>
    Effect.gen(function* () {
      const mail = yield* AgentMail.Service
      const sessions = yield* Session.Service
      const worker = yield* sessions.create({ agent: "registry-send" })
      const senderSession = yield* sessions.create({})
      const result = yield* mail.send({
        sender: { sessionID: senderSession.id, agent: "main" },
        to: "registry-send",
        text: "registry hi",
      })
      expect(result.target).toBe(worker.id)
    }),
  )
})
