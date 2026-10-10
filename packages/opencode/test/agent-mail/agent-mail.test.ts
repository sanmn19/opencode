import { describe, expect } from "bun:test"
import { Cause, Effect, Exit } from "effect"
import { Session } from "@/session/session"
import { AgentMail } from "@/agent-mail/agent-mail"
import { it5 } from "./harness"

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
