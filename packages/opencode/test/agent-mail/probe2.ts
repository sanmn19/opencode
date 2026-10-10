import { Cause, Effect, Exit } from "effect"
import { SessionStatus } from "@/session/status"
import { AgentMail } from "@/agent-mail/agent-mail"
import { EventV2Bridge } from "@/event-v2-bridge"
import { Session } from "@/session/session"
import { makeMail5 } from "./harness"
import { withTmpdirInstance } from "../fixture/fixture"

const program = withTmpdirInstance()(
  Effect.gen(function* () {
    const bridge = yield* EventV2Bridge.Service
    const statuses = yield* SessionStatus.Service
    const sessions = yield* Session.Service
    const mail = yield* AgentMail.Service
    const session = yield* sessions.create({})

    const out: string[] = []
    const unsub = yield* bridge.listen((event) =>
      Effect.gen(function* () {
        out.push(`src-listen ${event.type}`)
        return
      }),
    )
    yield* Effect.addFinalizer(() => unsub)

    const queued = yield* mail.send({
      sender: { sessionID: session.id, agent: "main" },
      to: "nope-agent-zzz",
      text: "x",
    }).pipe(Effect.exit)
    out.push(`send ${Exit.isSuccess(queued) ? "ok" : JSON.stringify(queued).slice(0, 160)}`)

    yield* statuses.set(session.id, { type: "busy" })
    yield* statuses.set(session.id, { type: "idle" })
    out.push(`session ${session.id}`)

    yield* Effect.promise(() => Bun.write("/tmp/opencode/probe2-out.txt", out.join("\n")))
  }),
)

const started = program.pipe(Effect.scoped, Effect.provide(makeMail5()), Effect.exit)
const result = await Effect.runPromise(started as never).catch((e) => String(e))
await Bun.write("/tmp/opencode/probe2-status.txt", `status ${String(result).slice(0, 200)}`)
