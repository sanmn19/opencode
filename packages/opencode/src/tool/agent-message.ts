import { AgentMail } from "@/agent-mail/agent-mail"
import { MessageID } from "@/session/schema"
import * as Tool from "./tool"
import { Cause, Effect, Schema } from "effect"

const DESCRIPTION = [
  "Send a message to another agent's session — a peer agent you're coordinating with, or your parent agent.",
  "Delivery: if the recipient is idle it wakes and processes the message in a new turn;",
  "if it is busy, the message is held and delivered when its current run finishes.",
  "Use urgent to have a busy recipient process the message at its next turn boundary instead.",
  "Not sure who to message? Call agent_lookup first to discover available agents and their status.",
  "Coordinate shared resources (ports, GPUs, files) through these messages before acting on them,",
  "and reply with replyTo when answering a specific incoming message.",
].join("\n")

const Parameters = Schema.Struct({
  to: Schema.String.annotate({ description: "Recipient: a session ID or an agent name" }),
  text: Schema.String.annotate({ description: "Message body (text only)" }),
  replyTo: Schema.optional(Schema.String).annotate({ description: "Message ID this message replies to" }),
  urgent: Schema.optional(Schema.Boolean).annotate({
    description: "Process at the recipient's next turn boundary even while busy",
  }),
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
            Effect.map((result): Tool.ExecuteResult<{ target?: string; delivery?: string }> => ({
              title: `agent_message → ${result.target}`,
              metadata: { target: result.target, delivery: result.delivery },
              output:
                result.delivery === "delivered"
                  ? `Delivered to session ${result.target}. It will be processed at the recipient's next turn boundary.`
                  : `Queued for session ${result.target}; delivered when its current run finishes.`,
            })),
            Effect.catchCause((cause) => {
              const error = Cause.squash(cause)
              return Effect.succeed({
                title: "agent_message",
                metadata: { delivery: "bounced" },
                output: error instanceof Error ? error.message : String(error),
              })
            }),
          ),
    }
  }),
)

export * as AgentMessage from "./agent-message"
