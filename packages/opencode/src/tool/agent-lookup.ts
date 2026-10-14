import { AgentMail } from "@/agent-mail/agent-mail"
import * as Tool from "./tool"
import { Effect, Schema } from "effect"

const DESCRIPTION = [
  "Discover other agent sessions in this instance that you can message with agent_message.",
  "Use this BEFORE agent_message whenever you don't know the exact agent name or session ID,",
  "or when you want to check whether a peer is busy or idle before messaging it.",
  "Agents register automatically — any running agent session appears here with its current status.",
].join("\n")

const Parameters = Schema.Struct({
  name: Schema.optional(Schema.String).annotate({ description: "Filter by agent name (exact match)" }),
  directory: Schema.optional(Schema.String).annotate({ description: "Filter by project directory (exact match)" }),
  status: Schema.optional(Schema.Literals(["busy", "idle", "retry"])).annotate({
    description: "Filter by current status",
  }),
})

export const AgentLookupTool = Tool.define(
  "agent_lookup",
  Effect.gen(function* () {
    const mail = yield* AgentMail.Service
    return {
      description: DESCRIPTION,
      parameters: Parameters,
      execute: (params: Schema.Schema.Type<typeof Parameters>) =>
        mail
          .lookup({
            agent: params.name,
            directory: params.directory,
            status: params.status,
          })
          .pipe(
            Effect.map((entries) => ({
              title: "agent_lookup",
              metadata: { count: entries.length },
              output:
                entries.length === 0
                  ? "No matching agent sessions found."
                  : entries
                      .map((entry) =>
                        [
                          `- ${entry.agent} (${entry.status})`,
                          `  session: ${entry.sessionID}`,
                          entry.directory ? `  directory: ${entry.directory}` : undefined,
                          entry.title ? `  title: ${entry.title}` : undefined,
                        ]
                          .filter(Boolean)
                          .join("\n"),
                      )
                      .join("\n"),
            })),
          ),
    }
  }),
)

export * as AgentLookup from "./agent-lookup"
