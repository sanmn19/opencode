import { expect, test } from "bun:test"
import { Schema } from "effect"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { MessageID } from "../../src/session/schema"

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
  expect(part.replyTo).toBe(MessageID.make("msg_01JT0000000000000000000000"))
  expect(part.urgent).toBe(true)
})
