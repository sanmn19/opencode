import { Context, Effect, Schema } from "effect"
import { MessageID, SessionID } from "@/session/schema"
import type { SessionV1 } from "@opencode-ai/core/v1/session"

export interface SendInput {
  to: string
  text: string
  urgent?: boolean
  replyTo?: MessageID
  agent?: string
  sender: { sessionID: SessionID; agent: string }
}

export interface DeliveryResult {
  target: SessionID
  delivery: "delivered" | "queued"
}

export interface RegistryEntry {
  sessionID: SessionID
  agent: string
  directory?: string
  title?: string
  status: "busy" | "idle" | "retry"
  lastActive: number
}

export interface RegistryQuery {
  agent?: string
  directory?: string
  status?: "busy" | "idle" | "retry"
}

export interface Admitter {
  prompt(input: {
    sessionID: SessionID
    messageID?: MessageID
    agent?: string
    parts: Array<Omit<SessionV1.AgentMessagePartInput, "id">>
  }): Effect.Effect<void, unknown>
}

export type SendError =
  | MessagingDisabled
  | UnknownTarget
  | SelfSend
  | TooLarge
  | RateLimited
  | TargetMissing

export interface Interface {
  readonly send: (input: SendInput) => Effect.Effect<DeliveryResult, SendError>
  readonly setAdmitter: (admitter: Admitter) => Effect.Effect<void>
  readonly lookup: (query: RegistryQuery) => Effect.Effect<Array<RegistryEntry>>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/AgentMail") {}

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

export * as AgentMail from "./agent-mail"
