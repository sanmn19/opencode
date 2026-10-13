# Agent-to-Agent Messaging (A2A) — Design

Date: 2026-10-09
Status: Draft for review
Owner: sanmn19

## Problem

When a `task` subagent runs, the parent session's next provider turn cannot start
until the subagent finishes: the `task` tool blocks on the child session's drain
(`packages/opencode/src/tool/task.ts:328-346`, `Effect.raceFirst(background.wait, …)`).
The parent can only queue silent user input in the meantime — the assistant thread
itself is frozen.

Beyond subagents, there is no way for *independent* agents (e.g. two sibling
sessions coordinating on a shared build server) to talk to each other at all.

## Goals

1. While a subagent executes, the main agent's session stays responsive: the
   subagent notifies the main agent on completion and the main agent processes it
   in a real provider turn.
2. General agent-to-agent communication: any two sessions in an instance can
   exchange messages, including siblings with no parent/child relationship.
3. Coordination over shared resources happens in conversation; opencode provides
   reliable delivery, not resource locks.
4. Works across clients: TUI, web/desktop (`packages/app`, `packages/session-ui`),
   and third-party clients such as a phone client hitting the HTTP API.

## Non-goals

- No built-in resource lock/lease primitives (coordination is prompt-level).
- No cross-instance or cross-project messaging.
- No broadcast/topics/group chats in v1 (1:1 addressing only).
- No port of the `task` tool to the V2 session core (tracked separately).

## Decisions (from brainstorming session)

- **Scope:** full A2A incl. sibling/unrelated sessions, from the start.
- **Delivery semantics:** mailbox + steer. Messages land durably and are processed
  at the receiving agent's next turn boundary; senders may mark a message
  `urgent` to steer an in-flight turn.
- **Coordination:** messaging only.
- **TUI surface:** transcript messages plus minimal client-side unread badges.
- **Approach:** dedicated instance-level A2A service (Approach B), modeled on the
  existing instance-scoped `BackgroundJob`/`background/` pattern.

## Architecture

### New service: `AgentMail`

- Location: `packages/opencode/src/agent-mail/agent-mail.ts`
- Effect v4 service in repo conventions: self-export at bottom, layer via
  `InstanceState` (one copy per open instance), following the module shape and
  Effect rules in `packages/opencode/AGENTS.md`.
- Responsibilities:
  - Address resolution (sessionID or agent name → target session).
  - Delivery state machine per message (admit now / hold FIFO / steer).
  - Rate limiting and size validation.
  - Idempotence (dedupe by messageID).

### Message model

An agent message is a **part in the receiving session's transcript** — the
transcript remains the single source of truth. New part type in
`packages/schema`:

```ts
{
  type: "agent-message",
  synthetic: true,
  sender: { sessionID: SessionID, agent: string },
  to: SessionID,
  replyTo?: MessageID,   // optional thread correlation
  urgent?: boolean,
  text: string
}
```

- Persisted through existing `Session.updatePart` → file-backed `Storage`
  (`storage/session/part/...`). No database migrations.
- Transported to clients via the existing `message.part.updated` SSE event —
  **no new event types**.

### Delivery policy

| Target state | Normal message | Urgent message |
| --- | --- | --- |
| Idle | Admit immediately → triggers a provider turn | Admit immediately |
| Busy (mid-turn) | Held FIFO in mailbox; delivered when run drains | Admit at next turn boundary (steer) |
| Unknown target / self-send | Bounce error to sender (as tool error) | — |

- Busy/idle observed via the existing `SessionStatus` stream (`session/status`).
- Admission calls `SessionPrompt.prompt` — the same entry point the server routes
  and the TUI use for mid-run user messages (join/wake semantics already proven).
- FIFO ordering when multiple messages queue for the same target.
- Subagent completion notification (`task.ts` `inject`/`notify`, lines 227-265)
  migrates to send through `AgentMail`, so background-subagent completions carry
  the same sender metadata and badge semantics as ordinary A2A messages.

### Addressing

- `to` accepts a sessionID (precise) or an agent name (e.g. `"build-agent"`),
  resolved to the most recently active session running that agent in the
  instance (via `SessionStatus`). Ambiguous names bounce with the candidate list
  in the error so the sending model can disambiguate.
- Instance-scoped only; sessions in other projects are not addressable.

## Surfaces

### Tool: `agent_message` (builtin)

- Registered in `packages/opencode/src/tool/registry.ts`.
- Parameters: `to: string`, `text: string`, `replyTo?: MessageID`, `urgent?: boolean`.
- Returns resolved target sessionID + delivery outcome (`delivered` | `queued`).
- Sender identity derives from the executing session/agent context — never from
  tool args (no spoofing).
- Availability and permission follow the existing per-agent `tools` config;
  a read-only agent can disable `agent_message` there.

### Protocol / server

- **No new routes required** for agent-initiated messaging.
- One addition for external clients: `POST /session/:sessionID/agent-message`
  (sender = an existing session in the instance, so replies route back symmetric).
- No new SSE event types.

### TUI / web / mobile clients

- Render the `agent-message` part type with sender styling — `packages/tui`
  (`PART_MAPPING`, `routes/session/index.tsx`) and `packages/session-ui` for
  web/desktop. Any HTTP client that renders session parts gains support; phone
  clients need only client-side additions.
- Unread badge: client-side — count unseen `agent-message` parts per session
  (tracked in the sync context), badge in the session list / toast when
  unfocused, cleared on view. Server stays ignorant of read state.

## Safety

- **Prompt-injection hygiene:** payload text only (no embeds/files). Every
  delivered message is wrapped with an unambiguous header,
  `[agent-message from=<agent> session=<id>]`; clients read structured metadata
  from the part, models read the header.
- **Loop guard:** per (sender → target) rate cap, default 30 msgs/min
  (configurable). When tripped, the send fails back to the sender.
- **Size cap:** message text ≤ 16 KB (tool parameter validation).
- **Auto-wake control:** `a2a.autoWake` (default `true`). With `false`,
  deliveries wait like queue-mode input; setups that must not burn tokens from
  agent chatter can disable wakeups.

## Rollout

- Config flag `a2a.enabled`, default `false` while soaking; flip to `true`
  in a later release (mirrors `experimentalBackgroundSubagents` handling in
  `packages/opencode/src/effect/runtime-flags.ts`).
- `task`'s background completion injection moves behind the same `AgentMail`
  path once it lands (kept behavior-compatible with the current experimental flow).

## Testing

No mocks (repo rule) — exercise real `SessionPrompt`/`Storage`:

- `AgentMail` delivery state machine: idle-admit, busy-queue-FIFO,
  urgent-steer-mid-run, bounce (unknown target, self-send), rate limit, size cap.
- `agent_message` tool round-trip incl. sender-identity derivation.
- Route test for `POST /session/:sessionID/agent-message` via the existing
  HttpApi test patterns in `packages/opencode/src/server/`.
- Manual TUI checklist: rendering, badge, badge-clear-on-view, urgent steer.
- Run from `packages/opencode` (`bun test`), not repo root.

## Files expected to change

| Area | Path |
| --- | --- |
| Schema | `packages/schema/src/*` (new part type) |
| Core service | `packages/opencode/src/agent-mail/agent-mail.ts` (new) |
| Runtime flags | `packages/opencode/src/effect/runtime-flags.ts` |
| Tool | `packages/opencode/src/tool/agent-message.ts` (new), `src/tool/registry.ts` |
| Task integration | `packages/opencode/src/tool/task.ts` (inject/notify → AgentMail) |
| Server route | `packages/opencode/src/server/routes/instance/httpapi/groups/session.ts`, `handlers/session.ts` |
| TUI | `packages/tui/src/routes/session/index.tsx`, `src/component/*`, sync context badge |
| Web shared UI | `packages/session-ui/src/*` (part renderer) |
| Config | `packages/opencode/src/config/*` (`a2a.enabled`, `a2a.autoWake`) |
