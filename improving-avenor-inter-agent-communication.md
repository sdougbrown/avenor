# Improving Avenor Inter-Agent Communication

Based on a thorough analysis of pi-intercom (a Pi extension for 1:1 session messaging) and avenor's existing broker/channels infrastructure, this document identifies gaps and proposes concrete improvements.

---

## Current State: What Avenor Has Today

Avenor already ships a **channels** layer (commit `3d93743`, PR #48) for inter-agent communication:

- **Broker `/send` endpoint** — routes messages between registered runs by `run_id`
- **`avenor_send` / `avenor_upsend` MCP tools** — exposed to Claude Code agents via the claude-channel sidecar, enabling both horizontal (peer) and vertical (parent→child / child→parent) messaging
- **`PollAgentMessages`** — polls the in-process broker every 2s for `agent_message` control messages and injects them as `<channel>`-wrapped prompts with untrusted-content disclaimers
- **Run ID propagation** — supervisor passes `broker_url` and `parent_run_id` to spawned children so they can find each other
- **Team-run seeding** — pre-phase output can be injected as a seed message before team members start
- **Pretty-printing** — channel messages rendered with `📨 agent-name` attribution in the OpenCode plugin

The picture is a solid foundation — the routing, the trust boundary, the channel-wrap injection pipeline — but the interaction patterns are limited to **fire-and-forget** plain-text messages.

---

## What pi-intercom Does Better (10 Key Patterns)

| Pattern | pi-intercom | Avenor today | Gap |
|---|---|---|---|
| **Structured ask/wait** | `ask` sends with `expectsReply`, blocks until a matching reply, timeout configurable | `avenor_send` only; no built-in ask/reply correlation or blocking wait | ⚠️ Critical |
| **Reply threading** | `reply` auto-targets the current turn context or single pending ask; `send` infers it | No concept of pending asks or reply-to threading | ⚠️ Critical |
| **Session presence** | Live status (`idle`/`thinking`/`tool:name`), context %, token counts, model, cwd | No discovery: runs can't see what peers are alive, their state, or capacity | ⚠️ Critical |
| **Message lifecycle** | Full receipt chain: `receiver_received → acknowledged → injected`; dedup by message ID | HTTP 200 from broker is the only delivery signal; no end-to-end tracking | 🟡 Important |
| **Mailbox for offline delivery** | Queues messages for disconnected sessions (24h), delivers on reconnect by session ID or name+directory | No mailbox: if the target finishes, message disappears | 🟡 Important |
| **Attachments** | Typed attachments (`file`, `snippet`, `context`) carried inline | Pure text only | 🟡 Nice-to-have |
| **Cancel / supersede** | Explicit `cancel` and `supersede` operations with end-to-end visibility | No cancel path for in-flight messages | 🟡 Important |
| **Extension bus** | Namespace-based extension-to-extension messaging; owner election; compare-and-swap state | No extension channel — only agent runs can send | 🟢 Nice-to-have |
| **Live context visibility** | Presence data includes `contextPct`, `contextTokens`, `contextWindow` — senders can gauge recipient capacity | None — run about to send can't know peer is at 95% context | 🟢 Nice-to-have |
| **Cross-process broker** | Unix socket / named pipe; 1:1 SSH sessions across different terminals | In-process HTTP only; team members in separate processes can't message each other | 🟡 Important |

---

## Proposed Improvements

### Phase 1: Enrich the Message Model (Minimal schema changes, big impact)

**Current `AgentMessage`** (`internal/runtime/broker/message.go`):
```go
type AgentMessage struct {
    From      string `json:"from"`
    FromRunID string `json:"from_run_id"`
    Message   string `json:"message"`
    Role      string `json:"role"`
}
```

**Proposed enriched message:**
```go
type AgentMessage struct {
    ID           string          `json:"id"`                     // stable message ID for dedup + correlation
    From         string          `json:"from"`
    FromRunID    string          `json:"from_run_id"`
    ToRunID      string          `json:"to_run_id"`
    Message      string          `json:"message"`
    Role         string          `json:"role"`
    ReplyTo      string          `json:"reply_to,omitempty"`      // correlation: this is an answer to msg ID X
    ExpectsReply bool            `json:"expects_reply,omitempty"` // correlation: sender is blocking on a reply
    Attachments  []Attachment    `json:"attachments,omitempty"`   // typed payloads
    Supersedes   string          `json:"supersedes,omitempty"`    // replaces a previous message
}

type Attachment struct {
    Type     string `json:"type"`             // "file", "snippet", "context"
    Name     string `json:"name"`
    Content  string `json:"content"`
    Language string `json:"language,omitempty"`
}
```

**Impact:**
- The `ExpectsReply` + `ReplyTo` pair gives us **blocking ask/reply threading** with zero new endpoints — just enriched payload.
- `ID` enables deduplication and cancellation.
- Modified `avenor_send` tool gains optional `expects_reply` and `reply_to` parameters.
- New `avenor_ask` tool wraps `avenor_send` with `expects_reply=true` and blocks on a reply channel added to `RunState`.

---

### Phase 2: Add Ask/Reply to the Broker

**Broker changes** (`internal/runtime/broker/broker.go`):

- **Add `ReplyWaiter` to `RunState`** — a map of `messageID → chan Envelope` so a blocking sender can be matched to an incoming reply.
- **Add `handleAsk` path** — When an incoming `agent_message` has `expects_reply=true`, the broker registers a pending ask edge in `RunState.PendingAsks[messageID] → { fromRunID, toRunID, createdAt }`.
- **Add `handleReply` path** — When an incoming `agent_message` has `reply_to`, the broker finds the pending ask edge, pushes the reply to the original sender's `ReplyWaiter` channel, and removes the edge.
- **Timeout on asks** — `RunState` periodically prunes expired ask edges (configurable, default 10m). When an ask expires, it sends a `timeout` notification back to the waiting channel.
- **Mutual-ask guard** — Reject a new ask when a reverse ask edge exists between the same pair (both waiting on each other = deadlock).

**No new endpoints required** — the existing `/send` endpoint already carries the message payload. The broker inspects `reply_to` and `expects_reply` from the envelope and acts accordingly.

**But we do need a new `/poll_control` variant for "wait for a specific reply":**
- `/wait_reply` — POST with `run_id` and `waiting_for_message_id`. Long-polls (up to the ask timeout) for a reply matching that message ID. Returns immediately if the reply is already queued.

---

### Phase 3: Session Presence & Discovery

**Goal:** A run can list its peers and see who's alive, what they're doing, and how loaded they are.

**Mechanism:**
- Add a `/sessions` endpoint to the in-process broker that returns all registered runs with their metadata.
- Expand `RunState` with a `SessionInfo` struct:
  ```go
  type SessionInfo struct {
      RunID         string `json:"run_id"`
      RunLabel      string `json:"run_label,omitempty"`
      Backend       string `json:"backend,omitempty"`
      Model         string `json:"model,omitempty"`
      Dir           string `json:"dir,omitempty"`
      Status        string `json:"status"`          // "idle", "thinking", "tool:name"
      ContextPct    *int   `json:"context_pct,omitempty"`    // 0-100 or null
      ContextTokens *int   `json:"context_tokens,omitempty"` // raw tokens or null
      ContextWindow int    `json:"context_window,omitempty"`
      StartedAt     int64  `json:"started_at"`
      LastSeen      int64  `json:"last_seen"`
  }
  ```
- Backends periodically push context-usage metadata into `RunState` via a new `UpdateSessionInfo` broker call.
- New `avenor_peers` tool exposed via channeltools MCP server — returns a formatted list of peer runs.
- Optionally, a `avenor_subscribe` notification can stream `session_joined` / `session_left` / `presence_update` events back through the channel wrapping mechanism.

**Broker becomes a registry, not just a message router.** This is a small change but a big capability unlock.

---

### Phase 4: Cross-Process Broker (Optional but Transformative)

Avenor's broker is in-process HTTP — fine for runs under the same `avenor stable` supervisor. But it means **a run in one supervisor can't message a run in another supervisor** or, more importantly, a **standalone Pi session running pi-intercom**.

Two options:

**Option A: Broker proxy in the supervisor**
- The stable supervisor exposes a `/external-broker` endpoint on its control socket.
- External processes (other supervisors, pi-intercom sessions) connect here.
- Messages targeting a `run_id` known to a different supervisor are forwarded.
- No single shared broker; the supervisor is the authority for its own runs but can proxy to others.

**Option B: Broker federation via Unix sockets**
- Introduce a lightweight external-facing broker process (`avenor bridge`) that speaks a simplified pi-intercom-like protocol over a Unix socket.
- Runs under different supervisors can discover each other through the bridge.
- The bridge is optional — default is in-process, bridge is for multi-supervisor topologies.

**Recommendation:** Start with Option A (lightweight, no new binary) and let demand drive a bridge.

---

### Phase 5: Mailbox for Offline Delivery

When a message targets a `run_id` that has finished:
- **Short-lived retention (5m default):** The broker holds the message in the target run's mailbox.
- **Reconnect redelivery:** If a new run registers with the same `run_id`, queued messages are flushed to it.
- **Expiry:** After retention period, the ask edge expires and the sender (if waiting) gets a `delivery_failed` instead of hanging.
- **No persistence:** Mailbox is in-memory, lost on broker restart. Explicit design choice — no durable storage dependency.

---

### Phase 6: Extension Bus (Inspired by pi-intercom's `extension-bus-v1`)

Non-agent components (tools, plugins, custom MCP servers) can register namespace-scoped channels on the broker:

```go
type ExtensionCapability struct {
    Namespace    string `json:"namespace"`    // e.g. "code-review/v1"
    OwnerEligible bool   `json:"owner_eligible"`
}
```

- Registered during run start or via a new `/register_extension` endpoint.
- Elects one "owner" per namespace (oldest registration wins).
- Owner can write shared state via `/extension_state_commit` with compare-and-swap.
- Any capable extension can publish to all peers via `/extension_publish`.
- No agent session involvement — extensions talk directly through the broker.

This is lower priority but would make avenor's broker a general-purpose coordination fabric, not just an agent message bus.

---

## Summary: What to Build and In What Order

| Priority | What | Why |
|---|---|---|
| **P1** | Enrich `AgentMessage` with `id`, `reply_to`, `expects_reply`, `attachments` | Unlocks every other pattern |
| **P1** | Add ask-edge tracking + reply matching to broker `RunState` | Blocking ask/wait without polling |
| **P1** | New `avenor_ask` MCP tool (wraps `avenor_send` + waits) | Agent-facing blocking communication |
| **P1** | Add `/wait_reply` long-poll endpoint | Lets backends block on replies without busy-polling |
| **P2** | Add `SessionInfo` to `RunState` + `/sessions` endpoint | Peer discovery and presence |
| **P2** | New `avenor_peers` MCP tool | Agent-facing peer listing |
| **P2** | Add context-usage push from backends to broker | Live capacity visibility |
| **P2** | Mailbox for offline delivery (5 min default) | Don't lose messages to transient finish |
| **P2** | `avenor_reply` MCP tool (auto-target pending asks) | Easy reply for agents without reconstructing IDs |
| **P3** | Auto-thread replies (`send` infers pending ask target) | pi-intercom parity — one less step for agents |
| **P3** | Message cancellation via `/cancel_message` | Agents can cancel their own asks |
| **P3** | Mutual-ask deadlock guard | Prevents two agents from blocking each other forever |
| **P4** | Cross-process broker proxy in stable supervisor | Multi-supervisor messaging |
| **P4** | Extension bus (namespace-scoped channels) | Non-agent inter-component communication |
| **P4** | Cross-process bridge binary | pi-intercom interop |

---

## How It Relates to Issue #167

Issue #167 (based on commit history analysis) relates to gaps in how spawned children communicate with their supervisor and each other — specifically around run identity, lifecycle synchronization, and the reliability of parent→child routing. The proposals above directly address these:

1. **`reply_to` + `expects_reply`** turns fire-and-forget into correlated request-response, so the supervisor knows when a child is done asking and gets the answer routed back.
2. **Session presence** lets the supervisor see that a child is still alive, thinking, or blocked — no more guessing from sentinel file state.
3. **Ask timeout and mailbox** mean a child won't hang forever if the supervisor finishes, and messages to a restarted supervisor aren't silently lost.
4. **Cancellation** lets the supervisor tell a child "I already answered this — stop waiting."

These are the patterns that pi-intercom has proven work in production for inter-session coordination, adapted to avenor's run-centric model.
