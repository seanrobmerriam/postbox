# Postbox — Exactly-Once Agent Mailbox

Postbox is a message broker purpose-built for agent-to-agent communication.
It gives every agent a durable, persistent inbox, ties delivery acknowledgment
to the receiving agent's own workflow checkpoints (not just "the socket
accepted the bytes"), and routes poisoned messages to dead-letter queues
instead of retrying forever.

## What "exactly-once" means here

A naive broker considers a message delivered once the consumer's HTTP call
returns `200`. But if agent B crashes between receiving the message and
finishing whatever it was supposed to do with it, that "successful delivery"
is a lie — the work never happened.

Postbox closes that gap by making acknowledgment **two-phase and checkpoint-bound**:

1. **Claim** — B's runtime pulls (or is pushed) a message; it's marked
   `claimed` with a lease and becomes invisible to other consumers, but is
   NOT removed from the inbox.
2. **Commit** — B acknowledges only after recording its own durable
   checkpoint that the message's effect has taken hold. The ack call carries
   a caller-supplied `checkpoint_token` that Postbox stores as an audit
   trail — Postbox does not validate the token's meaning, only that one was
   supplied and is non-empty, enforcing that callers can't ack out of
   laziness without pointing to *something* durable.

If a lease expires without a commit (crash, hang), the message becomes
visible again and is redelivered — this is where **exactly-once** really means
**at-least-once delivery + idempotent processing**, and Postbox makes that
honest rather than hiding it: every message carries a stable `message_id`,
and Postbox's idempotency ledger (`is_committed(mailbox_id, message_id)`)
lets a consumer check "have I already handled this" before doing expensive
work, so redelivery after a crash is a no-op rather than double-processing.

**Do not oversell "exactly-once"** as something achievable at the network
layer alone. It requires: at-least-once delivery from the broker, idempotent
processing on the consumer, and an idempotency check that is itself durable.
Postbox provides the first and the third; the consumer must provide the
second.

## Architecture

```
            ┌────────────────────────────────────────────┐
            │              postbox binary                 │
            │                                             │
       ┌────│───────┐         ┌────────────┐              │
       │ HTTP :8080│         │ gRPC :50051│              │
       │  (axum)   │         │  (tonic)   │              │
       └────│───────┘         └─────┬──────┘              │
            │                       │                     │
            └──────┬────────────────┘                     │
                   │                                      │
                   ▼                                      │
       ┌──────────────────────┐  ┌─────────────────┐     │
       │   postbox-core       │  │  postbox-mcp    │     │
       │  (MailboxStore trait)│  │  (MCP, rmcp)    │     │
       │                      │  │                 │     │
       │ ┌─ MemoryStore ──┐   │  │  7 tools + 1    │     │
       │ │   fake (tests) │   │  │  resource       │     │
       │ └────────────────┘   │  └─────────────────┘     │
       │ ┌─ SqliteStore ──┐   │                          │
       │ │  WAL, 1 tx     │   │                          │
       │ └────────────────┘   │                          │
       │   ↑ sweeper task     │                          │
       └──────────────────────┘                          │
            │                                            │
            └─────────────  SQLite file (WAL) ───────────┘
```

### Storage

SQLite via `sqlx` in WAL mode; all storage behind a `MailboxStore` trait
with an in-memory fake for fast unit tests. Every state transition (claim,
commit, release, dead-letter) is a single SQLite transaction — there is no
read-then-write race where two consumers could both believe they claimed the
same message.

Concurrent writers are serialized at the application level via a
`tokio::sync::Mutex` around the SQLite pool. This removes the SQLite
concurrency edge cases from the contract and keeps the SQL simple. WAL still
gives us crash safety.

### Lease expiry

A background sweeper task (periodic scan, not one timer per message) wakes up
at a configurable interval and restores abandoned leases to `pending` without
bumping `attempt_count`. A single task bounds memory and gives us crash
recovery for free: a fresh sweeper on startup reclaims whatever expired
while the process was down.

### Two front ends, no duplicated business logic

- **HTTP (axum)** and **gRPC (tonic)** live in `postbox-grpc`. We chose
  **split ports** because HTTP REST and gRPC have different HTTP versions
  (1.1 vs 2) that load balancers, proxies, and observability tools treat
  differently. Splitting keeps the operational story cleaner and avoids the
  `tower::steer::Steer` indirection needed for multi-protocol single-port
  serving.
- **MCP server** (`postbox-mcp`, using `rmcp`) exposes mailbox operations as
  seven tools and the `mailbox://{agent_id}/pending` resource so an LLM
  agent can send/check/claim/ack its own messages directly from a chat loop
  without custom glue.

Both front ends call into `postbox-core` only.

## API at a glance

### HTTP (REST + JSON)

| Method  | Route                                                      | Description |
|---------|-----------------------------------------------------------|-------------|
| `POST`  | `/v1/mailboxes/{agent_id}`                               | Ensure mailbox |
| `GET`   | `/v1/mailboxes/{agent_id}`                               | Get mailbox |
| `POST`  | `/v1/mailboxes/{agent_id}/send`                          | Send a message |
| `GET`   | `/v1/mailboxes/{agent_id}/peek`                          | Peek without claiming |
| `POST`  | `/v1/mailboxes/{agent_id}/claim`                          | Claim next visible message |
| `GET`   | `/v1/mailboxes/{agent_id}/committed/{message_id}`        | Idempotency check |
| `GET`   | `/v1/mailboxes/{agent_id}/dead-letters`                 | List DLQ |
| `POST`  | `/v1/dead-letters/{message_id}/replay`                   | Replay a DLQ record |
| `POST`  | `/v1/messages/{message_id}/commit`                       | Commit (requires `checkpoint_token`) |
| `POST`  | `/v1/messages/{message_id}/release`                      | Release (transient\|permanent) |
| `POST`  | `/v1/messages/{message_id}/reject-validation`           | Pre-claim poison reject |
| `GET`   | `/healthz`                                                | Liveness |

### MCP tools

| Tool                  | Args                                                                       |
|-----------------------|----------------------------------------------------------------------------|
| `send_message`        | `to_agent`, `payload_base64`, `headers`?, `delay_ms`?, `from_agent`?      |
| `check_inbox`         | `agent_id`, `max`?                                                         |
| `claim_message`       | `agent_id`, `claimer_id`, `lease_duration_ms`?                            |
| `commit_message`      | `message_id`, `claimer_id`, `checkpoint_token`                              |
| `release_message`     | `message_id`, `claimer_id`, `failure_kind` (`transient`\|`permanent`), `note`? |
| `list_dead_letters`   | `mailbox_id`, `filter`?, `limit`?                                          |
| `replay_dead_letter`  | `message_id`, `target_mailbox`?, `replayed_by`                             |

### MCP resource

| URI                                  | Resource |
|--------------------------------------|----------|
| `mailbox://{agent_id}/pending`       | JSON document of visible messages for `agent_id`. |

## A worked two-agent handoff (with a simulated crash)

This walkthrough is also runnable as a `pub async fn` test in
`crates/postbox-core/tests/` (`fifo_ordering_holds_across_redelivery`).

### Step 1: Alice is told to do something

```
POST /v1/mailboxes/alice
POST /v1/mailboxes/alice/send
    { "from": "bob",
      "payload_base64": "${BASE64(\"fetch-weather\")}" }
→ { "message_id": "01HABCDEF...", ... }
```

### Step 2: Alice claims the message

```
POST /v1/mailboxes/alice/claim
    { "claimer_id": "alice-runtime", "lease_duration_ms": 5000 }
→ { "message": { ... }, "lease_expires_at_ms": 1700000005000 }
```

Alice's runtime now holds an exclusive lease. No other consumer can see
this message.

### Step 3: simulate crash — Alice's process dies before committing

Alice pulled the message, started her work, then her process died before
writing her durable checkpoint. From Postbox's perspective the message is
still `claimed`; nothing changes until the lease expires.

### Step 4: the sweeper reclaims the expired lease

After 5 seconds the sweeper wakes up:

```
SELECT message_id, lease_expires_at_ms FROM messages
 WHERE status = 'claimed' AND lease_expires_at_ms <= <now>;
```

It atomically moves every expired row back to `pending`
(`status='pending', lease_expires_at_ms=NULL, claimed_by=NULL`). Critically,
`attempt_count` is **not** incremented here — only when the consumer
explicitly fails the claim is `attempt_count` bumped.

### Step 5: Alice's restart reclaims the same work

Alice's runtime comes back up and asks for work again:

```
POST /v1/mailboxes/alice/claim
    { "claimer_id": "alice-runtime", "lease_duration_ms": 5000 }
→ { "message": { "attempt_count": 2, "message_id": "01HABCDEF...", ... } }
```

`attempt_count` is now `2` — this is the second *claim cycle*, not the second
send. The ULID is still stable.

### Step 6: Alice actually finishes, persists her checkpoint, and commits

Alice records `"waitpoint:alice:weather-fetch:ok"` in her state store, then
tells Postbox:

```
POST /v1/messages/01HABCDEF.../commit
    { "claimer_id": "alice-runtime",
      "checkpoint_token": "waitpoint:alice:weather-fetch:ok" }
→ 204 No Content
```

The `checkpoint_token` is opaque to Postbox — it just records it as an audit
field on the message. The lender of trust here is "the consumer pointed at
something durable."

### Step 7: idempotency check (if the commit reply was lost)

If the commit reply got lost on the way back to Alice, her runtime might
retry. Before re-doing the work, it asks Postbox:

```
GET /v1/mailboxes/alice/committed/01HABCDEF...
→ { "committed": true }
```

The idempotency ledger is durable — Postbox commits the
`(mailbox_id, message_id)` pair inside the same SQLite transaction as the
`commit()` itself, so once `committed=true` returns, redelivery of the same
`message_id` is safe to skip.

### Step 8: poison path (if it kept crashing)

If Alice had crashed 5 times in a row, each `claim` would have bumped
`attempt_count`. When `attempt_count > max_attempts`, the next `claim` itself
would dead-letter the message with `poison_reason: "max_attempts_exceeded"`,
and the DLQ record carries the full failure history:

```json
{
  "message_id": "01HABCDEF...",
  "mailbox_id": "alice",
  "attempt_count": 6,
  "poison_reason": "max_attempts_exceeded",
  "failure_history": [ { ... }, { ... }, ... ]
}
```

To re-inject for another attempt:

```
POST /v1/dead-letters/01HABCDEF.../replay
    { "replayed_by": "ops" }
→ 201 Created
  { "message_id": "01HANEWID...",  // new ULID
    "attempt_count": 0,
    "headers": { "replayed_from": "01HABCDEF...", "replayed_by": "ops" } }
```

The original DLQ record is preserved for audit.

## What counts as "poisoned"

A message is dead-lettered on exactly one of three paths. The DLQ record
distinguishes them via `poison_reason`:

| Path                  | `poison_reason`           | When                                                  |
|-----------------------|---------------------------|-------------------------------------------------------|
| Max attempts          | `max_attempts_exceeded`   | `attempt_count` reaches `mailbox.max_attempts`        |
| Consumer refused      | `permanent_failure`       | `release(..., PermanentFailure)` from a consumer      |
| Bad payload on arrival| `validation_failed`       | `reject_validation(...)` before any claim             |

This way a developer inspecting the DLQ can tell "kept crashing the consumer"
from "was malformed on arrival" from "consumer looked at it and refused."

## Running the binary

```bash
cargo run -p postbox -- \
  --db sqlite://$PWD/postbox.db \
  --http 127.0.0.1:8080 \
  --grpc 127.0.0.1:50051 \
  --sweep-interval 5s
```

Flags:

| Flag                 | Default              | Description |
|----------------------|----------------------|-------------|
| `--db`               | `sqlite::memory:`    | SQLite URL. Use `sqlite://./postbox.db` for persistence. |
| `--http`             | `127.0.0.1:8080`     | HTTP listen address, or `off` to disable. |
| `--grpc`             | `127.0.0.1:50051`    | gRPC listen address, or `off` to disable. |
| `--sweep-interval`   | `5s`                 | Lease recovery sweep interval, or `off` to disable. |
| `--mcp`              | `off`                | `stdio` to serve MCP over stdin/stdout. |
| `-v`                 | `info`               | Verbosity (`-v`=debug, `-vv`=trace). |

Environment variables mirror flags with a `POSTBOX_` prefix
(e.g. `POSTBOX_HTTP=off`).

### Graceful shutdown

`SIGINT` and `SIGTERM` cause the binary to:

1. Stop accepting new HTTP / gRPC connections.
2. Cancel pending MCP stdio reads.
3. Drain in-flight state transitions to a quiescent state, persisting any
   lease updates that were mid-process.
4. Stop the sweeper last so it can do one final pass before exit.

Everything is a single SQLite transaction per transition, so the worst case
on ungraceful exit is a loss of in-flight requests already in progress, never
a torn state mutation.

## Testing

```
cargo test --workspace
```

The integration tests stand up real HTTP and gRPC servers in-process on
ephemeral ports, real MCP servers via a `tokio::io::duplex` transport, and
both an in-memory and SQLite-backed `MailboxStore` against the same behavior
suite.

Property tests (`proptest`) exercise the five invariants from the spec:

1. A message is visible to at most one active claim at a time.
2. A message transitions `pending -> claimed -> committed` or
   `pending -> claimed -> pending` (lease expiry, retry) or
   `-> dead_lettered`, never backward from `committed`, never skipping
   `claimed`.
3. `attempt_count` increments exactly once per claim, never on
   lease-driven visibility restoration alone until re-claimed.
4. FIFO mailboxes preserve per-sender order even across redeliveries;
   unordered mailboxes never require it.
5. A message that reaches `max_attempts` is dead-lettered exactly once and
   stops being claimable.

The concurrency stress test (`concurrent_claimers_never_double_claim_a_message_while_lease_is_active`)
spawns 16 simultaneous claimers against 100 owed messages and asserts every
message is committed exactly once.

## Out of scope

Multi-broker clustering/replication, pub-sub fan-out (Postbox is
point-to-point mailbox delivery, not a topic broker),
authentication/authorization beyond agent-id scoping, and payload encryption
at rest. Postbox delivers and tracks messages durably; it is not a general
event bus.

## License

Dual-licensed under MIT OR Apache-2.0.