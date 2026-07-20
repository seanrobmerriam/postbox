<img src="assets/postbox2.png" alt="postbox logo" width="1200">   
# Postbox

Postbox is a message broker for agent-to-agent communication. Every agent gets a durable inbox, delivery acknowledgment is tied to the receiving agent's own workflow checkpoints, and messages that keep failing get routed to a dead-letter queue instead of retrying forever.

## What "exactly-once" actually means here

Most brokers consider a message delivered once the consumer's HTTP call returns `200`. That's fine until agent B crashes between receiving the message and finishing the work it was supposed to trigger. The broker thinks everything went great; the work never happened.

Postbox closes that gap by splitting acknowledgment into two phases, the second of which is bound to the consumer's own checkpoint:

1. **Claim.** B's runtime pulls (or is pushed) a message. It's marked `claimed` with a lease and hidden from other consumers, but it stays in the inbox.
2. **Commit.** B acks only after it has durably recorded that the message's effect took hold. The ack carries a caller-supplied `checkpoint_token`, which Postbox stores as an audit trail. Postbox doesn't validate what the token means — only that one was supplied and is non-empty. The point is that you can't ack out of laziness; you have to point at *something* durable.

If a lease expires without a commit — crash, hang, whatever — the message becomes visible again and gets redelivered. This is where I should be honest about the term: "exactly-once" is really at-least-once delivery plus idempotent processing, and Postbox doesn't pretend otherwise. Every message carries a stable `message_id`, and the idempotency ledger (`is_committed(mailbox_id, message_id)`) lets a consumer ask "have I already handled this?" before doing expensive work. Redelivery after a crash becomes a no-op instead of double-processing.

So, the division of labor: exactly-once needs at-least-once delivery from the broker, idempotent processing on the consumer, and an idempotency check that is itself durable. Postbox gives you the first and the third. The second is on you.

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

SQLite via `sqlx` in WAL mode. All storage sits behind a `MailboxStore` trait with an in-memory fake for fast unit tests. Every state transition — claim, commit, release, dead-letter — is a single SQLite transaction, so there's no read-then-write window where two consumers could both think they claimed the same message.

Concurrent writers are serialized at the application level with a `tokio::sync::Mutex` around the SQLite pool. That takes SQLite's concurrency edge cases out of the contract entirely and keeps the SQL simple. WAL still gives us crash safety.

### Lease expiry

A background sweeper (one periodic scan, not a timer per message) wakes on a configurable interval and moves abandoned leases back to `pending` without touching `attempt_count`. A single task keeps memory bounded, and it makes crash recovery free: a fresh sweeper on startup reclaims whatever expired while the process was down.

### Two front ends, one core

HTTP (axum) and gRPC (tonic) both live in `postbox-grpc`. They run on split ports rather than sharing one. HTTP/1.1 and HTTP/2 get treated differently by load balancers, proxies, and observability tooling, so splitting keeps the operational story simple and avoids the `tower::steer::Steer` indirection you'd need for single-port multi-protocol serving.

There's also an MCP server (`postbox-mcp`, built on `rmcp`) that exposes mailbox operations as seven tools plus a `mailbox://{agent_id}/pending` resource. An LLM agent can send, check, claim, and ack its own messages straight from a chat loop without any custom glue.

Both front ends call into `postbox-core` and nothing else.

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

## A two-agent handoff, with a crash in the middle

This walkthrough exists as a runnable test in `crates/postbox-core/tests/` (`fifo_ordering_holds_across_redelivery`), so you can follow along or just run it.

### 1. Alice gets told to do something

```
POST /v1/mailboxes/alice
POST /v1/mailboxes/alice/send
    { "from": "bob",
      "payload_base64": "${BASE64(\"fetch-weather\")}" }
→ { "message_id": "01HABCDEF...", ... }
```

### 2. Alice claims the message

```
POST /v1/mailboxes/alice/claim
    { "claimer_id": "alice-runtime", "lease_duration_ms": 5000 }
→ { "message": { ... }, "lease_expires_at_ms": 1700000005000 }
```

Alice's runtime now holds an exclusive lease. No other consumer can see this message.

### 3. Crash — Alice dies before committing

Alice pulled the message, started her work, then her process died before writing her durable checkpoint. From Postbox's perspective the message is still `claimed`; nothing changes until the lease expires.

### 4. The sweeper reclaims the expired lease

Five seconds later the sweeper wakes up:

```
SELECT message_id, lease_expires_at_ms FROM messages
 WHERE status = 'claimed' AND lease_expires_at_ms <= <now>;
```

It atomically moves every expired row back to `pending` (`status='pending', lease_expires_at_ms=NULL, claimed_by=NULL`). Note that `attempt_count` does **not** get bumped here — it only increments when a consumer explicitly fails a claim.

### 5. Alice restarts and picks the same work back up

```
POST /v1/mailboxes/alice/claim
    { "claimer_id": "alice-runtime", "lease_duration_ms": 5000 }
→ { "message": { "attempt_count": 2, "message_id": "01HABCDEF...", ... } }
```

`attempt_count` is now `2` — second *claim cycle*, not second send. The ULID hasn't changed.

### 6. Alice finishes, persists her checkpoint, then commits

Alice records `"waitpoint:alice:weather-fetch:ok"` in her own state store, then tells Postbox:

```
POST /v1/messages/01HABCDEF.../commit
    { "claimer_id": "alice-runtime",
      "checkpoint_token": "waitpoint:alice:weather-fetch:ok" }
→ 204 No Content
```

The token is opaque to Postbox — it just gets recorded as an audit field on the message. The trust model is simply that the consumer pointed at something durable before acking.

### 7. Idempotency check, in case the commit reply got lost

If the commit reply never made it back to Alice, her runtime might retry. Before redoing the work, it asks:

```
GET /v1/mailboxes/alice/committed/01HABCDEF...
→ { "committed": true }
```

The idempotency ledger is durable. The `(mailbox_id, message_id)` pair is written inside the same SQLite transaction as the `commit()` itself, so once you've seen `committed=true`, skipping a redelivery of that `message_id` is safe.

### 8. The poison path, if she'd kept crashing

Say Alice crashed five times in a row. Each `claim` would have bumped `attempt_count`, and once `attempt_count > max_attempts`, the next `claim` dead-letters the message with `poison_reason: "max_attempts_exceeded"`. The DLQ record carries the full failure history:

```json
{
  "message_id": "01HABCDEF...",
  "mailbox_id": "alice",
  "attempt_count": 6,
  "poison_reason": "max_attempts_exceeded",
  "failure_history": [ { ... }, { ... }, ... ]
}
```

To re-inject it for another try:

```
POST /v1/dead-letters/01HABCDEF.../replay
    { "replayed_by": "ops" }
→ 201 Created
  { "message_id": "01HANEWID...",  // new ULID
    "attempt_count": 0,
    "headers": { "replayed_from": "01HABCDEF...", "replayed_by": "ops" } }
```

The original DLQ record sticks around for audit.

## What counts as "poisoned"

A message gets dead-lettered on exactly one of three paths, and the DLQ record tells you which via `poison_reason`:

| Path                  | `poison_reason`           | When                                                  |
|-----------------------|---------------------------|-------------------------------------------------------|
| Max attempts          | `max_attempts_exceeded`   | `attempt_count` reaches `mailbox.max_attempts`        |
| Consumer refused      | `permanent_failure`       | `release(..., PermanentFailure)` from a consumer      |
| Bad payload on arrival| `validation_failed`       | `reject_validation(...)` before any claim             |

So when you're digging through the DLQ, you can tell "kept crashing the consumer" apart from "was malformed on arrival" apart from "consumer looked at it and said no."

## Running the binary

```bash
cargo run -p postbox -- \
  --db sqlite://$PWD/postbox.db \
  --http 127.0.0.1:8080 \
  --grpc 127.0.0.1:50051 \
  --sweep-interval 5s
```

| Flag                 | Default              | Description |
|----------------------|----------------------|-------------|
| `--db`               | `sqlite::memory:`    | SQLite URL. Use `sqlite://./postbox.db` for persistence. |
| `--http`             | `127.0.0.1:8080`     | HTTP listen address, or `off` to disable. |
| `--grpc`             | `127.0.0.1:50051`    | gRPC listen address, or `off` to disable. |
| `--sweep-interval`   | `5s`                 | Lease recovery sweep interval, or `off` to disable. |
| `--mcp`              | `off`                | `stdio` to serve MCP over stdin/stdout. |
| `-v`                 | `info`               | Verbosity (`-v`=debug, `-vv`=trace). |

Environment variables mirror the flags with a `POSTBOX_` prefix (e.g. `POSTBOX_HTTP=off`).

### Graceful shutdown

On `SIGINT` or `SIGTERM` the binary:

1. Stops accepting new HTTP / gRPC connections.
2. Cancels pending MCP stdio reads.
3. Drains in-flight state transitions, persisting any lease updates that were mid-process.
4. Stops the sweeper last, so it gets one final pass before exit.

Every transition is a single SQLite transaction, so the worst case on an ungraceful exit is losing requests that were still in flight — never a torn state mutation.

## Testing

```
cargo test --workspace
```

The integration tests stand up real HTTP and gRPC servers in-process on ephemeral ports, real MCP servers over a `tokio::io::duplex` transport, and run both the in-memory and SQLite-backed `MailboxStore` against the same behavior suite.

Property tests (`proptest`) cover the five invariants from the spec:

1. A message is visible to at most one active claim at a time.
2. A message goes `pending -> claimed -> committed`, or `pending -> claimed -> pending` (lease expiry, retry), or `-> dead_lettered` — never backward from `committed`, never skipping `claimed`.
3. `attempt_count` increments exactly once per claim, and never on lease-driven visibility restoration alone.
4. FIFO mailboxes preserve per-sender order even across redeliveries; unordered mailboxes never require it.
5. A message that hits `max_attempts` is dead-lettered exactly once and stops being claimable.

There's also a concurrency stress test (`concurrent_claimers_never_double_claim_a_message_while_lease_is_active`) that throws 16 simultaneous claimers at 100 messages and asserts every one is committed exactly once.

## Out of scope

Multi-broker clustering and replication, pub-sub fan-out (Postbox is point-to-point mailbox delivery, not a topic broker), auth beyond agent-id scoping, and payload encryption at rest. Postbox delivers and tracks messages durably; it's not trying to be a general event bus.

## License

Dual-licensed under MIT OR Apache-2.0.
