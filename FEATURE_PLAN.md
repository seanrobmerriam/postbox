# Feature Plan

Ordered by implementation effort and dependency. Each section describes the
feature, its design sketch, files touched, and acceptance criteria.

---

## 1. Priority ordering

**What**: Wire the existing `priority` field on `Message` into claim ordering.
Add a third `OrderingMode::Priority` that picks the highest-priority visible
message rather than the oldest one.

**Design**:
- Extend `OrderingMode` in `types.rs` with a `Priority` variant.
- In `sqlite.rs` `claim()`: when mode is `Priority`, sort candidates by
  `priority DESC, created_at_ms ASC, message_id ASC` instead of FIFO.
- In `memory.rs` `claim()`: apply the same sort in Rust.
- Expose the new mode in HTTP (`ensure_mailbox` accepts `"priority"` string),
  gRPC (proto `ordering_mode` field), and MCP (`ensure_mailbox` tool arg).
- Add `validate_priority` to reject values outside a documented range
  (suggested: `i32::MIN..=i32::MAX` is fine, but document the convention
  that higher values = higher priority).

**Files**: `types.rs`, `sqlite.rs`, `memory.rs`, `http.rs`, `grpc.rs`,
`server.rs`, `postbox.proto`

**Acceptance criteria**:
- A mailbox configured as `priority` delivers the highest-priority message
  first, regardless of insertion order.
- Equal-priority messages are delivered FIFO within that priority band.
- Existing `fifo` and `unordered` mailboxes are unaffected.

---

## 2. Message TTL / automatic expiration

**What**: Messages that are never claimed before a caller-supplied deadline
are automatically moved to the DLQ with a new `PoisonReason::Expired`.

**Design**:
- Add an optional `expires_at_ms` column to the `messages` table (migration).
- Add `ttl: Option<Duration>` to `SendRequest`; the store converts it to an
  absolute timestamp at insert time.
- Extend `sweep_expired_leases` (or add a sibling `sweep_expired_messages`)
  to dead-letter all `pending` rows where `expires_at_ms <= now`.
- Add `PoisonReason::Expired` to `error.rs` / `types.rs`; map it everywhere
  `PoisonReason` is matched.
- Expose `ttl_ms` in HTTP send body, gRPC `SendMessageRequest`, and MCP
  `send_message` tool.

**Files**: `types.rs`, `error.rs`, `sqlite.rs` (migration + sweep),
`memory.rs`, `sweeper.rs`, `http.rs`, `grpc.rs`, `server.rs`,
`postbox.proto`

**Acceptance criteria**:
- A message with `ttl = 1s` is in the DLQ with reason `expired` after the
  sweeper runs past its deadline and was never claimed.
- A message with no TTL is unaffected.
- The DLQ record preserves the original payload.

---

## 3. Fanout / broadcast send

**What**: Send one logical message to multiple target mailboxes atomically. If
any insert fails (capacity, validation) the entire fanout is rolled back.

**Design**:
- Add a `fanout_send(req: FanoutRequest) -> Result<Vec<Message>, PostboxError>`
  method to `MailboxStore`.
- `FanoutRequest` contains `targets: Vec<String>`, `sender_id`, `payload`,
  `headers`, `priority`, `delay`.
- SQLite backend: open one transaction, insert one row per target mailbox,
  do capacity and payload checks inside the same transaction.
- Memory backend: hold the `Mutex` guard for the duration of all inserts so
  the fanout is atomic.
- HTTP: `POST /v1/fanout` with body `{ "targets": [...], ... }`, returns
  `201 Created` with `{ "messages": [...] }`.
- gRPC: `FanoutRequest` / `FanoutResponse` RPC.
- MCP: `fanout_message` tool.

**Files**: `store.rs`, `types.rs`, `sqlite.rs`, `memory.rs`, `http.rs`,
`grpc.rs`, `server.rs`, `postbox.proto`

**Acceptance criteria**:
- All target mailboxes receive an identical copy of the message.
- If one target is at capacity, no mailbox receives the message.
- Each resulting message has a distinct `message_id`.

---

## 4. Admin / inspection API

**What**: Read-only endpoints for observing queue state across all mailboxes.
Useful for dashboards, debugging, and health checks.

**Design**:
- Add `list_mailboxes(limit, cursor) -> Result<Vec<Mailbox>, PostboxError>` to
  `MailboxStore` (cursor-based pagination on `agent_id`).
- Add `mailbox_stats(agent_id) -> Result<MailboxStats, PostboxError>` returning
  pending/claimed/committed/dead-lettered counts and oldest-pending timestamp.
- HTTP routes:
  - `GET /v1/mailboxes` — paginated list with `?limit=&after=`.
  - `GET /v1/mailboxes/{agent_id}/stats` — per-mailbox counters.
- gRPC: `ListMailboxes` and `GetMailboxStats` RPCs.
- MCP: `list_mailboxes` and `mailbox_stats` tools.

**Files**: `store.rs`, `types.rs`, `sqlite.rs`, `memory.rs`, `http.rs`,
`grpc.rs`, `server.rs`, `postbox.proto`

**Acceptance criteria**:
- `list_mailboxes` returns all mailboxes with correct pagination.
- `mailbox_stats` counts match actual row counts in each status bucket.
- Both endpoints are read-only (no state mutation).

---

## 5. Prometheus metrics

**What**: Export counters and gauges for key queue events so operators can
alert on DLQ growth, stale leases, and throughput.

**Design**:
- Add `metrics` crate dependency (workspace-level).
- Instrument `postbox-core` at key transitions: increment counters in
  `send()`, `claim()`, `commit()`, `release()`, and `sweep_expired_leases()`.
  Label each counter with `mailbox_id` where appropriate.
- Key metrics:
  - `postbox_messages_sent_total{mailbox_id}`
  - `postbox_messages_claimed_total{mailbox_id}`
  - `postbox_messages_committed_total{mailbox_id}`
  - `postbox_messages_released_total{mailbox_id, kind}` (transient/permanent)
  - `postbox_messages_dead_lettered_total{mailbox_id, reason}`
  - `postbox_leases_swept_total`
  - `postbox_mailbox_pending_messages{mailbox_id}` (gauge, from `pending_count`)
- Expose a `GET /metrics` endpoint in the HTTP front end that renders
  Prometheus text format via `metrics-exporter-prometheus`.

**Files**: `Cargo.toml` (workspace deps), `postbox-core/src/*.rs`,
`postbox-grpc/src/http.rs`, `postbox/src/main.rs`

**Acceptance criteria**:
- `GET /metrics` returns valid Prometheus text.
- After sending N messages, `postbox_messages_sent_total` increments by N.
- DLQ counter increments on dead-letter regardless of reason.

---

## 6. DLQ retention and purge

**What**: Dead letters accumulate forever today. Add a configurable retention
window; the sweeper prunes DLQ rows older than the window, and expose an
explicit purge API for operators.

**Design**:
- Add per-mailbox `dlq_retention: Option<Duration>` to `MailboxConfig` and
  the `mailboxes` table.
- In the sweeper: after sweeping expired leases, delete DLQ rows where
  `dead_lettered_at_ms < now - dlq_retention` for mailboxes that have a
  configured retention.
- Add `purge_dead_letters(mailbox_id, before: SystemTime) -> Result<usize>`
  to `MailboxStore` for explicit, operator-triggered purge.
- HTTP: `DELETE /v1/mailboxes/{agent_id}/dead-letters?before_ms=`.
- gRPC: `PurgeDeadLetters` RPC.
- MCP: `purge_dead_letters` tool.

**Files**: `store.rs`, `types.rs`, `sqlite.rs` (migration + sweeper hook),
`memory.rs`, `sweeper.rs`, `http.rs`, `grpc.rs`, `server.rs`,
`postbox.proto`

**Acceptance criteria**:
- DLQ rows older than `dlq_retention` are deleted by the sweeper.
- `purge_dead_letters(before=T)` deletes all DLQ rows with
  `dead_lettered_at < T` and returns the count deleted.
- DLQ rows younger than the retention window are untouched.

---

## 7. gRPC streaming claim

**What**: A server-streaming RPC that pushes messages to the consumer as they
become visible, eliminating polling.

**Design**:
- Add `StreamClaim(StreamClaimRequest) returns (stream ClaimResponse)` to the
  proto.
- The server loops: `claim()` → if `Some`, stream the message; if `None`,
  wait `poll_interval_ms` (sent in the request) then retry. Continue until
  the client cancels or `max_messages` is reached.
- Honour lease duration from the request; default to the mailbox's configured
  `lease_duration`.
- Back-pressure: only send the next claim after the previous one has been
  committed or released (requires the client to call `commit`/`release` and
  the server to track outstanding leases per stream).

**Simpler variant (v1)**: Stream without back-pressure — just push each
claimed message immediately and let the client deal with ordering. The full
back-pressure variant is a v2 concern.

**Files**: `postbox.proto`, `grpc.rs`

**Acceptance criteria**:
- Client receives messages in order as they are enqueued.
- Cancelling the stream does not leave any message in a permanently claimed
  state (lease expiry handles recovery).
- Works under the existing SQLite write-lock — no deadlock.

---

## 8. Postgres backend

**What**: A `PostgresStore` implementing `MailboxStore` backed by Postgres,
enabling horizontal scaling beyond a single process.

**Design**:
- Add `postbox-pg` crate to the workspace.
- Re-use the same SQL schema shape; replace `INTEGER PRIMARY KEY` with
  `BIGSERIAL`, `TEXT` ULIDs can stay as `TEXT` or use `UUID` (ULID is
  lexicographically sortable as text).
- Replace the application-level `write_lock` with `SELECT ... FOR UPDATE
  SKIP LOCKED` for claim (the standard Postgres queue pattern).
- Atomic claim via a CTE:
  ```sql
  WITH cte AS (
    SELECT message_id FROM messages
    WHERE mailbox_id = $1 AND status = 'pending' AND visible_at <= now()
    ORDER BY created_at, message_id
    LIMIT 1
    FOR UPDATE SKIP LOCKED
  )
  UPDATE messages SET status = 'claimed', ... FROM cte WHERE ...
  RETURNING *;
  ```
- The sweeper works unchanged against the trait.
- Wire into the binary via a `--backend postgres --db <connection-string>` flag.
- Gate behind a `postgres` Cargo feature so the SQLite-only build remains
  minimal.

**Files**: new `crates/postbox-pg/` crate, `Cargo.toml`, `postbox/src/main.rs`

**Acceptance criteria**:
- `postbox-pg` passes the same property test suite as `postbox-core`'s
  `MemoryStore` and `SqliteStore`.
- Two `postbox` processes backed by the same Postgres DB do not deliver the
  same message to two consumers simultaneously.
- Graceful degradation: if Postgres is unavailable, `send()` returns
  `PostboxError::Storage`; the binary logs and exits cleanly.
