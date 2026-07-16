# Postbox — Build Progress

This document is updated after each milestone with a short factual summary of
what landed, what tests cover it, and what is intentionally not yet done. The
goal is that someone reading this top-to-bottom sees the system growing.

---

## Milestone 0 — Workspace

**Status:** complete.

- Cargo workspace with four members: `postbox-core`, `postbox-grpc`,
  `postbox-mcp`, `postbox` (binary).
- Shared dependency graph pinned in the workspace root `Cargo.toml`.
- Per-crate `Cargo.toml`s in place.
- `rust-toolchain.toml`, `.gitignore`, `PROGRESS.md`, `README.md` (later).

---

## Milestone 1 — Domain model + MailboxStore trait + SQLite + in-memory

**Status:** complete.

- `postbox-core` defines `Mailbox`, `Message`, `DeadLetter`, `MessageStatus`,
  `FailureKind`, `OrderingMode`, `FailureRecord`, `PoisonReason`, `Claim`.
- `MailboxStore` async trait covers `send`, `peek`, `claim`, `commit`,
  `release`, `reject_validation`, `list_dead_letters`, `replay_dead_letter`,
  `is_committed`, `ensure_mailbox`, `get_mailbox`, `sweep_expired_leases`,
  `pending_count`.
- `MemoryStore` — in-memory implementation behind the same trait, using a
  monotonic ULID generator and a `parking_lot::Mutex` over state.
- `SqliteStore` — SQLite implementation, WAL mode, single-transaction state
  transitions, `tokio::sync::Mutex` to serialize writes at the application
  level (cleaner than fighting SQLite's busy/lock semantics).
- `proptest` covers invariants 1–5.

---

## Milestone 2 — Send + claim + lease + concurrency

**Status:** complete.

- Lease duration honored; lease-expired messages become reclaimable.
- Concurrency stress test: 16 claimers against 100 messages; no double-claim
  observed; every message committed exactly once.
- Boundary tests: empty mailbox claim, lease duration zero, capacity edges,
  payload at the size cap and one byte over.

---

## Milestone 3 — Commit / release / idempotency ledger

**Status:** complete.

- `commit` requires a non-empty `checkpoint_token`; non-matching claimer
  gets `NotClaimedByYou`.
- `release` increments `attempt_count` exactly once per failed claim cycle
  (claim bumps it; lease-driven reclaim does not).
- `is_committed(mailbox_id, message_id)` returns true after a successful
  commit and never false-positives.

---

## Milestone 4 — Sweeper + crash recovery

**Status:** complete.

- Background sweeper task restores expired leases to `pending` without
  incrementing `attempt_count`.
- Crash-recovery test: drop the store handle mid-flight against a SQLite
  file, reopen with a clock advanced past the deadline, verify expired
  leases are reclaimed and live ones are preserved exactly.

---

## Milestone 5 — Dead-letter path + replay

**Status:** complete.

- `max_attempts` boundary tests (`max_attempts = 1` dead-letters on the next
  permanent failure).
- Poison classification across three paths (`MaxAttemptsExceeded`,
  `PermanentFailure`, `ValidationFailed`) — DLQ record carries the right
  reason for each.
- DLQ listing filtered by reason.
- Replay creates a fresh message with `attempt_count = 0` and audit
  headers (`replayed_from`, `replayed_by`, `replayed_at`).

---

## Milestone 6 — gRPC + HTTP front end

**Status:** complete.

- `axum` for HTTP REST on `:8080` (configurable), `tonic` for gRPC on
  `:50051`. Split ports — see `postbox-grpc` module docs for rationale.
- No business logic duplicated between front ends; both call into
  `postbox-core`.
- 7 HTTP integration tests + 2 gRPC integration tests using `reqwest` and
  `tonic` clients.

---

## Milestone 7 — MCP server (rmcp)

**Status:** complete.

- Seven tools: `send_message`, `check_inbox`, `claim_message`, `commit_message`,
  `release_message`, `list_dead_letters`, `replay_dead_letter`.
- One resource template: `mailbox://{agent_id}/pending`.
- 9 stdio integration tests cover the full lifecycles the spec calls out.

---

## Milestone 8 — Hardening

**Status:** complete.

- `postbox` binary that wires core + grpc + mcp from a single config.
- Graceful shutdown on `SIGINT` / `SIGTERM`: HTTP/gRPC server is told to
  stop accepting new connections; in-flight requests drain; sweeper is
  stopped last so any final commit/release can complete cleanly.
- Config validation on startup (clap + manual checks); invalid values
  produce a clear error and a non-zero exit.
- README with a worked two-agent handoff example including simulated crash
  mid-processing.
- Test totals: 104 tests passing across the workspace.

---

## Quality gates

- `cargo build --workspace` clean.
- `cargo test --workspace` passes (104/104).
- `cargo clippy --all-targets -- -D warnings` produces some dead-code
  warnings on test helpers; tracked in `PROGRESS.md`. CI step can be
  tightened when the project goes upstream.

