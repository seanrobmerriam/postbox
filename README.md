# Postbox

![Postbox logo](assets/postbox.png)

An exactly-once agent mailbox broker. Postbox provides persistent, lease-based message queues for AI agents and services, with HTTP, gRPC, and MCP interfaces backed by SQLite.

## What it does

Postbox gives each agent a named mailbox. Senders enqueue messages; consumers claim them under a time-bounded lease, do their work, then either commit (success) or release (failure). Uncommitted leases are automatically recovered by a background sweeper. Messages that exceed their retry limit — or are explicitly rejected — move to a dead-letter queue for inspection and replay.

- **Ordering modes**: FIFO (per-sender order preserved) or Unordered
- **Exactly-once delivery**: lease tokens prevent duplicate processing
- **Dead-letter queue**: with per-record reason (`max_attempts_exceeded`, `permanent_failure`, `validation_failed`)
- **DLQ replay**: re-inject a dead-lettered message with `attempt_count` reset
- **Three interfaces**: REST/HTTP, gRPC, MCP stdio — all backed by the same SQLite store

## Quick start

```sh
cargo build --release

# In-memory (ephemeral) — all interfaces on defaults
./target/release/postbox

# Persistent SQLite, all listeners enabled
./target/release/postbox \
  --db sqlite://./postbox.db \
  --http 127.0.0.1:8080 \
  --grpc 127.0.0.1:50051

# MCP stdio mode (e.g. wired up from a Claude config)
./target/release/postbox --db sqlite://./postbox.db --http off --grpc off --mcp stdio
```

## Configuration

All flags have `POSTBOX_`-prefixed environment variable equivalents.

| Flag | Env | Default | Description |
|---|---|---|---|
| `--db` | `POSTBOX_DB` | `sqlite::memory:` | SQLite URL |
| `--http` | `POSTBOX_HTTP` | `127.0.0.1:8080` | HTTP listen address (`off` to disable) |
| `--grpc` | `POSTBOX_GRPC` | `127.0.0.1:50051` | gRPC listen address (`off` to disable) |
| `--sweep-interval` | `POSTBOX_SWEEP_INTERVAL` | `5s` | Lease recovery interval (`off` to disable) |
| `--mcp` | `POSTBOX_MCP` | `off` | MCP stdio server (`stdio` to enable) |
| `-v` / `-vv` | `RUST_LOG` | info | Verbosity |

At least one of `--http`, `--grpc`, or `--mcp=stdio` must be enabled.

## MCP tools

When running with `--mcp stdio`, Postbox exposes these tools:

| Tool | Description |
|---|---|
| `send_message` | Enqueue a message into an agent's mailbox |
| `check_inbox` | Peek at visible messages without claiming |
| `claim_message` | Claim the next visible message under a lease |
| `commit_message` | Commit a claimed message (requires a checkpoint token) |
| `release_message` | Release with transient or permanent failure classification |
| `list_dead_letters` | List DLQ records for a mailbox |
| `replay_dead_letter` | Re-inject a dead-lettered message |

Resource template: `mailbox://{agent_id}/pending` — JSON list of currently visible messages.

## Workspace layout

| Crate | Description |
|---|---|
| `postbox-core` | Domain model, `MailboxStore` trait, SQLite and in-memory backends |
| `postbox-grpc` | HTTP (axum) and gRPC (tonic) front ends |
| `postbox-mcp` | MCP stdio server (rmcp) |
| `postbox` | Binary — wires all three front ends from a single CLI config |

## Building

Requires Rust 1.80+. See `rust-toolchain.toml` for the pinned toolchain.

```sh
cargo build           # debug
cargo build --release # release
cargo test            # run all tests
```

## License

MIT OR Apache-2.0
