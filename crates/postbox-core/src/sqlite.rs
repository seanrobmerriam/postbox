//! SQLite-backed implementation of [`MailboxStore`].
//!
//! All state transitions run inside a single SQL transaction so the
//! read-then-write race that would let two consumers believe they have both
//! claimed the same message is impossible. The database is opened in WAL
//! mode for durability across crashes. Concurrent writers are serialized at
//! the application level via a `tokio::sync::Mutex` — this removes the
//! SQLite concurrency edge cases from the contract and keeps the SQL simple.
//!
//! See `tests/integration_sqlite.rs` for end-to-end tests against a temp
//! SQLite file.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteJournalMode, SqliteSynchronous};
use sqlx::{Row, SqlitePool};
use ulid::{Generator, Ulid};

use crate::clock::Clock;
use crate::error::PostboxError;
use crate::store::MailboxStore;
use crate::types::{
    validate_agent_id, Claim, DeadLetter, FailureKind, FailureRecord, Mailbox, MailboxConfig,
    Message, MessageStatus, OrderingMode, PoisonReason, SendRequest,
};

/// SQLite-backed [`MailboxStore`].
pub struct SqliteStore {
    pool: SqlitePool,
    clock: Arc<dyn Clock>,
    /// In-process monotonic ULID generator so messages generated within
    /// the same millisecond are strictly ordered.
    ulid_gen: Mutex<Generator>,
    /// Serializes all write operations so concurrent claimers contend at
    /// the application level rather than relying on SQLite's WAL locking
    /// + busy_timeout. WAL still gives us crash safety; the lock just
    /// removes the SQLite concurrency surface from the contract.
    write_lock: tokio::sync::Mutex<()>,
}

/// Configuration for opening a [`SqliteStore`].
#[derive(Debug, Clone)]
pub struct SqliteStoreConfig {
    /// Database URL. Use `"sqlite::memory:"` for tests, or
    /// `"sqlite://path/to/file.db?mode=rwc"` for a file.
    pub url: String,
    /// Maximum pool size. Default 16.
    pub max_connections: u32,
}

impl SqliteStoreConfig {
    /// Configured for an in-memory database with a sensible default pool size.
    /// Useful for tests.
    pub fn memory() -> Self {
        Self {
            url: "sqlite::memory:".to_string(),
            max_connections: 4,
        }
    }
}

impl SqliteStore {
    /// Open the store, run migrations, and return the ready-to-use backend.
    pub async fn connect(
        config: SqliteStoreConfig,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, PostboxError> {
        let opts: SqliteConnectOptions = config
            .url
            .parse()
            .map_err(|e: sqlx::Error| PostboxError::Storage(format!("connect: {e}")))?;
        let opts = opts
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(30));
        let pool = SqlitePoolOptions::new()
            .max_connections(config.max_connections)
            // busy_timeout must be set on every connection; the connection
            // option only applies at first acquire otherwise.
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    sqlx::query("PRAGMA busy_timeout = 30000;")
                        .execute(&mut *conn)
                        .await?;
                    sqlx::query("PRAGMA journal_mode = WAL;")
                        .execute(&mut *conn)
                        .await?;
                    Ok(())
                })
            })
            .connect_with(opts)
            .await
            .map_err(|e| PostboxError::Storage(format!("pool: {e}")))?;
        Self::migrate(&pool).await?;
        Ok(Self {
            pool,
            clock,
            ulid_gen: Mutex::new(Generator::new()),
            write_lock: tokio::sync::Mutex::new(()),
        })
    }

    /// The shared pool, useful for tests that want to inspect raw state.
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Run schema migrations. Idempotent.
    async fn migrate(pool: &SqlitePool) -> Result<(), PostboxError> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS mailboxes (
              agent_id           TEXT PRIMARY KEY,
              capacity           INTEGER NOT NULL,
              ordering_mode      TEXT NOT NULL,
              max_attempts       INTEGER NOT NULL,
              lease_duration_ms  INTEGER NOT NULL,
              max_payload_bytes  INTEGER NOT NULL,
              created_at_ms      INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS messages (
              message_id        TEXT PRIMARY KEY,
              mailbox_id        TEXT NOT NULL,
              sender_id         TEXT NOT NULL,
              payload           BLOB NOT NULL,
              headers_json      TEXT NOT NULL,
              priority          INTEGER NOT NULL,
              created_at_ms     INTEGER NOT NULL,
              visible_at_ms     INTEGER NOT NULL,
              status            TEXT NOT NULL CHECK (status IN ('pending','claimed','committed','dead_lettered')),
              attempt_count     INTEGER NOT NULL DEFAULT 0,
              lease_expires_at_ms INTEGER,
              claimed_by        TEXT,
              committed_at_ms   INTEGER,
              checkpoint_token  TEXT,
              FOREIGN KEY (mailbox_id) REFERENCES mailboxes(agent_id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_messages_pending
              ON messages(mailbox_id, status, visible_at_ms)
              WHERE status = 'pending';
            CREATE INDEX IF NOT EXISTS idx_messages_claimed
              ON messages(mailbox_id, status, lease_expires_at_ms)
              WHERE status = 'claimed';
            CREATE INDEX IF NOT EXISTS idx_messages_sender_created
              ON messages(mailbox_id, sender_id, created_at_ms);

            CREATE TABLE IF NOT EXISTS dead_letters (
              message_id         TEXT PRIMARY KEY,
              mailbox_id         TEXT NOT NULL,
              sender_id          TEXT NOT NULL,
              payload            BLOB NOT NULL,
              headers_json       TEXT NOT NULL,
              priority           INTEGER NOT NULL,
              created_at_ms      INTEGER NOT NULL,
              attempt_count      INTEGER NOT NULL,
              failure_history_json TEXT NOT NULL,
              poison_reason      TEXT NOT NULL,
              dead_lettered_at_ms INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_dlq_mailbox
              ON dead_letters(mailbox_id, dead_lettered_at_ms);

            CREATE TABLE IF NOT EXISTS idempotency_ledger (
              mailbox_id   TEXT NOT NULL,
              message_id   TEXT NOT NULL,
              committed_at_ms INTEGER NOT NULL,
              PRIMARY KEY (mailbox_id, message_id)
            );
            "#,
        )
        .execute(pool)
        .await
        .map_err(|e| PostboxError::Storage(format!("migrate: {e}")))?;
        Ok(())
    }

    fn ms(t: SystemTime) -> i64 {
        crate::types::system_time_to_millis(t)
    }
    fn st(ms: i64) -> SystemTime {
        crate::types::millis_to_system_time(ms)
    }

    fn next_ulid(&self) -> Ulid {
        let now = self.clock.now();
        let mut gen = self.ulid_gen.lock();
        gen.generate_from_datetime(now)
            .expect("monotonic ULID generator exhausted random bits")
    }

    fn row_to_message(row: &sqlx::sqlite::SqliteRow) -> Result<Message, PostboxError> {
        let message_id_s: String = row
            .try_get("message_id")
            .map_err(|e| PostboxError::Storage(format!("row: {e}")))?;
        let message_id = Ulid::from_string(&message_id_s)
            .map_err(|e| PostboxError::Storage(format!("ulid: {e}")))?;
        let status_s: String = row.try_get("status").map_err(|e| PostboxError::Storage(e.to_string()))?;
        let status = match status_s.as_str() {
            "pending" => MessageStatus::Pending,
            "claimed" => MessageStatus::Claimed,
            "committed" => MessageStatus::Committed,
            "dead_lettered" => MessageStatus::DeadLettered,
            other => return Err(PostboxError::Storage(format!("bad status {other}"))),
        };
        let headers_json: String = row
            .try_get("headers_json")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        let headers: std::collections::BTreeMap<String, String> =
            serde_json::from_str(&headers_json)
                .map_err(|e| PostboxError::Storage(format!("headers: {e}")))?;
        let payload_v: Vec<u8> = row
            .try_get("payload")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        let visible_at_ms: i64 = row
            .try_get("visible_at_ms")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        let created_at_ms: i64 = row
            .try_get("created_at_ms")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        let attempt_count: i64 = row
            .try_get("attempt_count")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        let lease_expires_at_ms: Option<i64> = row
            .try_get("lease_expires_at_ms")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        let claimed_by: Option<String> = row
            .try_get("claimed_by")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        let committed_at_ms: Option<i64> = row
            .try_get("committed_at_ms")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        let checkpoint_token: Option<String> = row
            .try_get("checkpoint_token")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        Ok(Message {
            message_id,
            mailbox_id: row
                .try_get::<String, _>("mailbox_id")
                .map_err(|e| PostboxError::Storage(e.to_string()))?,
            sender_id: row
                .try_get::<String, _>("sender_id")
                .map_err(|e| PostboxError::Storage(e.to_string()))?,
            payload: Bytes::from(payload_v),
            headers,
            priority: row
                .try_get::<i64, _>("priority")
                .map_err(|e| PostboxError::Storage(e.to_string()))? as i32,
            created_at: Self::st(created_at_ms),
            visible_at: Self::st(visible_at_ms),
            status,
            attempt_count: attempt_count as u32,
            lease_expires_at: lease_expires_at_ms.map(Self::st),
            claimed_by,
            committed_at: committed_at_ms.map(Self::st),
            checkpoint_token,
        })
    }
}

#[async_trait]
impl MailboxStore for SqliteStore {
    async fn ensure_mailbox(&self, config: MailboxConfig) -> Result<Mailbox, PostboxError> {
        let _g = self.write_lock.lock().await;
        validate_agent_id(&config.agent_id)?;
        let now = self.clock.now();
        let now_ms = Self::ms(now);
        let lease_ms = config.lease_duration.as_millis() as i64;
        let ordering = match config.ordering_mode {
            OrderingMode::Fifo => "fifo",
            OrderingMode::Unordered => "unordered",
        };
        sqlx::query(
            r#"
            INSERT INTO mailboxes
              (agent_id, capacity, ordering_mode, max_attempts,
               lease_duration_ms, max_payload_bytes, created_at_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(agent_id) DO NOTHING
            "#,
        )
        .bind(&config.agent_id)
        .bind(config.capacity as i64)
        .bind(ordering)
        .bind(config.max_attempts as i64)
        .bind(lease_ms)
        .bind(config.max_payload_bytes as i64)
        .bind(now_ms)
        .execute(&self.pool)
        .await
        .map_err(|e| PostboxError::Storage(format!("ensure_mailbox: {e}")))?;
        self.get_mailbox(&config.agent_id)
            .await?
            .ok_or_else(|| PostboxError::Storage("missing mailbox after upsert".into()))
    }

    async fn get_mailbox(&self, agent_id: &str) -> Result<Option<Mailbox>, PostboxError> {
        validate_agent_id(agent_id)?;
        let row = sqlx::query("SELECT * FROM mailboxes WHERE agent_id = ?")
            .bind(agent_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| PostboxError::Storage(format!("get_mailbox: {e}")))?;
        let Some(row) = row else { return Ok(None) };
        let ordering_mode = match row
            .try_get::<String, _>("ordering_mode")
            .map_err(|e| PostboxError::Storage(e.to_string()))?
            .as_str()
        {
            "fifo" => OrderingMode::Fifo,
            "unordered" => OrderingMode::Unordered,
            other => {
                return Err(PostboxError::Storage(format!(
                    "unknown ordering_mode {other}"
                )))
            }
        };
        let lease_ms: i64 = row
            .try_get("lease_duration_ms")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        let created_at_ms: i64 = row
            .try_get("created_at_ms")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        Ok(Some(Mailbox {
            agent_id: row
                .try_get::<String, _>("agent_id")
                .map_err(|e| PostboxError::Storage(e.to_string()))?,
            capacity: row
                .try_get::<i64, _>("capacity")
                .map_err(|e| PostboxError::Storage(e.to_string()))? as usize,
            ordering_mode,
            max_attempts: row
                .try_get::<i64, _>("max_attempts")
                .map_err(|e| PostboxError::Storage(e.to_string()))? as u32,
            lease_duration: Duration::from_millis(lease_ms.max(0) as u64),
            max_payload_bytes: row
                .try_get::<i64, _>("max_payload_bytes")
                .map_err(|e| PostboxError::Storage(e.to_string()))? as usize,
            created_at: Self::st(created_at_ms),
        }))
    }

    async fn send(&self, req: SendRequest) -> Result<Message, PostboxError> {
        let _g = self.write_lock.lock().await;
        validate_agent_id(&req.target_mailbox)?;
        validate_agent_id(&req.sender_id)?;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| PostboxError::Storage(format!("begin: {e}")))?;

        // Implicit mailbox creation on first send.
        let row = sqlx::query("SELECT * FROM mailboxes WHERE agent_id = ?")
            .bind(&req.target_mailbox)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| PostboxError::Storage(format!("send lookup: {e}")))?;
        let (capacity, max_payload_bytes, _max_attempts): (i64, i64, i64) = match row {
            Some(r) => (
                r.try_get::<i64, _>("capacity")
                    .map_err(|e| PostboxError::Storage(e.to_string()))?,
                r.try_get::<i64, _>("max_payload_bytes")
                    .map_err(|e| PostboxError::Storage(e.to_string()))?,
                r.try_get::<i64, _>("max_attempts")
                    .map_err(|e| PostboxError::Storage(e.to_string()))?,
            ),
            None => {
                let now_ms = Self::ms(self.clock.now());
                let defaults = MailboxConfig::defaults_for(req.target_mailbox.clone());
                let lease_ms = defaults.lease_duration.as_millis() as i64;
                sqlx::query(
                    r#"
                    INSERT INTO mailboxes
                      (agent_id, capacity, ordering_mode, max_attempts,
                       lease_duration_ms, max_payload_bytes, created_at_ms)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    "#,
                )
                .bind(&defaults.agent_id)
                .bind(defaults.capacity as i64)
                .bind("fifo")
                .bind(defaults.max_attempts as i64)
                .bind(lease_ms)
                .bind(defaults.max_payload_bytes as i64)
                .bind(now_ms)
                .execute(&mut *tx)
                .await
                .map_err(|e| PostboxError::Storage(format!("auto-create: {e}")))?;
                (
                    defaults.capacity as i64,
                    defaults.max_payload_bytes as i64,
                    defaults.max_attempts as i64,
                )
            }
        };

        if req.payload.len() as i64 > max_payload_bytes {
            return Err(PostboxError::PayloadTooLarge {
                size: req.payload.len(),
                max: max_payload_bytes as usize,
            });
        }

        let active_row = sqlx::query(
            "SELECT COUNT(*) AS c FROM messages
             WHERE mailbox_id = ? AND status IN ('pending','claimed')",
        )
        .bind(&req.target_mailbox)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| PostboxError::Storage(format!("capacity count: {e}")))?;
        let active: i64 = active_row
            .try_get("c")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        if active >= capacity {
            return Err(PostboxError::MailboxFull {
                agent_id: req.target_mailbox.clone(),
                size: active as usize,
                capacity: capacity as usize,
            });
        }

        let now = self.clock.now();
        let now_ms = Self::ms(now);
        let visible_at_ms = req
            .delay
            .map(|d| now_ms.saturating_add(d.as_millis() as i64))
            .unwrap_or(now_ms);
        let message_id = self.next_ulid();
        let headers_json = serde_json::to_string(&req.headers)
            .map_err(|e| PostboxError::Storage(format!("headers: {e}")))?;

        sqlx::query(
            r#"
            INSERT INTO messages
              (message_id, mailbox_id, sender_id, payload, headers_json,
               priority, created_at_ms, visible_at_ms, status, attempt_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0)
            "#,
        )
        .bind(message_id.to_string())
        .bind(&req.target_mailbox)
        .bind(&req.sender_id)
        .bind(req.payload.as_ref())
        .bind(headers_json)
        .bind(req.priority as i64)
        .bind(now_ms)
        .bind(visible_at_ms)
        .execute(&mut *tx)
        .await
        .map_err(|e| PostboxError::Storage(format!("insert: {e}")))?;

        tx.commit()
            .await
            .map_err(|e| PostboxError::Storage(format!("commit send: {e}")))?;

        Ok(Message {
            message_id,
            mailbox_id: req.target_mailbox,
            sender_id: req.sender_id,
            payload: req.payload,
            headers: req.headers,
            priority: req.priority,
            created_at: Self::st(now_ms),
            visible_at: Self::st(visible_at_ms),
            status: MessageStatus::Pending,
            attempt_count: 0,
            lease_expires_at: None,
            claimed_by: None,
            committed_at: None,
            checkpoint_token: None,
        })
    }

    async fn peek(
        &self,
        mailbox_id: &str,
        max: usize,
    ) -> Result<Vec<Message>, PostboxError> {
        validate_agent_id(mailbox_id)?;
        let now_ms = Self::ms(self.clock.now());
        let row = sqlx::query("SELECT ordering_mode FROM mailboxes WHERE agent_id = ?")
            .bind(mailbox_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| PostboxError::Storage(format!("peek: {e}")))?;
        let ordering_mode = match row {
            Some(r) => match r
                .try_get::<String, _>("ordering_mode")
                .map_err(|e| PostboxError::Storage(e.to_string()))?
                .as_str()
            {
                "fifo" => OrderingMode::Fifo,
                _ => OrderingMode::Unordered,
            },
            None => {
                return Err(PostboxError::MailboxNotFound {
                    agent_id: mailbox_id.to_string(),
                })
            }
        };

        let rows = sqlx::query(
            "SELECT * FROM messages
             WHERE mailbox_id = ? AND status = 'pending' AND visible_at_ms <= ?
             ORDER BY created_at_ms ASC, message_id ASC",
        )
        .bind(mailbox_id)
        .bind(now_ms)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| PostboxError::Storage(format!("peek rows: {e}")))?;

        let mut messages: Vec<Message> = rows
            .iter()
            .map(Self::row_to_message)
            .collect::<Result<_, _>>()?;
        if matches!(ordering_mode, OrderingMode::Fifo) {
            let mut first_per_sender: std::collections::BTreeMap<String, (SystemTime, Ulid)> =
                std::collections::BTreeMap::new();
            for m in &messages {
                first_per_sender
                    .entry(m.sender_id.clone())
                    .or_insert((m.created_at, m.message_id));
            }
            messages.sort_by(|a, b| {
                let ka = first_per_sender
                    .get(&a.sender_id)
                    .copied()
                    .unwrap_or((a.created_at, a.message_id));
                let kb = first_per_sender
                    .get(&b.sender_id)
                    .copied()
                    .unwrap_or((b.created_at, b.message_id));
                ka.cmp(&kb).then(a.created_at.cmp(&b.created_at)).then(
                    a.message_id.cmp(&b.message_id),
                )
            });
        }
        messages.truncate(max);
        Ok(messages)
    }

    async fn claim(
        &self,
        mailbox_id: &str,
        claimer_id: &str,
        lease_duration: Duration,
    ) -> Result<Option<Claim>, PostboxError> {
        let _g = self.write_lock.lock().await;
        validate_agent_id(mailbox_id)?;
        validate_agent_id(claimer_id)?;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| PostboxError::Storage(format!("begin claim: {e}")))?;

        let mb_row = sqlx::query("SELECT * FROM mailboxes WHERE agent_id = ?")
            .bind(mailbox_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| PostboxError::Storage(format!("claim lookup: {e}")))?;
        let mb = match mb_row {
            Some(r) => r,
            None => {
                return Err(PostboxError::MailboxNotFound {
                    agent_id: mailbox_id.to_string(),
                })
            }
        };
        let max_attempts: i64 = mb
            .try_get("max_attempts")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        let ordering_mode = match mb
            .try_get::<String, _>("ordering_mode")
            .map_err(|e| PostboxError::Storage(e.to_string()))?
            .as_str()
        {
            "fifo" => OrderingMode::Fifo,
            _ => OrderingMode::Unordered,
        };

        let now = self.clock.now();
        let now_ms = Self::ms(now);
        let lease_ms = lease_duration.as_millis() as i64;
        let lease_expires_at_ms = now_ms.saturating_add(lease_ms);

        sqlx::query(
            "UPDATE messages
               SET status = 'pending',
                   lease_expires_at_ms = NULL,
                   claimed_by = NULL
             WHERE mailbox_id = ?
               AND status = 'claimed'
               AND lease_expires_at_ms <= ?",
        )
        .bind(mailbox_id)
        .bind(now_ms)
        .execute(&mut *tx)
        .await
        .map_err(|e| PostboxError::Storage(format!("reclaim: {e}")))?;

        let rows = sqlx::query(
            "SELECT * FROM messages
             WHERE mailbox_id = ? AND status = 'pending' AND visible_at_ms <= ?
             ORDER BY created_at_ms ASC, message_id ASC",
        )
        .bind(mailbox_id)
        .bind(now_ms)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| PostboxError::Storage(format!("candidates: {e}")))?;

        let mut messages: Vec<Message> = rows
            .iter()
            .map(Self::row_to_message)
            .collect::<Result<_, _>>()?;
        if messages.is_empty() {
            tx.commit().await.ok();
            return Ok(None);
        }
        if matches!(ordering_mode, OrderingMode::Fifo) {
            let mut first_per_sender: std::collections::BTreeMap<String, (SystemTime, Ulid)> =
                std::collections::BTreeMap::new();
            for m in &messages {
                first_per_sender
                    .entry(m.sender_id.clone())
                    .or_insert((m.created_at, m.message_id));
            }
            messages.sort_by(|a, b| {
                let ka = first_per_sender
                    .get(&a.sender_id)
                    .copied()
                    .unwrap_or((m_or_default(a), a.message_id));
                let kb = first_per_sender
                    .get(&b.sender_id)
                    .copied()
                    .unwrap_or((m_or_default(b), b.message_id));
                ka.cmp(&kb)
                    .then(a.created_at.cmp(&b.created_at))
                    .then(a.message_id.cmp(&b.message_id))
            });
        }
        let target = messages.into_iter().next().unwrap();
        let mid = target.message_id;

        let updated = sqlx::query(
            "UPDATE messages
                SET status = 'claimed',
                    attempt_count = attempt_count + 1,
                    lease_expires_at_ms = ?,
                    claimed_by = ?
              WHERE message_id = ?
                AND status = 'pending'",
        )
        .bind(lease_expires_at_ms)
        .bind(claimer_id)
        .bind(mid.to_string())
        .execute(&mut *tx)
        .await
        .map_err(|e| PostboxError::Storage(format!("claim update: {e}")))?;
        if updated.rows_affected() == 0 {
            tx.commit().await.ok();
            return Ok(None);
        }

        let new_attempt_count = target.attempt_count + 1;
        if new_attempt_count as i64 > max_attempts {
            let dlq_row = sqlx::query("SELECT * FROM messages WHERE message_id = ?")
                .bind(mid.to_string())
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| PostboxError::Storage(format!("post-claim read: {e}")))?;
            let m = Self::row_to_message(&dlq_row)?;
            let failure_history = vec![FailureRecord {
                attempt: m.attempt_count,
                claimed_by: Some(claimer_id.to_string()),
                failure_kind: FailureKind::Permanent,
                note: Some(format!(
                    "attempt_count {} > max_attempts {}",
                    m.attempt_count, max_attempts
                )),
                at: now,
            }];
            let headers_json = serde_json::to_string(&m.headers)
                .map_err(|e| PostboxError::Storage(e.to_string()))?;
            sqlx::query(
                "INSERT OR REPLACE INTO dead_letters
                  (message_id, mailbox_id, sender_id, payload, headers_json,
                   priority, created_at_ms, attempt_count,
                   failure_history_json, poison_reason, dead_lettered_at_ms)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(m.message_id.to_string())
            .bind(&m.mailbox_id)
            .bind(&m.sender_id)
            .bind(m.payload.as_ref())
            .bind(headers_json)
            .bind(m.priority as i64)
            .bind(Self::ms(m.created_at))
            .bind(m.attempt_count as i64)
            .bind(serde_json::to_string(&failure_history)
                .map_err(|e| PostboxError::Storage(e.to_string()))?)
            .bind("max_attempts_exceeded")
            .bind(now_ms)
            .execute(&mut *tx)
            .await
            .map_err(|e| PostboxError::Storage(format!("dlq insert: {e}")))?;
            sqlx::query("DELETE FROM messages WHERE message_id = ?")
                .bind(mid.to_string())
                .execute(&mut *tx)
                .await
                .map_err(|e| PostboxError::Storage(format!("dlq remove: {e}")))?;
            tx.commit().await.ok();
            return Ok(None);
        }

        tx.commit()
            .await
            .map_err(|e| PostboxError::Storage(format!("commit claim: {e}")))?;

        let mut claimed = target;
        claimed.status = MessageStatus::Claimed;
        claimed.attempt_count = new_attempt_count;
        claimed.lease_expires_at = Some(Self::st(lease_expires_at_ms));
        claimed.claimed_by = Some(claimer_id.to_string());

        Ok(Some(Claim {
            message: claimed,
            lease_expires_at: Self::st(lease_expires_at_ms),
        }))
    }

    async fn commit(
        &self,
        message_id: Ulid,
        claimer_id: &str,
        checkpoint_token: &str,
    ) -> Result<(), PostboxError> {
        let _g = self.write_lock.lock().await;
        validate_agent_id(claimer_id)?;
        if checkpoint_token.is_empty() {
            return Err(PostboxError::EmptyCheckpointToken(message_id));
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| PostboxError::Storage(format!("begin commit: {e}")))?;
        let row = sqlx::query(
            "SELECT status, claimed_by, mailbox_id FROM messages WHERE message_id = ?",
        )
        .bind(message_id.to_string())
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| PostboxError::Storage(format!("commit lookup: {e}")))?;
        let row = row.ok_or(PostboxError::MessageNotFound(message_id))?;
        let status: String = row
            .try_get("status")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        let mailbox_id: String = row
            .try_get("mailbox_id")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        match status.as_str() {
            "claimed" => {}
            "committed" => return Err(PostboxError::AlreadyCommitted(message_id)),
            "dead_lettered" => return Err(PostboxError::MessageNotFound(message_id)),
            _ => return Err(PostboxError::MessageNotClaimed(message_id)),
        }
        let claimed_by: Option<String> = row
            .try_get("claimed_by")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        match claimed_by {
            Some(c) if c == claimer_id => {}
            Some(c) => {
                return Err(PostboxError::NotClaimedByYou {
                    message_id,
                    claimer: c,
                    caller: claimer_id.to_string(),
                });
            }
            None => return Err(PostboxError::MessageNotClaimed(message_id)),
        }
        let now_ms = Self::ms(self.clock.now());
        sqlx::query(
            "UPDATE messages
                SET status = 'committed',
                    committed_at_ms = ?,
                    checkpoint_token = ?,
                    lease_expires_at_ms = NULL,
                    claimed_by = NULL
              WHERE message_id = ? AND status = 'claimed' AND claimed_by = ?",
        )
        .bind(now_ms)
        .bind(checkpoint_token)
        .bind(message_id.to_string())
        .bind(claimer_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| PostboxError::Storage(format!("commit update: {e}")))?;
        sqlx::query(
            "INSERT OR IGNORE INTO idempotency_ledger
              (mailbox_id, message_id, committed_at_ms) VALUES (?, ?, ?)",
        )
        .bind(&mailbox_id)
        .bind(message_id.to_string())
        .bind(now_ms)
        .execute(&mut *tx)
        .await
        .map_err(|e| PostboxError::Storage(format!("ledger insert: {e}")))?;
        tx.commit()
            .await
            .map_err(|e| PostboxError::Storage(format!("commit tx: {e}")))?;
        Ok(())
    }

    async fn release(
        &self,
        message_id: Ulid,
        claimer_id: &str,
        failure: FailureKind,
        note: Option<&str>,
    ) -> Result<(), PostboxError> {
        let _g = self.write_lock.lock().await;
        validate_agent_id(claimer_id)?;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| PostboxError::Storage(format!("begin release: {e}")))?;
        let row = sqlx::query(
            "SELECT status, claimed_by, attempt_count, mailbox_id, sender_id,
                    payload, headers_json, priority, created_at_ms
               FROM messages WHERE message_id = ?",
        )
        .bind(message_id.to_string())
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| PostboxError::Storage(format!("release lookup: {e}")))?;
        let row = row.ok_or(PostboxError::MessageNotFound(message_id))?;
        let status: String = row
            .try_get("status")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        if status != "claimed" {
            return Err(PostboxError::MessageNotClaimed(message_id));
        }
        let claimed_by: Option<String> = row
            .try_get("claimed_by")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        match claimed_by {
            Some(c) if c == claimer_id => {}
            Some(c) => {
                return Err(PostboxError::NotClaimedByYou {
                    message_id,
                    claimer: c,
                    caller: claimer_id.to_string(),
                });
            }
            None => return Err(PostboxError::MessageNotClaimed(message_id)),
        }

        let now = self.clock.now();
        let now_ms = Self::ms(now);
        let attempt_count: i64 = row
            .try_get("attempt_count")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;

        match failure {
            FailureKind::Transient => {
                sqlx::query(
                    "UPDATE messages
                        SET status = 'pending',
                            lease_expires_at_ms = NULL,
                            claimed_by = NULL
                      WHERE message_id = ?",
                )
                .bind(message_id.to_string())
                .execute(&mut *tx)
                .await
                .map_err(|e| PostboxError::Storage(format!("transient release: {e}")))?;
            }
            FailureKind::Permanent => {
                let mailbox_id: String = row
                    .try_get("mailbox_id")
                    .map_err(|e| PostboxError::Storage(e.to_string()))?;
                let sender_id: String = row
                    .try_get("sender_id")
                    .map_err(|e| PostboxError::Storage(e.to_string()))?;
                let payload: Vec<u8> = row
                    .try_get("payload")
                    .map_err(|e| PostboxError::Storage(e.to_string()))?;
                let headers_json: String = row
                    .try_get("headers_json")
                    .map_err(|e| PostboxError::Storage(e.to_string()))?;
                let priority: i64 = row
                    .try_get("priority")
                    .map_err(|e| PostboxError::Storage(e.to_string()))?;
                let created_at_ms: i64 = row
                    .try_get("created_at_ms")
                    .map_err(|e| PostboxError::Storage(e.to_string()))?;

                let failure_history = vec![FailureRecord {
                    attempt: attempt_count as u32,
                    claimed_by: Some(claimer_id.to_string()),
                    failure_kind: FailureKind::Permanent,
                    note: note.map(str::to_string),
                    at: now,
                }];
                sqlx::query(
                    "INSERT OR REPLACE INTO dead_letters
                      (message_id, mailbox_id, sender_id, payload, headers_json,
                       priority, created_at_ms, attempt_count,
                       failure_history_json, poison_reason, dead_lettered_at_ms)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                )
                .bind(message_id.to_string())
                .bind(&mailbox_id)
                .bind(&sender_id)
                .bind(payload)
                .bind(headers_json)
                .bind(priority)
                .bind(created_at_ms)
                .bind(attempt_count)
                .bind(serde_json::to_string(&failure_history)
                    .map_err(|e| PostboxError::Storage(e.to_string()))?)
                .bind("permanent_failure")
                .bind(now_ms)
                .execute(&mut *tx)
                .await
                .map_err(|e| PostboxError::Storage(format!("dlq insert: {e}")))?;
                sqlx::query("DELETE FROM messages WHERE message_id = ?")
                    .bind(message_id.to_string())
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| PostboxError::Storage(format!("dlq remove: {e}")))?;
            }
        }
        tx.commit()
            .await
            .map_err(|e| PostboxError::Storage(format!("release commit: {e}")))?;
        Ok(())
    }

    async fn reject_validation(
        &self,
        message_id: Ulid,
        note: &str,
    ) -> Result<(), PostboxError> {
        let _g = self.write_lock.lock().await;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| PostboxError::Storage(format!("begin reject: {e}")))?;
        let row = sqlx::query("SELECT * FROM messages WHERE message_id = ?")
            .bind(message_id.to_string())
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| PostboxError::Storage(format!("reject lookup: {e}")))?;
        let row = row.ok_or(PostboxError::MessageNotFound(message_id))?;
        let status: String = row
            .try_get("status")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        if status != "pending" {
            return Err(PostboxError::MessageNotClaimable {
                message_id,
                status: match status.as_str() {
                    "claimed" => MessageStatus::Claimed,
                    "committed" => MessageStatus::Committed,
                    "dead_lettered" => MessageStatus::DeadLettered,
                    _ => MessageStatus::Pending,
                },
            });
        }
        let mailbox_id: String = row
            .try_get("mailbox_id")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        let sender_id: String = row
            .try_get("sender_id")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        let payload: Vec<u8> = row
            .try_get("payload")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        let headers_json: String = row
            .try_get("headers_json")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        let priority: i64 = row
            .try_get("priority")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        let created_at_ms: i64 = row
            .try_get("created_at_ms")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        let attempt_count: i64 = row
            .try_get("attempt_count")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;

        let now = self.clock.now();
        let now_ms = Self::ms(now);
        let failure_history = vec![FailureRecord {
            attempt: attempt_count as u32,
            claimed_by: None,
            failure_kind: FailureKind::Permanent,
            note: Some(note.to_string()),
            at: now,
        }];
        sqlx::query(
            "INSERT OR REPLACE INTO dead_letters
              (message_id, mailbox_id, sender_id, payload, headers_json,
               priority, created_at_ms, attempt_count,
               failure_history_json, poison_reason, dead_lettered_at_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(message_id.to_string())
        .bind(&mailbox_id)
        .bind(&sender_id)
        .bind(payload)
        .bind(headers_json)
        .bind(priority)
        .bind(created_at_ms)
        .bind(attempt_count)
        .bind(serde_json::to_string(&failure_history)
            .map_err(|e| PostboxError::Storage(e.to_string()))?)
        .bind("validation_failed")
        .bind(now_ms)
        .execute(&mut *tx)
        .await
        .map_err(|e| PostboxError::Storage(format!("dlq insert reject: {e}")))?;
        sqlx::query("DELETE FROM messages WHERE message_id = ?")
            .bind(message_id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(|e| PostboxError::Storage(format!("dlq remove reject: {e}")))?;
        tx.commit()
            .await
            .map_err(|e| PostboxError::Storage(format!("reject commit: {e}")))?;
        Ok(())
    }

    async fn list_dead_letters(
        &self,
        mailbox_id: &str,
        filter: Option<PoisonReason>,
        limit: usize,
    ) -> Result<Vec<DeadLetter>, PostboxError> {
        validate_agent_id(mailbox_id)?;
        let rows = if let Some(f) = filter {
            let reason = match f {
                PoisonReason::MaxAttemptsExceeded => "max_attempts_exceeded",
                PoisonReason::PermanentFailure => "permanent_failure",
                PoisonReason::ValidationFailed => "validation_failed",
            };
            sqlx::query(
                "SELECT * FROM dead_letters
                  WHERE mailbox_id = ? AND poison_reason = ?
                  ORDER BY dead_lettered_at_ms ASC
                  LIMIT ?",
            )
            .bind(mailbox_id)
            .bind(reason)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| PostboxError::Storage(format!("list dlq filter: {e}")))?
        } else {
            sqlx::query(
                "SELECT * FROM dead_letters
                  WHERE mailbox_id = ?
                  ORDER BY dead_lettered_at_ms ASC
                  LIMIT ?",
            )
            .bind(mailbox_id)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| PostboxError::Storage(format!("list dlq: {e}")))?
        };

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let message_id_s: String = row
                .try_get("message_id")
                .map_err(|e| PostboxError::Storage(e.to_string()))?;
            let message_id = Ulid::from_string(&message_id_s)
                .map_err(|e| PostboxError::Storage(format!("ulid: {e}")))?;
            let payload: Vec<u8> = row
                .try_get("payload")
                .map_err(|e| PostboxError::Storage(e.to_string()))?;
            let headers_json: String = row
                .try_get("headers_json")
                .map_err(|e| PostboxError::Storage(e.to_string()))?;
            let headers: std::collections::BTreeMap<String, String> =
                serde_json::from_str(&headers_json)
                    .map_err(|e| PostboxError::Storage(format!("headers: {e}")))?;
            let failure_history_json: String = row
                .try_get("failure_history_json")
                .map_err(|e| PostboxError::Storage(e.to_string()))?;
            let failure_history: Vec<FailureRecord> = serde_json::from_str(&failure_history_json)
                .map_err(|e| PostboxError::Storage(format!("history: {e}")))?;
            let reason_s: String = row
                .try_get("poison_reason")
                .map_err(|e| PostboxError::Storage(e.to_string()))?;
            let poison_reason = match reason_s.as_str() {
                "max_attempts_exceeded" => PoisonReason::MaxAttemptsExceeded,
                "permanent_failure" => PoisonReason::PermanentFailure,
                "validation_failed" => PoisonReason::ValidationFailed,
                other => return Err(PostboxError::Storage(format!("bad reason {other}"))),
            };
            out.push(DeadLetter {
                message_id,
                mailbox_id: row
                    .try_get::<String, _>("mailbox_id")
                    .map_err(|e| PostboxError::Storage(e.to_string()))?,
                sender_id: row
                    .try_get::<String, _>("sender_id")
                    .map_err(|e| PostboxError::Storage(e.to_string()))?,
                payload: Bytes::from(payload),
                headers,
                priority: row
                    .try_get::<i64, _>("priority")
                    .map_err(|e| PostboxError::Storage(e.to_string()))? as i32,
                created_at: Self::st(
                    row.try_get::<i64, _>("created_at_ms")
                        .map_err(|e| PostboxError::Storage(e.to_string()))?,
                ),
                attempt_count: row
                    .try_get::<i64, _>("attempt_count")
                    .map_err(|e| PostboxError::Storage(e.to_string()))? as u32,
                failure_history,
                poison_reason,
                dead_lettered_at: Self::st(
                    row.try_get::<i64, _>("dead_lettered_at_ms")
                        .map_err(|e| PostboxError::Storage(e.to_string()))?,
                ),
            });
        }
        Ok(out)
    }

    async fn replay_dead_letter(
        &self,
        message_id: Ulid,
        target_mailbox: Option<&str>,
        replayed_by: &str,
    ) -> Result<Message, PostboxError> {
        let _g = self.write_lock.lock().await;
        validate_agent_id(replayed_by)?;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| PostboxError::Storage(format!("begin replay: {e}")))?;
        let row = sqlx::query("SELECT * FROM dead_letters WHERE message_id = ?")
            .bind(message_id.to_string())
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| PostboxError::Storage(format!("replay lookup: {e}")))?;
        let row = row.ok_or(PostboxError::MessageNotFound(message_id))?;

        let mailbox_id: String = row
            .try_get("mailbox_id")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        let target = target_mailbox
            .map(str::to_string)
            .unwrap_or(mailbox_id.clone());
        validate_agent_id(&target)?;

        let mb_row = sqlx::query("SELECT * FROM mailboxes WHERE agent_id = ?")
            .bind(&target)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| PostboxError::Storage(format!("replay mb lookup: {e}")))?;
        let mb_row = mb_row.ok_or_else(|| PostboxError::MailboxNotFound {
            agent_id: target.clone(),
        })?;
        let capacity: i64 = mb_row
            .try_get("capacity")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        let max_payload_bytes: i64 = mb_row
            .try_get("max_payload_bytes")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        let payload: Vec<u8> = row
            .try_get("payload")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        if payload.len() as i64 > max_payload_bytes {
            return Err(PostboxError::PayloadTooLarge {
                size: payload.len(),
                max: max_payload_bytes as usize,
            });
        }
        let active_row = sqlx::query(
            "SELECT COUNT(*) AS c FROM messages
             WHERE mailbox_id = ? AND status IN ('pending','claimed')",
        )
        .bind(&target)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| PostboxError::Storage(format!("replay capacity: {e}")))?;
        let active: i64 = active_row
            .try_get("c")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        if active >= capacity {
            return Err(PostboxError::MailboxFull {
                agent_id: target.clone(),
                size: active as usize,
                capacity: capacity as usize,
            });
        }

        let sender_id: String = row
            .try_get("sender_id")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        let priority: i64 = row
            .try_get("priority")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        let headers_json: String = row
            .try_get("headers_json")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        let mut headers: std::collections::BTreeMap<String, String> =
            serde_json::from_str(&headers_json)
                .map_err(|e| PostboxError::Storage(format!("headers: {e}")))?;
        headers.insert("replayed_from".to_string(), message_id.to_string());
        headers.insert("replayed_by".to_string(), replayed_by.to_string());

        let now = self.clock.now();
        let now_ms = Self::ms(now);
        headers.insert("replayed_at".to_string(), now_ms.to_string());
        let new_headers_json = serde_json::to_string(&headers)
            .map_err(|e| PostboxError::Storage(format!("headers: {e}")))?;

        let new_id = self.next_ulid();
        sqlx::query(
            r#"
            INSERT INTO messages
              (message_id, mailbox_id, sender_id, payload, headers_json,
               priority, created_at_ms, visible_at_ms, status, attempt_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0)
            "#,
        )
        .bind(new_id.to_string())
        .bind(&target)
        .bind(&sender_id)
        .bind(&payload)
        .bind(new_headers_json)
        .bind(priority)
        .bind(now_ms)
        .bind(now_ms)
        .execute(&mut *tx)
        .await
        .map_err(|e| PostboxError::Storage(format!("replay insert: {e}")))?;
        tx.commit()
            .await
            .map_err(|e| PostboxError::Storage(format!("replay commit: {e}")))?;

        Ok(Message {
            message_id: new_id,
            mailbox_id: target,
            sender_id,
            payload: Bytes::from(payload),
            headers,
            priority: priority as i32,
            created_at: Self::st(now_ms),
            visible_at: Self::st(now_ms),
            status: MessageStatus::Pending,
            attempt_count: 0,
            lease_expires_at: None,
            claimed_by: None,
            committed_at: None,
            checkpoint_token: None,
        })
    }

    async fn is_committed(
        &self,
        mailbox_id: &str,
        message_id: Ulid,
    ) -> Result<bool, PostboxError> {
        validate_agent_id(mailbox_id)?;
        let row = sqlx::query(
            "SELECT 1 AS x FROM idempotency_ledger
              WHERE mailbox_id = ? AND message_id = ?",
        )
        .bind(mailbox_id)
        .bind(message_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| PostboxError::Storage(format!("is_committed: {e}")))?;
        Ok(row.is_some())
    }

    async fn sweep_expired_leases(
        &self,
        now: SystemTime,
    ) -> Result<usize, PostboxError> {
        let _g = self.write_lock.lock().await;
        let now_ms = Self::ms(now);
        let res = sqlx::query(
            "UPDATE messages
                SET status = 'pending',
                    lease_expires_at_ms = NULL,
                    claimed_by = NULL
              WHERE status = 'claimed'
                AND lease_expires_at_ms <= ?",
        )
        .bind(now_ms)
        .execute(&self.pool)
        .await
        .map_err(|e| PostboxError::Storage(format!("sweep: {e}")))?;
        Ok(res.rows_affected() as usize)
    }

    async fn pending_count(&self, mailbox_id: &str) -> Result<usize, PostboxError> {
        validate_agent_id(mailbox_id)?;
        let row = sqlx::query(
            "SELECT COUNT(*) AS c FROM messages
             WHERE mailbox_id = ? AND status IN ('pending','claimed')",
        )
        .bind(mailbox_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| PostboxError::Storage(format!("pending_count: {e}")))?;
        let c: i64 = row
            .try_get("c")
            .map_err(|e| PostboxError::Storage(e.to_string()))?;
        Ok(c as usize)
    }
}

#[inline]
fn m_or_default(m: &Message) -> SystemTime {
    m.created_at
}