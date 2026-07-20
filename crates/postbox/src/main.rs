//! `postbox` server binary. Wires core + grpc + mcp from a single config.
//!
//! Usage:
//!
//! ```text
//! postbox --db sqlite://./postbox.db
//! postbox --db sqlite::memory: --http :8080 --grpc :50051
//! ```
//!
//! Config is read from CLI flags first, then environment variables (prefix
//! `POSTBOX_`), then defaults.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use postbox_core::{
    sqlite::SqliteStoreConfig, MailboxStore, SqliteStore, SystemClock,
};
use postbox_grpc::http::{router as http_router, AppState};
use postbox_mcp::PostboxMcp;
use rmcp::transport::io::stdio;
use tokio::signal::unix::{signal, SignalKind};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "postbox", about = "Exactly-once agent mailbox broker")]
struct Cli {
    /// SQLite database URL. `sqlite::memory:` for a one-shot server.
    #[arg(long, env = "POSTBOX_DB", default_value = "sqlite::memory:")]
    db: String,

    /// HTTP listen address.
    #[arg(long, env = "POSTBOX_HTTP", default_value = "127.0.0.1:8080")]
    http: String,

    /// gRPC listen address. Set to `off` to disable.
    #[arg(long, env = "POSTBOX_GRPC", default_value = "127.0.0.1:50051")]
    grpc: String,

    /// Sweep interval for lease recovery. `off` disables the sweeper.
    #[arg(long, env = "POSTBOX_SWEEP_INTERVAL", default_value = "5s")]
    sweep_interval: String,

    /// Run an in-process MCP stdio server. `off` disables.
    #[arg(long, env = "POSTBOX_MCP", default_value = "off")]
    mcp: String,

    /// Verbosity (`-v`, `-vv`, …).
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
}

#[derive(Debug)]
struct Config {
    db_url: String,
    http: Option<String>,
    grpc: Option<String>,
    sweep_interval: Option<Duration>,
    run_mcp_stdio: bool,
}

impl Config {
    fn from_cli(cli: Cli) -> Result<Self> {
        let sweep = match cli.sweep_interval.as_str() {
            "off" => None,
            other => Some(humantime::parse_duration(other).with_context(|| {
                format!("invalid --sweep-interval `{other}` (examples: `1s`, `500ms`, `off`)")
            })?),
        };
        let grpc = match cli.grpc.as_str() {
            "off" => None,
            other => Some(other.to_string()),
        };
        let http = match cli.http.as_str() {
            "off" => None,
            other => Some(other.to_string()),
        };
        let run_mcp = cli.mcp == "stdio";
        Ok(Self {
            db_url: cli.db,
            http,
            grpc,
            sweep_interval: sweep,
            run_mcp_stdio: run_mcp,
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.verbose);

    let config = Config::from_cli(cli)?;
    info!(?config, "postbox starting");

    // Validate config up front so we fail fast with a clear message.
    if config.http.is_none() && config.grpc.is_none() && !config.run_mcp_stdio {
        anyhow::bail!(
            "all listeners disabled; enable at least one of --http, --grpc, or --mcp=stdio"
        );
    }
    if !config.db_url.starts_with("sqlite:") {
        anyhow::bail!("only sqlite URLs are supported; got: {}", config.db_url);
    }

    let clock: Arc<dyn postbox_core::Clock> = Arc::new(SystemClock);

    let sqlite_cfg = SqliteStoreConfig {
        url: config.db_url.clone(),
        max_connections: 8,
    };

    let sqlite_store = Arc::new(
        SqliteStore::connect(sqlite_cfg, clock.clone())
            .await
            .context("opening SQLite store")?,
    );
    let store: Arc<dyn MailboxStore> = sqlite_store.clone();

    // Shutdown broadcast: sending `true` tells HTTP and gRPC to drain and exit.
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Sweeper.
    let sweeper_handle = if let Some(interval) = config.sweep_interval {
        let store_for_sweeper: Arc<dyn MailboxStore> = store.clone();
        let h = postbox_core::sweeper::spawn_arc(store_for_sweeper, clock.clone(), interval);
        Some(h)
    } else {
        None
    };

    // HTTP.
    let http_handle = if let Some(addr) = config.http.clone() {
        let app = http_router(AppState::new(store.clone()));
        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .with_context(|| format!("failed to bind HTTP address {addr}"))?;
        info!(addr = %addr, "HTTP listening");
        let mut rx = shutdown_rx.clone();
        let handle = tokio::spawn(async move {
            let shutdown_fut = async move { rx.changed().await.ok(); };
            if let Err(e) = axum::serve(listener, app).with_graceful_shutdown(shutdown_fut).await {
                tracing::error!(error = %e, "HTTP server exited with error");
            }
        });
        Some(handle)
    } else {
        info!("HTTP listener disabled");
        None
    };

    // gRPC.
    let grpc_handle = if let Some(addr) = config.grpc.clone() {
        info!(addr = %addr, "gRPC listening");
        let store_for_grpc = store.clone();
        let mut rx = shutdown_rx.clone();
        let handle = tokio::spawn(async move {
            let shutdown_fut = async move { rx.changed().await.ok(); };
            if let Err(e) = postbox_grpc::grpc::serve_with_shutdown(
                store_for_grpc,
                postbox_grpc::grpc::GrpcServeConfig::from_addr(addr),
                shutdown_fut,
            )
            .await
            {
                tracing::error!(error = %e, "gRPC server exited with error");
            }
        });
        Some(handle)
    } else {
        info!("gRPC listener disabled");
        None
    };

    // MCP stdio.
    let mcp_handle = if config.run_mcp_stdio {
        info!("MCP stdio enabled");
        let server = PostboxMcp::new(store.clone());
        let handle = tokio::spawn(async move {
            use rmcp::ServiceExt;
            let transport = stdio();
            if let Err(e) = server.serve(transport).await {
                tracing::error!(error = %e, "MCP server exited with error");
            }
        });
        Some(handle)
    } else {
        None
    };

    // Wait for signal.
    wait_for_shutdown().await;
    info!("shutdown signal received; draining servers");

    // Signal HTTP and gRPC to stop accepting new connections and drain.
    let _ = shutdown_tx.send(true);

    // Give servers up to 10 s to drain in-flight requests before we give up.
    let drain_timeout = Duration::from_secs(10);
    if let Some(h) = http_handle {
        if tokio::time::timeout(drain_timeout, h).await.is_err() {
            warn!("HTTP server did not drain within timeout; forcing stop");
        }
    }
    if let Some(h) = grpc_handle {
        if tokio::time::timeout(drain_timeout, h).await.is_err() {
            warn!("gRPC server did not drain within timeout; forcing stop");
        }
    }
    if let Some(h) = mcp_handle {
        h.abort();
        let _ = h.await;
    }
    if let Some(h) = sweeper_handle {
        h.stop().await;
    }
    info!("postbox stopped cleanly");
    Ok(())
}

fn init_tracing(verbose: u8) {
    let default = match verbose {
        0 => "info",
        1 => "debug",
        _ => "trace",
    };
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .try_init();
}

async fn wait_for_shutdown() {
    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e, "failed to install SIGTERM handler; using fallback");
            tokio::time::sleep(Duration::from_secs(u64::MAX / 2)).await;
            return;
        }
    };
    let mut sigint = match signal(SignalKind::interrupt()) {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e, "failed to install SIGINT handler; using fallback");
            tokio::time::sleep(Duration::from_secs(u64::MAX / 2)).await;
            return;
        }
    };
    tokio::select! {
        _ = sigterm.recv() => info!("SIGTERM"),
        _ = sigint.recv() => info!("SIGINT"),
    }
}

/// Default path used by the README example.
#[allow(dead_code)]
fn _default_path() -> PathBuf {
    PathBuf::from("./postbox.db")
}