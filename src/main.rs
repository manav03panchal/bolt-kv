use anyhow::Result;
use bolt_kv::run_server;
use clap::Parser;
use std::path::PathBuf;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tracing::{info, warn};

/// A high-performance, persistent, in-memory key-value store.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    /// The address to bind the server to.
    #[arg(long, default_value = "127.0.0.1:7878")]
    addr: String,

    /// The path to the Append-Only File for persistence.
    /// If not provided, persistence is disabled.
    #[arg(long)]
    aof_path: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize our structured logger.
    tracing_subscriber::fmt::init();

    // Parse the command-line arguments.
    let cli = Cli::parse();

    // Use the parsed arguments.
    let addr = &cli.addr;
    let aof_path = cli.aof_path;

    info!("Starting BoltKV server...");

    let (shutdown_tx, _) = broadcast::channel(1);

    let ctrl_c_tx = shutdown_tx.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        tracing::info!("\nCtrl+C detected!");
        ctrl_c_tx.send(()).unwrap();
    });

    let listener = TcpListener::bind(addr).await?;
    tracing::info!("Listening on: {}", addr);
    if let Some(path) = &aof_path {
        info!("Persistence enabled. AOF path: {}", path.display());
    } else {
        warn!("Persistence is disabled. Data will be lost on shutdown.");
    }

    run_server(listener, shutdown_tx, aof_path).await?;

    Ok(())
}
