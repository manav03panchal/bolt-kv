use anyhow::Result;
use bolt_kv::run_server;
use std::path::PathBuf; // We need to import this to create a file path
use tokio::net::TcpListener;
use tokio::sync::broadcast;

#[tokio::main]
async fn main() -> Result<()> {
    let (shutdown_tx, _) = broadcast::channel(1);

    let ctrl_c_tx = shutdown_tx.clone();
    tokio::spawn(async move {
        // Wait for the Ctrl+C signal from the OS.
        tokio::signal::ctrl_c().await.unwrap();
        println!("\nCtrl+C detected!");
        ctrl_c_tx.send(()).unwrap();
    });

    // Bind the listener
    let listener = TcpListener::bind("127.0.0.1:7878").await?;

    let aof_path = Some(PathBuf::from("bolt-kv.aof"));

    // Start the main server loop, now with all three arguments.
    run_server(listener, shutdown_tx, aof_path).await?;

    Ok(())
}
