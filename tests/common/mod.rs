use bolt_kv::run_server;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

// Updated to accept password parameter
pub async fn start_test_server(
    aof_path: Option<PathBuf>,
    password: Option<String>, // New argument
) -> (std::net::SocketAddr, JoinHandle<()>, broadcast::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    let server_shutdown_tx = shutdown_tx.clone();
    // Clone the password to move it into the task
    let password_clone = password.clone();
    let server_handle = tokio::spawn(async move {
        run_server(listener, server_shutdown_tx, aof_path, password_clone)
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(10)).await;

    (addr, server_handle, shutdown_tx)
}

pub async fn connect_client(addr: std::net::SocketAddr) -> TcpStream {
    TcpStream::connect(addr).await.unwrap()
}

pub async fn send_command(client: &mut TcpStream, command: &str) -> String {
    client.write_all(command.as_bytes()).await.unwrap();

    // Read the response
    let mut buffer = [0u8; 1024];
    let n = client.read(&mut buffer).await.unwrap();
    String::from_utf8_lossy(&buffer[..n]).to_string()
}
