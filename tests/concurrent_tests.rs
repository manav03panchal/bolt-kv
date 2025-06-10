use bolt_kv::run_server;
use std::time::Duration;
use tempfile::NamedTempFile;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

async fn connect_client(addr: std::net::SocketAddr) -> BufReader<TcpStream> {
    let client_stream = TcpStream::connect(addr).await.unwrap();
    BufReader::new(client_stream)
}

async fn write_and_read_response(
    client: &mut BufReader<TcpStream>,
    command: &str,
    expected_response: Option<&str>,
) -> String {
    client
        .get_mut()
        .write_all(command.as_bytes())
        .await
        .unwrap();

    let mut response = String::new();
    client.read_line(&mut response).await.unwrap();

    if let Some(expected) = expected_response {
        assert_eq!(response, expected);
    }

    response
}

async fn start_test_server(
    aof_path: Option<std::path::PathBuf>,
) -> (std::net::SocketAddr, JoinHandle<()>, broadcast::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    let server_shutdown_tx = shutdown_tx.clone();
    let server_handle = tokio::spawn(async move {
        run_server(listener, server_shutdown_tx, aof_path, None)
            .await
            .unwrap();
    });

    // Small delay to ensure server is ready
    tokio::time::sleep(Duration::from_millis(5)).await;

    (addr, server_handle, shutdown_tx)
}

#[tokio::test]
async fn test_multiple_concurrent_clients() {
    // Start server
    let (addr, server_handle, shutdown_tx) = start_test_server(None).await;

    // Connect multiple clients
    let mut client1 = connect_client(addr).await;
    let mut client2 = connect_client(addr).await;
    let mut client3 = connect_client(addr).await;

    // Client 1 sets a key
    write_and_read_response(&mut client1, "SET key1 value1\n", Some("OK\n")).await;

    // Client 2 sets a different key
    write_and_read_response(&mut client2, "SET key2 value2\n", Some("OK\n")).await;

    // Client 3 gets both keys
    let resp1 = write_and_read_response(&mut client3, "GET key1\n", Some("value1\n")).await;
    let resp2 = write_and_read_response(&mut client3, "GET key2\n", Some("value2\n")).await;

    assert_eq!(resp1, "value1\n");
    assert_eq!(resp2, "value2\n");

    // Client 1 deletes key2
    write_and_read_response(&mut client1, "DEL key2\n", Some("OK\n")).await;

    // Client 2 verifies key2 is gone
    write_and_read_response(&mut client2, "GET key2\n", Some("NULL\n")).await;

    // Shutdown
    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();
}

#[tokio::test]
async fn test_concurrent_set_same_key() {
    // Start server
    let (addr, server_handle, shutdown_tx) = start_test_server(None).await;

    // Connect multiple clients
    let mut client1 = connect_client(addr).await;
    let mut client2 = connect_client(addr).await;

    // Both clients set the same key concurrently
    let task1 = tokio::spawn(async move {
        write_and_read_response(&mut client1, "SET shared_key client1_value\n", Some("OK\n")).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        write_and_read_response(&mut client1, "GET shared_key\n", None).await
    });

    let task2 = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(5)).await;
        write_and_read_response(&mut client2, "SET shared_key client2_value\n", Some("OK\n")).await;
        write_and_read_response(&mut client2, "GET shared_key\n", None).await
    });

    // Check results
    let result1 = task1.await.unwrap();
    let result2 = task2.await.unwrap();

    // Both should see the latest value
    assert_eq!(result1.trim(), "client2_value");
    assert_eq!(result2.trim(), "client2_value");

    // Shutdown
    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();
}

#[tokio::test]
async fn test_concurrent_persistence() {
    let temp_file = NamedTempFile::new().unwrap();
    let aof_path = temp_file.path().to_path_buf();

    // Start server with persistence
    let (addr, server_handle, shutdown_tx) = start_test_server(Some(aof_path.clone())).await;

    // Create multiple clients and perform concurrent operations
    let mut clients = Vec::new();
    for _ in 0..5 {
        clients.push(connect_client(addr).await);
    }

    // Each client will set different keys concurrently
    let mut tasks = Vec::new();
    for (i, mut client) in clients.into_iter().enumerate() {
        let task = tokio::spawn(async move {
            let key = format!("concurrent_key_{}", i);
            let value = format!("concurrent_value_{}", i);
            let cmd = format!("SET {} {}\n", key, value);
            write_and_read_response(&mut client, &cmd, Some("OK\n")).await;
        });
        tasks.push(task);
    }

    // Wait for all tasks to complete
    for task in tasks {
        task.await.unwrap();
    }

    // Shutdown the server
    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();

    // Start a new server with the same AOF file
    let (new_addr, new_server_handle, new_shutdown_tx) =
        start_test_server(Some(aof_path.clone())).await;

    // Connect to the new server and verify all data is there
    let mut new_client = connect_client(new_addr).await;

    // Check that all 5 keys were persisted
    for i in 0..5 {
        let key = format!("concurrent_key_{}", i);
        let expected_value = format!("concurrent_value_{}\n", i);
        let cmd = format!("GET {}\n", key);
        write_and_read_response(&mut new_client, &cmd, Some(&expected_value)).await;
    }

    // Shutdown the new server
    new_shutdown_tx.send(()).unwrap();
    new_server_handle.await.unwrap();
}

#[tokio::test]
async fn test_concurrent_delete() {
    // Start server
    let (addr, server_handle, shutdown_tx) = start_test_server(None).await;

    // Connect two clients
    let mut client1 = connect_client(addr).await;
    let mut client2 = connect_client(addr).await;

    // Set up initial data
    write_and_read_response(
        &mut client1,
        "SET conflict_key initial_value\n",
        Some("OK\n"),
    )
    .await;

    // Both clients try to DELETE the same key concurrently
    let task1 = tokio::spawn(async move {
        write_and_read_response(&mut client1, "DEL conflict_key\n", Some("OK\n")).await;
        client1
    });

    let task2 = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1)).await; // Slight delay
        write_and_read_response(&mut client2, "DEL conflict_key\n", Some("OK\n")).await;
        client2
    });

    // Both deletions should succeed
    let mut client1 = task1.await.unwrap();
    let mut client2 = task2.await.unwrap();

    // Both clients should see NULL when getting the deleted key
    write_and_read_response(&mut client1, "GET conflict_key\n", Some("NULL\n")).await;
    write_and_read_response(&mut client2, "GET conflict_key\n", Some("NULL\n")).await;

    // Shutdown
    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();
}
