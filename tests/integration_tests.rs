use bolt_kv::{Db, process_connection, run_server};
use dashmap::DashMap;
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;

#[tokio::test]
async fn test_persistence() {
    let temp_file = NamedTempFile::new().unwrap();
    let aof_path = temp_file.path().to_path_buf();

    // --- PHASE 1: Run the server, write some data, and shut it down. ---
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, _) = broadcast::channel(1);

    let server_task = tokio::spawn(run_server(
        listener,
        shutdown_tx.clone(),
        Some(aof_path.clone()),
        None,
    ));

    // Only a minimal sleep to ensure server is ready
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    {
        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(b"SET name persistence\n").await.unwrap();
        let mut response = [0u8; 3]; // Just "OK\n"
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"OK\n");
    } // client is dropped here, connection closes.

    // Cleanly shut down the first server.
    shutdown_tx.send(()).unwrap();
    server_task.await.unwrap().unwrap();

    // --- PHASE 2: Start a NEW server instance using the SAME AOF file. ---
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, _) = broadcast::channel(1);

    let server_task_2 = tokio::spawn(run_server(
        listener,
        shutdown_tx.clone(),
        Some(aof_path),
        None,
    ));
    // Only a minimal sleep to ensure server is ready
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    {
        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(b"GET name\n").await.unwrap();

        let mut response_buf = [0u8; 12]; // "persistence\n" is 12 bytes
        let n = client.read_exact(&mut response_buf).await.unwrap();
        let response = std::str::from_utf8(&response_buf[..n]).unwrap();

        assert_eq!(response, "persistence\n");
    } // client is dropped here.

    // Cleanly shut down the server from Phase 2.
    shutdown_tx.send(()).unwrap();
    server_task_2.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_set_and_get() {
    let (mut client, _db) = setup_test_server().await;
    client
        .get_mut()
        .write_all(b"SET name rusty\n")
        .await
        .unwrap();
    let mut response = String::new();
    client.read_line(&mut response).await.unwrap();
    assert_eq!(response, "OK\n");
    client.get_mut().write_all(b"GET name\n").await.unwrap();
    response.clear();
    client.read_line(&mut response).await.unwrap();
    assert_eq!(response, "rusty\n");
}

#[tokio::test]
async fn test_get_non_existent() {
    let (mut client, _db) = setup_test_server().await;
    client
        .get_mut()
        .write_all(b"GET non_existent_key\n")
        .await
        .unwrap();
    let mut response = String::new();
    client.read_line(&mut response).await.unwrap();
    assert_eq!(response, "NULL\n");
}

#[tokio::test]
async fn test_del_command() {
    let (mut client, db) = setup_test_server().await;
    db.insert("to_delete".to_string(), "some_value".to_string());
    client
        .get_mut()
        .write_all(b"DEL to_delete\n")
        .await
        .unwrap();
    let mut response = String::new();
    client.read_line(&mut response).await.unwrap();
    assert_eq!(response, "OK\n");
    assert!(!db.contains_key("to_delete"));
}

#[tokio::test]
async fn test_unknown_command() {
    let (mut client, _db) = setup_test_server().await;
    client.get_mut().write_all(b"JUMP a_key\n").await.unwrap();
    let mut response = String::new();
    client.read_line(&mut response).await.unwrap();
    assert_eq!(response, "ERR unknown command\n");
}

#[tokio::test]
async fn test_graceful_shutdown() {
    let (shutdown_tx, _) = broadcast::channel(1);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_task = tokio::spawn(run_server(
        listener,
        shutdown_tx.clone(),
        None, // No AOF for this test
        None,
    ));
    // Reduced sleep time from 100ms to 5ms for faster test
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    shutdown_tx.send(()).unwrap();

    // Add a brief delay to let the shutdown propagate
    tokio::time::sleep(std::time::Duration::from_millis(1)).await;

    let client_result = TcpStream::connect(addr).await;
    assert!(
        client_result.is_err(),
        "Server should stop accepting new connections after shutdown signal"
    );
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_malformed_commands() {
    let (mut client, _db) = setup_test_server().await;

    // A map of bad commands to the error we expect.
    let test_cases = vec![
        ("SET key\n", "ERR wrong number of arguments for SET\n"),
        ("GET\n", "ERR wrong number of arguments for GET\n"),
        ("DEL\n", "ERR wrong number of arguments for DEL\n"),
    ];

    for (command, expected_response) in test_cases {
        client
            .get_mut()
            .write_all(command.as_bytes())
            .await
            .unwrap();
        let mut response = String::new();
        client.read_line(&mut response).await.unwrap();
        assert_eq!(response, expected_response);
    }
}

#[tokio::test]
async fn test_value_with_spaces() {
    let (mut client, _db) = setup_test_server().await;

    let key = "message";
    let value = "hello beautiful world";
    let set_command = format!("SET {} {}\n", key, value);

    client
        .get_mut()
        .write_all(set_command.as_bytes())
        .await
        .unwrap();
    let mut response = String::new();
    client.read_line(&mut response).await.unwrap();
    assert_eq!(response, "OK\n");

    let get_command = format!("GET {}\n", key);
    client
        .get_mut()
        .write_all(get_command.as_bytes())
        .await
        .unwrap();
    response.clear();
    client.read_line(&mut response).await.unwrap();
    assert_eq!(response.trim(), value);
}

#[tokio::test]
async fn test_persistence_with_delete() {
    let temp_file = NamedTempFile::new().unwrap();
    let aof_path = temp_file.path().to_path_buf();

    // Manually write a log file with a SET followed by a DEL
    {
        let mut file = File::create(&aof_path).await.unwrap();
        file.write_all(b"SET name rusty\n").await.unwrap();
        file.write_all(b"SET status cool\n").await.unwrap();
        file.write_all(b"DEL name\n").await.unwrap();
        file.flush().await.unwrap();
    } // File is closed here as `file` goes out of scope

    // Start the server. It should load this AOF.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, _) = broadcast::channel(1);

    let server_task = tokio::spawn(run_server(
        listener,
        shutdown_tx.clone(),
        Some(aof_path),
        None,
    ));
    // Reduced sleep time for faster tests
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    // Connect a client and check the state
    let mut client = TcpStream::connect(addr).await.unwrap();

    // 1. The DELETED key should not exist.
    client.write_all(b"GET name\n").await.unwrap();
    let mut response_buf = [0u8; 5]; // "NULL\n" is 5 bytes
    client.read_exact(&mut response_buf).await.unwrap();
    let response = std::str::from_utf8(&response_buf).unwrap();
    assert_eq!(response, "NULL\n");

    // 2. The OTHER key should still exist.
    client.write_all(b"GET status\n").await.unwrap();
    let mut response_buf = [0u8; 5]; // "cool\n" is 5 bytes
    client.read_exact(&mut response_buf).await.unwrap();
    let response = std::str::from_utf8(&response_buf).unwrap();
    assert_eq!(response, "cool\n");

    // Properly shut down the server
    shutdown_tx.send(()).unwrap();
    server_task.await.unwrap().unwrap();
}

async fn setup_test_server() -> (BufReader<TcpStream>, Db) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let db = Arc::new(DashMap::new());
    let (shutdown_tx, _) = broadcast::channel(1);

    let db_clone = db.clone();
    let aof_mutex = Arc::new(tokio::sync::Mutex::new(None));

    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let shutdown_rx = shutdown_tx.subscribe();
        process_connection(socket, db_clone, shutdown_rx, aof_mutex, Arc::new(None)).await;
    });

    // Small delay to avoid race condition, but keep it minimal
    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    let client_stream = TcpStream::connect(addr).await.unwrap();
    let client = BufReader::new(client_stream);

    (client, db)
}
