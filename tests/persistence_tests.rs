use bolt_kv::run_server;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::NamedTempFile;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

async fn start_test_server(
    aof_path: Option<PathBuf>,
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

async fn connect_client(addr: std::net::SocketAddr) -> BufReader<TcpStream> {
    let client_stream = TcpStream::connect(addr).await.unwrap();
    BufReader::new(client_stream)
}

async fn send_command(client: &mut BufReader<TcpStream>, command: &str) -> String {
    client
        .get_mut()
        .write_all(command.as_bytes())
        .await
        .unwrap();

    let mut response = String::new();
    client.read_line(&mut response).await.unwrap();

    response
}

#[tokio::test]
async fn test_persistence_with_overwritten_values() {
    let temp_file = NamedTempFile::new().unwrap();
    let aof_path = temp_file.path().to_path_buf();

    // Start server with persistence
    let (addr, server_handle, shutdown_tx) = start_test_server(Some(aof_path.clone())).await;

    // Connect client
    let mut client = connect_client(addr).await;

    // Set a key multiple times, overwriting the value
    send_command(&mut client, "SET persistent_key value1\n").await;
    send_command(&mut client, "SET persistent_key value2\n").await;
    send_command(&mut client, "SET persistent_key final_value\n").await;

    // Set another key
    send_command(&mut client, "SET another_key another_value\n").await;

    // Shutdown
    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();

    // Start a new server with the same AOF file
    let (new_addr, new_server_handle, new_shutdown_tx) =
        start_test_server(Some(aof_path.clone())).await;

    // Connect to the new server and verify the final value is preserved
    let mut new_client = connect_client(new_addr).await;

    let response = send_command(&mut new_client, "GET persistent_key\n").await;
    assert_eq!(response, "final_value\n");

    let response = send_command(&mut new_client, "GET another_key\n").await;
    assert_eq!(response, "another_value\n");

    // Shutdown
    new_shutdown_tx.send(()).unwrap();
    new_server_handle.await.unwrap();
}

#[tokio::test]
async fn test_persistence_with_deleted_keys() {
    let temp_file = NamedTempFile::new().unwrap();
    let aof_path = temp_file.path().to_path_buf();

    // Start server with persistence
    let (addr, server_handle, shutdown_tx) = start_test_server(Some(aof_path.clone())).await;

    // Connect client
    let mut client = connect_client(addr).await;

    // Set multiple keys
    send_command(&mut client, "SET key1 value1\n").await;
    send_command(&mut client, "SET key2 value2\n").await;
    send_command(&mut client, "SET key3 value3\n").await;

    // Delete one key
    send_command(&mut client, "DEL key2\n").await;

    // Verify the key was deleted
    let response = send_command(&mut client, "GET key2\n").await;
    assert_eq!(response, "NULL\n");

    // Shutdown
    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();

    // Start a new server with the same AOF file
    let (new_addr, new_server_handle, new_shutdown_tx) = start_test_server(Some(aof_path)).await;

    // Connect to the new server and verify the state is correct
    let mut new_client = connect_client(new_addr).await;

    // key1 and key3 should exist
    let response = send_command(&mut new_client, "GET key1\n").await;
    assert_eq!(response, "value1\n");

    let response = send_command(&mut new_client, "GET key3\n").await;
    assert_eq!(response, "value3\n");

    // key2 should still be deleted
    let response = send_command(&mut new_client, "GET key2\n").await;
    assert_eq!(response, "NULL\n");

    // Shutdown
    new_shutdown_tx.send(()).unwrap();
    new_server_handle.await.unwrap();
}

#[tokio::test]
async fn test_corrupted_aof_recovery() {
    let temp_file = NamedTempFile::new().unwrap();
    let aof_path = temp_file.path().to_path_buf();

    // Manually create a corrupted AOF file
    {
        let mut file = std::fs::File::create(&aof_path).unwrap();
        use std::io::Write;

        // Valid commands
        file.write_all(b"SET valid_key1 valid_value1\n").unwrap();
        file.write_all(b"SET valid_key2 valid_value2\n").unwrap();

        // Corrupted/partial command (missing value)
        file.write_all(b"SET corrupted_key\n").unwrap();

        // More valid commands
        file.write_all(b"SET valid_key3 valid_value3\n").unwrap();

        // Another corrupted command
        file.write_all(b"INVALID_COMMAND\n").unwrap();

        file.flush().unwrap();
    }

    // Start server with the corrupted AOF file
    let (addr, server_handle, shutdown_tx) = start_test_server(Some(aof_path)).await;

    // Connect client
    let mut client = connect_client(addr).await;

    // Check if valid keys were loaded
    let response = send_command(&mut client, "GET valid_key1\n").await;
    assert_eq!(response, "valid_value1\n");

    let response = send_command(&mut client, "GET valid_key2\n").await;
    assert_eq!(response, "valid_value2\n");

    let response = send_command(&mut client, "GET valid_key3\n").await;
    assert_eq!(response, "valid_value3\n");

    // The corrupted key should not exist
    let response = send_command(&mut client, "GET corrupted_key\n").await;
    assert_eq!(response, "NULL\n");

    // Shutdown
    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();
}

#[tokio::test]
async fn test_persistence_with_empty_aof() {
    let temp_file = NamedTempFile::new().unwrap();
    let aof_path = temp_file.path().to_path_buf();

    // Create an empty AOF file
    {
        let file = std::fs::File::create(&aof_path).unwrap();
        drop(file);
    }

    // Start server with the empty AOF file
    let (addr, server_handle, shutdown_tx) = start_test_server(Some(aof_path.clone())).await;

    // Connect client
    let mut client = connect_client(addr).await;

    // Set some data
    send_command(&mut client, "SET new_key new_value\n").await;

    // Verify data was set
    let response = send_command(&mut client, "GET new_key\n").await;
    assert_eq!(response, "new_value\n");

    // Shutdown
    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();

    // Verify the AOF file now contains data
    let metadata = std::fs::metadata(&aof_path).unwrap();
    assert!(
        metadata.len() > 0,
        "AOF file should not be empty after setting data"
    );
}

#[tokio::test]
async fn test_aof_file_size_growth() {
    let temp_file = NamedTempFile::new().unwrap();
    let aof_path = temp_file.path().to_path_buf();

    // Start server with persistence
    let (addr, server_handle, shutdown_tx) = start_test_server(Some(aof_path.clone())).await;

    // Connect client
    let mut client = connect_client(addr).await;

    // Record initial file size
    let initial_size = std::fs::metadata(&aof_path).unwrap().len();

    // Set 10 keys
    for i in 1..=10 {
        let cmd = format!("SET growth_key_{} growth_value_{}\n", i, i);
        send_command(&mut client, &cmd).await;
    }

    // Record file size after 10 SETs
    let size_after_10_sets = std::fs::metadata(&aof_path).unwrap().len();
    assert!(
        size_after_10_sets > initial_size,
        "AOF file should grow after setting keys"
    );

    // Overwrite the same key 10 times
    for i in 1..=10 {
        let cmd = format!("SET same_key iteration_{}\n", i);
        send_command(&mut client, &cmd).await;
    }

    // Record file size after 10 more operations
    let size_after_20_ops = std::fs::metadata(&aof_path).unwrap().len();
    assert!(
        size_after_20_ops > size_after_10_sets,
        "AOF file should continue to grow with overwritten keys"
    );

    // Shutdown
    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();
}

#[tokio::test]
async fn test_persistent_store_after_server_crash() {
    let temp_file = NamedTempFile::new().unwrap();
    let aof_path = temp_file.path().to_path_buf();

    // Start first server with persistence
    let (addr, server_handle, shutdown_tx) = start_test_server(Some(aof_path.clone())).await;

    // Connect client and set data
    {
        let mut client = connect_client(addr).await;
        send_command(&mut client, "SET crash_key1 value1\n").await;
        send_command(&mut client, "SET crash_key2 value2\n").await;
    }

    // Force "crash" by sending a signal and not waiting for clean shutdown
    shutdown_tx.send(()).unwrap();
    server_handle.abort(); // Force termination without waiting for clean shutdown

    // Start a second server with the same AOF file
    let (new_addr, new_server_handle, new_shutdown_tx) = start_test_server(Some(aof_path)).await;

    // Connect to the new server and verify data is still there
    let mut new_client = connect_client(new_addr).await;

    let response = send_command(&mut new_client, "GET crash_key1\n").await;
    assert_eq!(response, "value1\n");

    let response = send_command(&mut new_client, "GET crash_key2\n").await;
    assert_eq!(response, "value2\n");

    // Properly shut down the second server
    new_shutdown_tx.send(()).unwrap();
    new_server_handle.await.unwrap();
}
