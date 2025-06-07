use bolt_kv::run_server;
use std::time::Duration;
use tempfile::NamedTempFile;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

async fn start_test_server(
    aof_path: Option<std::path::PathBuf>,
) -> (std::net::SocketAddr, JoinHandle<()>, broadcast::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    let server_shutdown_tx = shutdown_tx.clone();
    let server_handle = tokio::spawn(async move {
        run_server(listener, server_shutdown_tx, aof_path)
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

#[tokio::test]
async fn test_empty_key() {
    // Start server
    let (addr, server_handle, shutdown_tx) = start_test_server(None).await;

    // Connect client
    let mut client = connect_client(addr).await;

    // Test with malformed SET command (missing value)
    write_and_read_response(
        &mut client,
        "SET \n",
        Some("ERR wrong number of arguments for SET\n"),
    )
    .await;

    // Test with malformed SET command (missing value after key)
    write_and_read_response(
        &mut client,
        "SET empty_key\n",
        Some("ERR wrong number of arguments for SET\n"),
    )
    .await;

    // Proper empty value (valid)
    write_and_read_response(&mut client, "SET empty_value_key empty\n", Some("OK\n")).await;
    write_and_read_response(&mut client, "GET empty_value_key\n", Some("empty\n")).await;

    // Shutdown
    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();
}

#[tokio::test]
async fn test_special_characters() {
    // Start server
    let (addr, server_handle, shutdown_tx) = start_test_server(None).await;

    // Connect client
    let mut client = connect_client(addr).await;

    // Test key with special characters
    let special_key = "key!@#$%^&*()_+";
    let special_value = "value<>?,./;'[]\\{}|\"";

    let set_cmd = format!("SET {} {}\n", special_key, special_value);
    write_and_read_response(&mut client, &set_cmd, Some("OK\n")).await;

    let get_cmd = format!("GET {}\n", special_key);
    let response = write_and_read_response(&mut client, &get_cmd, None).await;
    assert_eq!(response.trim(), special_value);

    // Shutdown
    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();
}

#[tokio::test]
async fn test_very_large_values() {
    // Start server
    let (addr, server_handle, shutdown_tx) = start_test_server(None).await;

    // Connect client
    let mut client = connect_client(addr).await;

    // Generate a large value (100KB)
    let large_value = "x".repeat(100 * 1024);
    let set_cmd = format!("SET large_key {}\n", large_value);

    // Set large value
    client
        .get_mut()
        .write_all(set_cmd.as_bytes())
        .await
        .unwrap();
    let mut response = String::new();
    client.read_line(&mut response).await.unwrap();
    assert_eq!(response, "OK\n");

    // Verify we can get it back
    client
        .get_mut()
        .write_all(b"GET large_key\n")
        .await
        .unwrap();

    // Reading large values requires different approach
    let mut buffer = Vec::new();
    let mut line_reader = BufReader::new(client.get_mut());
    line_reader.read_until(b'\n', &mut buffer).await.unwrap();

    let received_value = String::from_utf8(buffer).unwrap();
    assert_eq!(received_value.trim(), large_value);

    // Shutdown
    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();
}

#[tokio::test]
async fn test_non_ascii_characters() {
    // Start server
    let (addr, server_handle, shutdown_tx) = start_test_server(None).await;

    // Connect client
    let mut client = connect_client(addr).await;

    // Unicode characters
    let unicode_key = "unicode_key";
    let unicode_value = "こんにちは世界! 🚀 привет мир! 你好世界!";

    // Set unicode value
    let set_cmd = format!("SET {} {}\n", unicode_key, unicode_value);
    write_and_read_response(&mut client, &set_cmd, Some("OK\n")).await;

    // Get the unicode value back
    let get_cmd = format!("GET {}\n", unicode_key);
    let response = write_and_read_response(&mut client, &get_cmd, None).await;
    assert_eq!(response.trim(), unicode_value);

    // Shutdown
    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();
}

#[tokio::test]
async fn test_command_case_sensitivity() {
    // Start server
    let (addr, server_handle, shutdown_tx) = start_test_server(None).await;

    // Connect client
    let mut client = connect_client(addr).await;

    // Test lowercase command
    write_and_read_response(
        &mut client,
        "set lowercase_key value\n",
        Some("ERR unknown command\n"),
    )
    .await;

    // Test mixed case command
    write_and_read_response(
        &mut client,
        "SeT mixedcase_key value\n",
        Some("ERR unknown command\n"),
    )
    .await;

    // Test correct uppercase command
    write_and_read_response(&mut client, "SET uppercase_key value\n", Some("OK\n")).await;

    // Shutdown
    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();
}

#[tokio::test]
async fn test_malformed_command_syntax() {
    // Start server
    let (addr, server_handle, shutdown_tx) = start_test_server(None).await;

    // Connect client
    let mut client = connect_client(addr).await;

    // Test command with missing arguments
    write_and_read_response(
        &mut client,
        "SET\n",
        Some("ERR wrong number of arguments for SET\n"),
    )
    .await;
    write_and_read_response(
        &mut client,
        "GET\n",
        Some("ERR wrong number of arguments for GET\n"),
    )
    .await;
    write_and_read_response(
        &mut client,
        "DEL\n",
        Some("ERR wrong number of arguments for DEL\n"),
    )
    .await;

    // Shutdown
    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();
}

#[tokio::test]
async fn test_broken_connection() {
    // Start server
    let (addr, server_handle, shutdown_tx) = start_test_server(None).await;

    // First client sets a key
    let mut client1 = connect_client(addr).await;
    write_and_read_response(
        &mut client1,
        "SET important_key important_value\n",
        Some("OK\n"),
    )
    .await;

    // Drop the client connection abruptly
    drop(client1);

    // Connect a new client and verify the key is still there
    let mut client2 = connect_client(addr).await;
    write_and_read_response(
        &mut client2,
        "GET important_key\n",
        Some("important_value\n"),
    )
    .await;

    // Shutdown
    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();
}

#[tokio::test]
async fn test_persistence_error_recovery() {
    // Create temporary file for AOF
    let temp_file = NamedTempFile::new().unwrap();
    let aof_path = temp_file.path().to_path_buf();

    // Start server with persistence
    let (addr, server_handle, shutdown_tx) = start_test_server(Some(aof_path.clone())).await;

    // Connect client and set some data
    let mut client = connect_client(addr).await;
    write_and_read_response(
        &mut client,
        "SET recovery_key recovery_value\n",
        Some("OK\n"),
    )
    .await;

    // Shutdown the server
    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();

    // Manually corrupt the AOF file by appending invalid data
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&aof_path)
        .unwrap();
    use std::io::Write;
    file.write_all(b"INVALID COMMAND\n").unwrap();
    file.flush().unwrap();
    drop(file);

    // Start a new server with the corrupted AOF file
    // It should still recover the valid parts
    let (new_addr, new_server_handle, new_shutdown_tx) = start_test_server(Some(aof_path)).await;

    // Connect to the new server and verify data is still there
    let mut new_client = connect_client(new_addr).await;
    write_and_read_response(
        &mut new_client,
        "GET recovery_key\n",
        Some("recovery_value\n"),
    )
    .await;

    // Shutdown
    new_shutdown_tx.send(()).unwrap();
    new_server_handle.await.unwrap();
}
