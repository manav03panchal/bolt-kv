use bolt_kv::run_server;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

async fn start_test_server() -> (std::net::SocketAddr, JoinHandle<()>, broadcast::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    let server_shutdown_tx = shutdown_tx.clone();
    let server_handle = tokio::spawn(async move {
        run_server(listener, server_shutdown_tx, None, None)
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
async fn test_set_multiple_keys() {
    // Start server
    let (addr, server_handle, shutdown_tx) = start_test_server().await;

    // Connect client
    let mut client = connect_client(addr).await;

    // Set multiple keys
    for i in 1..=100 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        let cmd = format!("SET {} {}\n", key, value);

        let response = send_command(&mut client, &cmd).await;
        assert_eq!(response, "OK\n");
    }

    // Verify keys were set correctly
    for i in 1..=100 {
        let key = format!("key_{}", i);
        let expected_value = format!("value_{}", i);
        let cmd = format!("GET {}\n", key);

        let response = send_command(&mut client, &cmd).await;
        assert_eq!(response, format!("{}\n", expected_value));
    }

    // Shutdown
    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();
}

#[tokio::test]
async fn test_overwrite_key() {
    // Start server
    let (addr, server_handle, shutdown_tx) = start_test_server().await;

    // Connect client
    let mut client = connect_client(addr).await;

    // Set and then overwrite a key
    send_command(&mut client, "SET overwrite_key initial_value\n").await;
    let response = send_command(&mut client, "GET overwrite_key\n").await;
    assert_eq!(response, "initial_value\n");

    // Overwrite with a new value
    send_command(&mut client, "SET overwrite_key new_value\n").await;
    let response = send_command(&mut client, "GET overwrite_key\n").await;
    assert_eq!(response, "new_value\n");

    // Overwrite with an empty string
    send_command(&mut client, "SET overwrite_key \"\"\n").await;
    let response = send_command(&mut client, "GET overwrite_key\n").await;
    assert_eq!(response, "\"\"\n");

    // Shutdown
    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();
}

#[tokio::test]
async fn test_delete_nonexistent_key() {
    // Start server
    let (addr, server_handle, shutdown_tx) = start_test_server().await;

    // Connect client
    let mut client = connect_client(addr).await;

    // Delete a non-existent key
    let response = send_command(&mut client, "DEL nonexistent_key\n").await;

    // This should succeed even though the key doesn't exist
    assert_eq!(response, "OK\n");

    // Shutdown
    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();
}

#[tokio::test]
async fn test_get_after_delete() {
    // Start server
    let (addr, server_handle, shutdown_tx) = start_test_server().await;

    // Connect client
    let mut client = connect_client(addr).await;

    // Set a key
    send_command(&mut client, "SET delete_test_key delete_test_value\n").await;
    let response = send_command(&mut client, "GET delete_test_key\n").await;
    assert_eq!(response, "delete_test_value\n");

    // Delete the key
    send_command(&mut client, "DEL delete_test_key\n").await;

    // Get the deleted key should return NULL
    let response = send_command(&mut client, "GET delete_test_key\n").await;
    assert_eq!(response, "NULL\n");

    // Shutdown
    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();
}

#[tokio::test]
async fn test_complex_values() {
    // Start server
    let (addr, server_handle, shutdown_tx) = start_test_server().await;

    // Connect client
    let mut client = connect_client(addr).await;

    // Test with JSON-like values
    let json_value = r#"{"name":"test","data":["a","b",1,2],"nested":{"field":true}}"#;
    let cmd = format!("SET json_key {}\n", json_value);

    send_command(&mut client, &cmd).await;
    let response = send_command(&mut client, "GET json_key\n").await;
    assert_eq!(response, format!("{}\n", json_value));

    // Test with URL-like values
    let url_value = "https://example.com/path?param1=value1&param2=value2";
    let cmd = format!("SET url_key {}\n", url_value);

    send_command(&mut client, &cmd).await;
    let response = send_command(&mut client, "GET url_key\n").await;
    assert_eq!(response, format!("{}\n", url_value));

    // Shutdown
    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();
}

#[tokio::test]
async fn test_command_with_trailing_spaces() {
    // Start server
    let (addr, server_handle, shutdown_tx) = start_test_server().await;

    // Connect client
    let mut client = connect_client(addr).await;

    // Command with trailing spaces
    let response = send_command(&mut client, "SET trailing_key trailing_value   \n").await;

    // This should still work as the command parser is expected to trim
    assert_eq!(response, "OK\n");

    // Verify the key was set correctly
    let response = send_command(&mut client, "GET trailing_key\n").await;
    assert_eq!(response, "trailing_value\n");

    // Shutdown
    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();
}

#[tokio::test]
async fn test_repeated_gets_and_sets() {
    // Start server
    let (addr, server_handle, shutdown_tx) = start_test_server().await;

    // Connect client
    let mut client = connect_client(addr).await;

    // Repeatedly set and get the same key
    for i in 1..=10 {
        let value = format!("value_iteration_{}", i);
        let set_cmd = format!("SET repeated_key {}\n", value);

        // Set the key
        send_command(&mut client, &set_cmd).await;

        // Get the key to verify
        let response = send_command(&mut client, "GET repeated_key\n").await;
        assert_eq!(response, format!("{}\n", value));
    }

    // Shutdown
    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();
}

#[tokio::test]
async fn test_multiple_connections_same_key() {
    // Start server
    let (addr, server_handle, shutdown_tx) = start_test_server().await;

    // Set a value with the first client
    let mut client1 = connect_client(addr).await;
    send_command(&mut client1, "SET shared_key initial\n").await;

    // Connect a second client and verify it sees the same value
    let mut client2 = connect_client(addr).await;
    let response = send_command(&mut client2, "GET shared_key\n").await;
    assert_eq!(response, "initial\n");

    // Change the value with the second client
    send_command(&mut client2, "SET shared_key updated\n").await;

    // Verify the first client sees the updated value
    let response = send_command(&mut client1, "GET shared_key\n").await;
    assert_eq!(response, "updated\n");

    // Delete with the first client
    send_command(&mut client1, "DEL shared_key\n").await;

    // Verify it's gone for the second client
    let response = send_command(&mut client2, "GET shared_key\n").await;
    assert_eq!(response, "NULL\n");

    // Shutdown
    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();
}
