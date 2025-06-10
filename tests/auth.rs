// tests/auth.rs

// This line tells Rust to look for a `common` module in the `tests` directory.
mod common;
use common::{connect_client, send_command, start_test_server};

#[tokio::test]
async fn test_auth_flow() {
    // 1. Start the server in secure mode with a password.
    let password = "supersecretpassword".to_string();
    let (addr, server_handle, shutdown_tx) = start_test_server(None, Some(password.clone())).await;

    // 2. Connect a new client.
    let mut client = connect_client(addr).await;

    // 3. Trying a command before authenticating should fail.
    let res = send_command(&mut client, "GET some_key\n").await;
    assert_eq!(res, "ERR NOAUTH Authentication required.\n");

    // 4. Trying to authenticate with the wrong password should fail.
    let res = send_command(&mut client, "AUTH wrongpass\n").await;
    assert_eq!(res, "ERR invalid password\n");

    // 5. Authenticating with the correct password should succeed.
    let auth_cmd = format!("AUTH {}\n", password);
    let res = send_command(&mut client, &auth_cmd).await;
    assert_eq!(res, "OK\n");

    // 6. Now, commands should work.
    let res = send_command(&mut client, "SET name bolt\n").await;
    assert_eq!(res, "OK\n");
    let res = send_command(&mut client, "GET name\n").await;
    assert_eq!(res, "bolt\n");

    // 7. Cleanly shut down the server.
    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();
}

#[tokio::test]
async fn test_unsecured_server() {
    // Start the server WITHOUT a password.
    let (addr, server_handle, shutdown_tx) = start_test_server(None, None).await;
    let mut client = connect_client(addr).await;

    // Commands should work immediately without AUTH.
    let res = send_command(&mut client, "SET name bolt\n").await;
    assert_eq!(res, "OK\n");

    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();
}
