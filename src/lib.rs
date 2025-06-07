use anyhow::Result;
use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;

pub type Db = Arc<DashMap<String, String>>;

pub async fn run_server(
    listener: TcpListener,
    shutdown_tx: broadcast::Sender<()>,
    aof_path: Option<PathBuf>,
) -> Result<()> {
    let db: Db = Arc::new(DashMap::new());
    let mut shutdown_rx = shutdown_tx.subscribe();

    let aof_file = if let Some(path) = &aof_path {
        if path.exists() {
            let file = OpenOptions::new().read(true).open(path).await?;
            let mut reader = BufReader::new(file);
            let mut line = String::new();
            println!("Restoring database from AOF...");
            while reader.read_line(&mut line).await? > 0 {
                let parts: Vec<&str> = line.trim().splitn(3, ' ').collect();
                let command = parts.get(0).unwrap_or(&"");
                match *command {
                    "SET" => {
                        if let (Some(key), Some(value)) = (parts.get(1), parts.get(2)) {
                            db.insert(key.to_string(), value.to_string());
                        }
                    }
                    "DEL" => {
                        if let Some(key) = parts.get(1) {
                            db.remove(*key);
                        }
                    }
                    _ => {}
                }
                line.clear();
            }
            println!("Database restored.");
        }
        Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .await?,
        )
    } else {
        None
    };
    let aof_mutex = Arc::new(tokio::sync::Mutex::new(aof_file));

    println!("BoltKV server started.");

    loop {
        tokio::select! {
            res = listener.accept() => {
                let (socket, _) = res?;
                let db_clone = db.clone();
                let task_shutdown_rx = shutdown_tx.subscribe();
                let aof_clone = aof_mutex.clone();

                tokio::spawn(async move {
                    process_connection(socket, db_clone, task_shutdown_rx, aof_clone).await;
                });
            }
            _ = shutdown_rx.recv() => {
                println!("Shutdown signal received. Closing server.");
                break;
            }
        }
    }
    Ok(())
}

pub async fn process_connection(
    socket: TcpStream,
    db: Db,
    mut shutdown_rx: broadcast::Receiver<()>,
    aof: Arc<tokio::sync::Mutex<Option<File>>>,
) {
    let mut reader = BufReader::new(socket);

    macro_rules! write_response {
        ($msg:expr) => {
            if reader.get_mut().write_all($msg).await.is_err() {
                return;
            }
        };
    }

    loop {
        let mut line = String::new();
        tokio::select! {
            res = reader.read_line(&mut line) => {
                match res {
                    Ok(0) => return,
                    Ok(_) => {
                        let parts: Vec<&str> = line.trim().splitn(3, ' ').collect();
                        let command = parts.get(0).unwrap_or(&"");

                        match *command {
                            "SET" | "DEL" => {
                                if let Some(file) = &mut *aof.lock().await {
                                    if file.write_all(line.as_bytes()).await.is_err() {
                                        eprintln!("failed to write to AOF");
                                    }
                                    if file.flush().await.is_err() {
                                        eprintln!("failed to flush AOF");
                                    }
                                }
                                if *command == "SET" {
                                    if let (Some(key), Some(value)) = (parts.get(1), parts.get(2)) {
                                        db.insert(key.to_string(), value.to_string());
                                        write_response!(b"OK\n");
                                    } else {
                                        write_response!(b"ERR wrong number of arguments for SET\n");
                                    }
                                } else { // DEL
                                    if let Some(key) = parts.get(1) {
                                        db.remove(*key);
                                        write_response!(b"OK\n");
                                    } else {
                                        write_response!(b"ERR wrong number of arguments for DEL\n");
                                    }
                                }
                            }
                            "GET" => {
                                if let Some(key) = parts.get(1) {
                                    let response = match db.get(*key) {
                                        Some(value_ref) => format!("{}\n", value_ref.value()),
                                        None => "NULL\n".to_string(),
                                    };
                                    write_response!(response.as_bytes());
                                } else {
                                    write_response!(b"ERR wrong number of arguments for GET\n");
                                }
                            }
                            _ => {
                                if !command.is_empty() {
                                    write_response!(b"ERR unknown command\n");
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Error reading from socket: {}", e);
                        return;
                    }
                }
            }
            _ = shutdown_rx.recv() => {
                println!("Connection closing due to server shutdown.");
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::broadcast;

    // ... All test functions remain exactly the same ...
    #[tokio::test]
    async fn test_persistence() {
        let temp_file = NamedTempFile::new().unwrap();
        let aof_path = temp_file.path().to_path_buf();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, _) = broadcast::channel(1);
        let server_task = tokio::spawn(run_server(
            listener,
            shutdown_tx.clone(),
            Some(aof_path.clone()),
        ));
        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(b"SET name persistence\n").await.unwrap();
        client.read(&mut [0; 1024]).await.unwrap();
        shutdown_tx.send(()).unwrap();
        server_task.await.unwrap().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, _) = broadcast::channel(1);
        let _server_task = tokio::spawn(run_server(listener, shutdown_tx.clone(), Some(aof_path)));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(b"GET name\n").await.unwrap();
        let mut response_buf = vec![0; 1024];
        let n = client.read(&mut response_buf).await.unwrap();
        let response = std::str::from_utf8(&response_buf[..n]).unwrap();
        assert_eq!(response, "persistence\n");
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
        ));
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        shutdown_tx.send(()).unwrap();
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

        let _server_task = tokio::spawn(run_server(listener, shutdown_tx.clone(), Some(aof_path)));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Connect a client and check the state
        let mut client = TcpStream::connect(addr).await.unwrap();

        // 1. The DELETED key should not exist.
        client.write_all(b"GET name\n").await.unwrap();
        let mut response_buf = vec![0; 1024];
        let n = client.read(&mut response_buf).await.unwrap();
        let response = std::str::from_utf8(&response_buf[..n]).unwrap();
        assert_eq!(response, "NULL\n");

        // 2. The OTHER key should still exist.
        client.write_all(b"GET status\n").await.unwrap();
        let n = client.read(&mut response_buf).await.unwrap();
        let response = std::str::from_utf8(&response_buf[..n]).unwrap();
        assert_eq!(response, "cool\n");
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
            process_connection(socket, db_clone, shutdown_rx, aof_mutex).await;
        });

        let client_stream = TcpStream::connect(addr).await.unwrap();
        let client = BufReader::new(client_stream);

        (client, db)
    }
}
