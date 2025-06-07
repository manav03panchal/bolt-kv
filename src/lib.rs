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
                let command = parts.first().copied().unwrap_or("");
                match command {
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
                        let command = parts.first().unwrap_or(&"");

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
