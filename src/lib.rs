use anyhow::Result;
use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tracing::{error, info};

pub type Db = Arc<DashMap<String, String>>;

// This functions takes a listener, a shutdown transmitter and an aof path
pub async fn run_server(
    listener: TcpListener,
    shutdown_tx: broadcast::Sender<()>,
    aof_path: Option<PathBuf>,
    password: Option<String>, // New argument
) -> Result<()> {
    // make an instance of Db and subscribe to the shutdown transmitter.
    let db: Db = Arc::new(DashMap::new());
    let mut shutdown_rx = shutdown_tx.subscribe();

    // if  aof_path found, replay all actions to come to latest state
    // TODO: make this a function? maybe restore_db()?
    let aof_file = if let Some(path) = &aof_path {
        if path.exists() {
            let file = OpenOptions::new().read(true).open(path).await?;
            let mut reader = BufReader::new(file);
            let mut line = String::new();
            info!("Restoring database from AOF...");
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
            info!("Database restored.");
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

    // create thread-safe shared reference to a file that can be safely accessed from multiple async tasks
    let aof_mutex = Arc::new(tokio::sync::Mutex::new(aof_file));

    // Wrap password in an Arc for sharing
    let password = Arc::new(password);

    info!("BoltKV server started.");

    // run loop till shutdown signal not received.
    loop {
        tokio::select! {
            res = listener.accept() => {
                let (socket, _) = res?;
                let db_clone = db.clone();
                let task_shutdown_rx = shutdown_tx.subscribe();
                let aof_clone = aof_mutex.clone();
                let password_clone = password.clone(); // Clone the Arc handle

                tokio::spawn(async move {
                    process_connection(socket, db_clone, task_shutdown_rx, aof_clone, password_clone).await;
                });
            }
            _ = shutdown_rx.recv() => {
                info!("Shutdown signal received. Closing server.");
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
    password: Arc<Option<String>>, // New argument
) {
    let mut reader = BufReader::new(socket);
    // The bouncer's state for this connection.
    // If no password is required server-wide, they are authenticated by default.
    let mut is_authenticated = password.is_none();

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
                        let command = parts.first().copied().unwrap_or("");

                        // If the command is AUTH, handle it specially.
                        if command == "AUTH" {
                            if let Some(required_pass) = password.as_ref() {
                                if let Some(submitted_pass) = parts.get(1) {
                                    if submitted_pass == required_pass {
                                        is_authenticated = true;
                                        write_response!(b"OK\n");
                                    } else {
                                        write_response!(b"ERR invalid password\n");
                                    }
                                }
                            }
                            continue; // Go to next loop iteration
                        }

                        // The bouncer checks their hand stamp.
                        if !is_authenticated {
                            write_response!(b"ERR NOAUTH Authentication required.\n");
                            continue; // Go to next loop iteration
                        }

                        match command {
                            "SET" | "DEL" => {
                                if let Some(file) = &mut *aof.lock().await {
                                    if file.write_all(line.as_bytes()).await.is_err() {
                                        error!("failed to write to AOF");
                                    }
                                    if file.flush().await.is_err() {
                                        error!("failed to flush AOF");
                                    }
                                }
                                if command == "SET" {
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
                        error!(cause = %e , "Error reading from socket");
                        return;
                    }
                }
            }
            _ = shutdown_rx.recv() => {
                info!("Connection closing due to server shutdown.");
                return;
            }
        }
    }
}
