use crate::Db;
use dashmap::DashMap;
use std::io::{Read, Write};
use std::sync::Arc;
use tempfile::NamedTempFile;

#[test]
fn test_db_operations() {
    // Create a test database
    let db: Db = Arc::new(DashMap::new());

    // Test inserting a key
    db.insert("test_key".to_string(), "test_value".to_string());

    // Test getting a key - drop reference immediately to avoid deadlocks
    {
        let value = db.get("test_key").unwrap();
        assert_eq!(*value.value(), "test_value".to_string());
    }

    // Test updating a key
    db.insert("test_key".to_string(), "updated_value".to_string());

    // Get and check the updated value - drop reference immediately
    {
        let updated_value = db.get("test_key").unwrap();
        assert_eq!(*updated_value.value(), "updated_value".to_string());
    }

    // Test removing a key
    db.remove("test_key");
    assert!(db.get("test_key").is_none());
}

#[test]
fn test_command_parsing() {
    // Test SET command parsing
    let parts: Vec<&str> = "SET key value".trim().splitn(3, ' ').collect();
    assert_eq!(parts.len(), 3);
    assert_eq!(parts[0], "SET");
    assert_eq!(parts[1], "key");
    assert_eq!(parts[2], "value");

    // Test GET command parsing
    let parts: Vec<&str> = "GET key".trim().splitn(3, ' ').collect();
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0], "GET");
    assert_eq!(parts[1], "key");

    // Test DEL command parsing
    let parts: Vec<&str> = "DEL key".trim().splitn(3, ' ').collect();
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0], "DEL");
    assert_eq!(parts[1], "key");

    // Test empty command
    let parts: Vec<&str> = "".trim().splitn(3, ' ').collect();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0], "");
}

#[test]
fn test_aof_file_operations() {
    // Create a temporary file
    let mut temp_file = NamedTempFile::new().unwrap();

    // Write some commands to it
    writeln!(temp_file, "SET key1 value1").unwrap();
    writeln!(temp_file, "SET key2 value2").unwrap();
    writeln!(temp_file, "DEL key1").unwrap();

    // Create a database and load from AOF
    let db: Db = Arc::new(DashMap::new());
    let path = temp_file.path().to_path_buf();

    // Manually read and process AOF file using std::io
    let mut file = std::fs::File::open(&path).unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();

    for line in contents.lines() {
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
    }

    // Verify the database state
    assert!(db.get("key1").is_none());
    let value = db.get("key2").unwrap();
    assert_eq!(*value.value(), "value2".to_string());
}
