# BoltKV

BoltKV is a simple key-value store server written in Rust, with support for persistence through an append-only file (AOF).

## Features

- Fast in-memory key-value storage
- Simple SET/GET/DEL commands
- Persistent storage with AOF (Append-Only File)
- Concurrent connections with Tokio
- Graceful shutdown mechanism

## Getting Started

### Prerequisites

- Rust and Cargo (install from [rustup.rs](https://rustup.rs/))

### Building the Project

```bash
cargo build --release
```

### Running the Server

```bash
cargo run --release
```

### Usage Examples

Connect to the server using a TCP client like netcat:

```bash
nc localhost 6379
```

Once connected, you can use the following commands:

- `SET key value` - Store a key-value pair
- `GET key` - Retrieve a value by key
- `DEL key` - Delete a key-value pair

## Development

### Development Setup

1. Clone the repository
2. Install pre-commit hooks:

```bash
# Install pre-commit (macOS)
brew install pre-commit

# Or with pip
pip install pre-commit

# Set up the hooks
./scripts/setup-hooks.sh
```

The pre-commit hooks ensure that:
- Code is properly formatted with `cargo fmt`
- All Clippy lints pass
- All tests pass
- No trailing whitespace or missing EOF newlines

### Running Tests

```bash
cargo test
```

### Code Quality

This project uses:
- `cargo fmt` for code formatting
- `clippy` for linting
- Pre-commit hooks to enforce quality standards

## License

This project is licensed under the MIT License - see the LICENSE file for details.
