use std::io;
use std::sync::Arc;

use rust_redis::{Server, Store};

/// Loopback only: keeps the half-built server off the LAN.
const ADDR: &str = "127.0.0.1:6379";

fn main() -> io::Result<()> {
    let store = Arc::new(Store::new());
    let server = Server::bind(ADDR, store)?;
    eprintln!("listening on {}", server.local_addr()?);
    server.serve()
}
