use std::io;

use rust_redis::Server;

/// Loopback only: keeps the half-built server off the LAN.
const ADDR: &str = "127.0.0.1:6379";

fn main() -> io::Result<()> {
    let server = Server::bind(ADDR)?;
    eprintln!("listening on {}", server.local_addr()?);
    server.serve()
}
