use std::io;

use rust_redis::Server;

/// Application config, deliberately not part of the library.
///
/// Loopback only, so a half-built server is not exposed on the LAN.
const ADDR: &str = "127.0.0.1:6379";

fn main() -> io::Result<()> {
    let server = Server::bind(ADDR)?;
    eprintln!("listening on {}", server.local_addr()?);
    server.serve()
}
