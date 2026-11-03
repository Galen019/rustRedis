//! A minimal blocking, thread-per-connection TCP server.
//!
//! This is the socket plumbing only: it accepts connections and echoes every
//! byte back, verbatim, until the client closes its write half. No protocol.

use std::io::{self, ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread;

/// A bound TCP listener that has not started accepting yet.
///
/// Binding and serving are separate so callers can read the actually-bound
/// address (see [`Server::local_addr`]) *before* the accept loop starts, which
/// is what makes port `0` usable in tests without retries or sleeps.
pub struct Server {
    listener: TcpListener,
}

impl Server {
    /// Binds `addr`, e.g. `"127.0.0.1:6379"` or `"127.0.0.1:0"` for a
    /// kernel-assigned port. The library never hardcodes an address.
    pub fn bind(addr: &str) -> io::Result<Self> {
        Ok(Self {
            listener: TcpListener::bind(addr)?,
        })
    }

    /// The address the socket actually bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Accepts connections forever.
    ///
    /// Each connection is handled on a detached thread
    /// killing the process severs every in-flight connection
    /// A client that connects and never sends anything holds its thread until it goes away
    pub fn serve(self) -> io::Result<()> {
        loop {
            let (mut stream, _) = match self.listener.accept() {
                Ok(accepted) => accepted,
                Err(e) if is_transient(&e) => {
                    eprintln!("accept failed (transient, continuing): {e}");
                    continue;
                }
                // Anything else (fd exhaustion, broken listener) would turn
                // into a hot stderr-spamming loop if we continued.
                Err(e) => return Err(e),
            };

            // Disable Nagle because it adds ~40ms of latency to small request/response pairs.
            if let Err(e) = stream.set_nodelay(true) {
                eprintln!("failed to set TCP_NODELAY: {e}");
                continue;
            }

            let peer = peer_addr(&stream);
            eprintln!("accepted connection from {peer}");

            if let Err(e) = thread::Builder::new().spawn(move || {
                if let Err(e) = handle_connection(&mut stream) {
                    eprintln!("connection error from {}: {e}", peer_addr(&stream));
                }
            }) {
                eprintln!("failed to spawn connection thread from {peer}: {e}");
            }
        }
    }
}

/// Echoes bytes back until the peer closes its write half.
fn handle_connection(stream: &mut TcpStream) -> io::Result<()> {
    let mut buf = [0u8; 4096];
    loop {
        match stream.read(&mut buf) {
            // `Ok(0)` is EOF, not an error: the peer will send nothing more.
            Ok(0) => break,
            Ok(n) => stream.write_all(&buf[..n])?,
            Err(e) if e.kind() == ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

fn is_transient(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        ErrorKind::ConnectionAborted | ErrorKind::Interrupted
    )
}

fn peer_addr(stream: &TcpStream) -> String {
    stream
        .peer_addr()
        .map(|addr| addr.to_string())
        .unwrap_or_else(|_| "<unknown>".to_string())
}
