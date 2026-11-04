//! Blocking, thread-per-connection TCP echo server.
//!
//! - [`Server`]: socket plumbing
//! - [`Store`]: in-memory key/value map, not yet on the wire

mod store;

pub use store::Store;

use std::io::{self, ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread;

/// A bound TCP listener that has not started accepting yet.
pub struct Server {
    listener: TcpListener,
}

impl Server {
    /// Binds `addr` without accepting.
    ///
    /// - `"127.0.0.1:0"` asks the kernel for a free port
    /// - read it with [`Server::local_addr`] before [`Server::serve`]
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
    /// - one detached thread per connection
    /// - a silent client holds its thread until it disconnects
    /// - transient accept errors are retried; others end the loop
    pub fn serve(self) -> io::Result<()> {
        loop {
            let (mut stream, peer) = match self.listener.accept() {
                Ok(accepted) => accepted,
                Err(e) if is_transient(&e) => {
                    eprintln!("accept failed (transient, continuing): {e}");
                    continue;
                }
                // Other errors (fd exhaustion, broken listener) would hot-loop on stderr.
                Err(e) => return Err(e),
            };

            // Best-effort Nagle disable: ~40ms less latency on small round trips.
            // A failure is not fatal; echo with Nagle on.
            let _ = stream.set_nodelay(true);

            eprintln!("accepted connection from {peer}");

            if let Err(e) = thread::Builder::new().spawn(move || {
                if let Err(e) = handle_connection(&mut stream) {
                    eprintln!("connection error from {peer}: {e}");
                }
            }) {
                eprintln!("failed to spawn connection thread from {peer}: {e}");
            }
        }
    }
}

/// Echoes each read back until the peer half-closes.
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
