//! Blocking, thread-per-connection RESP2 server.
//!
//! - [`Server`]: socket plumbing and the connection loop
//! - `resp`: request framing and reply encoding
//! - `command`: command dispatch over the [`Store`]
//! - [`Store`]: in-memory key/value map

mod command;
mod resp;
mod store;

pub use store::Store;

use std::io::{self, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use crate::resp::{Reply, RequestError};

/// A bound TCP listener that has not started accepting yet.
pub struct Server {
    listener: TcpListener,
    store: Arc<Store>,
}

impl Server {
    /// Binds `addr` without accepting; every connection shares `store`.
    ///
    /// - `"127.0.0.1:0"` asks the kernel for a free port
    /// - read it with [`Server::local_addr`] before [`Server::serve`]
    pub fn bind(addr: &str, store: Arc<Store>) -> io::Result<Self> {
        Ok(Self {
            listener: TcpListener::bind(addr)?,
            store,
        })
    }

    /// The address the socket actually bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Accepts connections forever.
    ///
    /// - one detached thread per connection, all sharing the store
    /// - a silent client holds its thread until it disconnects
    /// - transient accept errors are retried; others end the loop
    pub fn serve(self) -> io::Result<()> {
        loop {
            let (stream, peer) = match self.listener.accept() {
                Ok(accepted) => accepted,
                Err(e) if is_transient(&e) => {
                    eprintln!("accept failed (transient, continuing): {e}");
                    continue;
                }
                // Other errors (fd exhaustion, broken listener) would hot-loop on stderr.
                Err(e) => return Err(e),
            };

            // Best-effort Nagle disable: ~40ms less latency on small round trips.
            // A failure is not fatal; serve with Nagle on.
            let _ = stream.set_nodelay(true);

            eprintln!("accepted connection from {peer}");

            let store = Arc::clone(&self.store);
            if let Err(e) = thread::Builder::new().spawn(move || {
                if let Err(e) = handle_connection(stream, &store, peer) {
                    eprintln!("connection error from {peer}: {e}");
                }
            }) {
                eprintln!("failed to spawn connection thread from {peer}: {e}");
            }
        }
    }
}

/// Serves RESP2 requests until the peer closes or the framing breaks.
fn handle_connection(stream: TcpStream, store: &Store, peer: SocketAddr) -> io::Result<()> {
    let mut stream = BufReader::new(stream);
    let mut out = Vec::new();

    loop {
        out.clear();
        let (reply, close) = match resp::read_request(&mut stream) {
            // `Ok(None)` is EOF at a frame boundary, not an error.
            Ok(None) => break,
            Ok(Some(args)) => match command::execute(store, &args) {
                // `*0` is a request with no command in it: nothing to reply with.
                None => continue,
                Some(executed) => (executed.reply, executed.close),
            },
            Err(RequestError::Protocol(e)) => {
                // The stream is unparseable: reply, then drop the connection.
                eprintln!("protocol error from {peer}: {}", e.detail());
                (
                    Reply::Error(format!("ERR Protocol error: {}", e.detail())),
                    true,
                )
            }
            Err(RequestError::Io(e)) => return Err(e),
        };

        resp::encode(&reply, &mut out);
        stream.get_mut().write_all(&out)?;

        if close {
            break;
        }
    }
    Ok(())
}

fn is_transient(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::ConnectionAborted | io::ErrorKind::Interrupted
    )
}
