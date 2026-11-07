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
use std::time::Duration;

use crate::resp::{Reply, RequestError};

/// A bound TCP listener that has not started accepting yet.
pub struct Server {
    listener: TcpListener,
    store: Arc<Store>,
    idle_timeout: Option<Duration>,
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
            idle_timeout: None,
        })
    }

    /// Closes connections that stall for `timeout`.
    ///
    /// - `None` or a zero duration leaves sockets blocking with no deadline
    /// - armed as read and write timeouts, so a client that stops mid-request
    ///   is closed too
    pub fn idle_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.idle_timeout = timeout.filter(|timeout| !timeout.is_zero());
        self
    }

    /// The address the socket actually bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Accepts connections forever.
    ///
    /// - one detached thread per connection, all sharing the store
    /// - with an idle timeout, each socket is armed so a stalled peer ends it
    /// - each connection end is logged once with its reason
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

            if let Some(timeout) = self.idle_timeout {
                // Silently losing the timeout would leave the connection unwatched.
                if let Err(e) = arm_timeouts(&stream, timeout) {
                    eprintln!("failed to set idle timeout for {peer}: {e}");
                }
            }

            eprintln!("accepted connection from {peer}");

            let store = Arc::clone(&self.store);
            let spawned =
                thread::Builder::new().spawn(move || match handle_connection(stream, &store) {
                    Ok(end) => eprintln!("disconnected {peer}: {}", end.reason()),
                    Err(e) => eprintln!("connection error from {peer}: {e}"),
                });

            if let Err(e) = spawned {
                eprintln!("failed to spawn connection thread from {peer}: {e}");
            }
        }
    }
}

/// Why a connection thread ended.
enum Disconnected {
    /// peer closed at a frame boundary
    Eof,
    /// peer sent `QUIT`
    Quit,
    /// the peer sent or drained nothing before the socket timeout
    Timeout,
    /// framing broke; the peer already got the error reply
    Protocol(String),
}

impl Disconnected {
    fn reason(&self) -> &str {
        match self {
            Self::Eof => "peer closed the connection",
            Self::Quit => "quit",
            Self::Timeout => "idle timeout",
            Self::Protocol(detail) => detail,
        }
    }
}

/// Serves RESP2 requests until EOF, `QUIT`, a protocol error, an idle timeout,
/// or another I/O failure.
///
/// - returns the end reason instead of logging; the caller owns stderr
/// - a timed-out read or write is a normal end, not an error
fn handle_connection(stream: TcpStream, store: &Store) -> io::Result<Disconnected> {
    let mut stream = BufReader::new(stream);
    let mut out = Vec::new();

    loop {
        out.clear();
        let (reply, end) = match resp::read_request(&mut stream) {
            // `Ok(None)` is EOF at a frame boundary, not an error.
            Ok(None) => return Ok(Disconnected::Eof),
            Ok(Some(args)) => match command::execute(store, &args) {
                // `*0` is a request with no command in it: nothing to reply with.
                None => continue,
                Some(executed) => (executed.reply, executed.close.then_some(Disconnected::Quit)),
            },
            Err(RequestError::Protocol(e)) => {
                // The stream is unparseable: reply, then drop the connection.
                let detail = e.detail();
                (
                    Reply::Error(format!("ERR Protocol error: {detail}")),
                    Some(Disconnected::Protocol(format!("protocol error: {detail}"))),
                )
            }
            Err(RequestError::Io(e)) if is_timeout(&e) => return Ok(Disconnected::Timeout),
            Err(RequestError::Io(e)) => return Err(e),
        };

        resp::encode(&reply, &mut out);
        if let Err(e) = stream.get_mut().write_all(&out) {
            return if is_timeout(&e) {
                Ok(Disconnected::Timeout)
            } else {
                Err(e)
            };
        }

        if let Some(end) = end {
            return Ok(end);
        }
    }
}

/// Arms the idle deadline on both directions.
fn arm_timeouts(stream: &TcpStream, timeout: Duration) -> io::Result<()> {
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))
}

/// A socket timeout: the peer sent or drained nothing within the deadline.
fn is_timeout(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
    )
}

fn is_transient(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::ConnectionAborted | io::ErrorKind::Interrupted
    )
}
