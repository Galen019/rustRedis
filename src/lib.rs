//! Blocking, thread-per-connection RESP2 server.
//!
//! - [`Server`]: socket plumbing and the connection loop
//! - `resp`: request framing and reply encoding
//! - `command`: command dispatch over the [`Store`]
//! - `expiry`: relative TTL parsing and deadline math
//! - [`Store`]: in-memory key/value map with lazy expiry

mod command;
mod expiry;
mod resp;
mod store;

pub use store::{Store, Ttl};

use std::io::{self, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use crate::resp::{Reply, RequestError};

/// Frame sent to a peer over the connection cap.
const MAX_CLIENTS_ERROR: &[u8] = b"-ERR max number of clients reached\r\n";

/// A bound TCP listener that has not started accepting yet.
pub struct Server {
    listener: TcpListener,
    store: Arc<Store>,
    idle_timeout: Option<Duration>,
    max_connections: Option<usize>,
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
            max_connections: None,
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

    /// Caps concurrently served connections.
    ///
    /// - `None` or `Some(0)` means no limit
    /// - an extra peer gets `-ERR max number of clients reached`, then a close
    /// - a silent client holds its slot until it closes or an idle timeout fires
    pub fn max_connections(mut self, max: Option<usize>) -> Self {
        self.max_connections = max.filter(|&max| max != 0);
        self
    }

    /// The address the socket actually bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Accepts connections forever.
    ///
    /// - one detached thread per connection, all sharing the store
    /// - connections over the cap are rejected without a thread
    /// - with an idle timeout, each socket is armed so a stalled peer ends it
    /// - each connection end is logged once with its reason
    /// - transient accept errors are retried; others end the loop
    pub fn serve(self) -> io::Result<()> {
        let connections = Arc::new(Connections::new(self.max_connections));
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

            let Some(permit) = connections.try_acquire() else {
                reject(&stream);
                eprintln!("rejected connection from {peer}: connection limit reached");
                continue;
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
            let spawned = thread::Builder::new().spawn(move || {
                let _permit = permit;
                match handle_connection(stream, &store) {
                    Ok(end) => eprintln!("disconnected {peer}: {}", end.reason()),
                    Err(e) => eprintln!("connection error from {peer}: {e}"),
                }
            });

            if let Err(e) = spawned {
                eprintln!("failed to spawn connection thread from {peer}: {e}");
            }
        }
    }
}

/// Live connection count against a shared ceiling.
struct Connections {
    live: AtomicUsize,
    max: Option<usize>,
}

impl Connections {
    fn new(max: Option<usize>) -> Self {
        Self {
            live: AtomicUsize::new(0),
            max,
        }
    }

    /// Reserves a slot, or `None` when the ceiling is already reached.
    ///
    /// - only the accept thread increments, so no lock is needed
    /// - a rejected attempt puts its increment back
    fn try_acquire(self: &Arc<Self>) -> Option<ConnectionPermit> {
        let previous = self.live.fetch_add(1, Ordering::Relaxed);
        if self.max.is_some_and(|max| previous >= max) {
            self.live.fetch_sub(1, Ordering::Relaxed);
            return None;
        }
        Some(ConnectionPermit {
            connections: Arc::clone(self),
        })
    }
}

/// Frees its slot on drop, including while unwinding.
struct ConnectionPermit {
    connections: Arc<Connections>,
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        self.connections.live.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Tells a peer over the cap why it is being closed.
fn reject(mut stream: &TcpStream) {
    // The peer may already be gone; the close is the same either way.
    let _ = stream.write_all(MAX_CLIENTS_ERROR);
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
            Ok(Some(args)) => match command::execute(store, &args, Instant::now()) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permit_is_released_on_drop() {
        let connections = Arc::new(Connections::new(Some(1)));
        let first = connections.try_acquire().expect("first slot");

        // A rejected attempt must not leak its increment.
        assert!(connections.try_acquire().is_none());
        assert_eq!(connections.live.load(Ordering::Relaxed), 1);

        drop(first);
        assert!(connections.try_acquire().is_some());
    }

    #[test]
    fn zero_cap_is_unlimited() {
        let server = Server::bind("127.0.0.1:0", Arc::new(Store::new())).expect("bind");
        assert!(server.max_connections(Some(0)).max_connections.is_none());
    }

    #[test]
    fn none_means_unlimited() {
        let connections = Arc::new(Connections::new(None));
        let permits: Vec<_> = (0..4)
            .map(|_| connections.try_acquire().expect("unlimited slot"))
            .collect();
        assert_eq!(permits.len(), 4);
    }

    #[test]
    fn panic_releases_the_permit() {
        let connections = Arc::new(Connections::new(Some(1)));
        let held = Arc::clone(&connections);

        let outcome = thread::spawn(move || {
            let _permit = held.try_acquire().expect("slot");
            panic!("unwind holding the permit");
        })
        .join();

        assert!(outcome.is_err());
        assert!(connections.try_acquire().is_some());
    }
}
