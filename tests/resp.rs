use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use rust_redis::{Server, Store};

/// Generous, but finite: a regression must fail, not hang `cargo test`.
const TIMEOUT: Duration = Duration::from_secs(5);

/// Binds port 0 and serves from a detached thread over `store`.
///
/// - the listener is live before return, so no sleep or retry is needed
fn start_server(store: Arc<Store>) -> SocketAddr {
    start_server_with(store, |server| server)
}

/// Binds port 0 and serves from a detached thread with `configure` applied.
fn start_server_with(store: Arc<Store>, configure: impl FnOnce(Server) -> Server) -> SocketAddr {
    let server = configure(Server::bind("127.0.0.1:0", store).expect("bind"));
    let addr = server.local_addr().expect("local_addr");

    // Detached on purpose: this thread is never joined and dies with the test
    // process.
    thread::spawn(move || {
        let _ = server.serve();
    });

    addr
}

fn connect(addr: SocketAddr) -> TcpStream {
    let stream = TcpStream::connect(addr).expect("connect");
    // Both directions need a timeout: a stuck server must fail the test, not
    // block it forever.
    stream
        .set_read_timeout(Some(TIMEOUT))
        .expect("set read timeout");
    stream
        .set_write_timeout(Some(TIMEOUT))
        .expect("set write timeout");
    stream
}

/// Builds a RESP2 request from raw argument bytes.
fn command(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for part in parts {
        out.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        out.extend_from_slice(part);
        out.extend_from_slice(b"\r\n");
    }
    out
}

fn send(client: &mut TcpStream, bytes: &[u8]) {
    client.write_all(bytes).expect("write");
}

/// Asserts the next bytes on the wire are exactly `expected`.
fn expect_reply(client: &mut TcpStream, expected: &[u8]) {
    let mut actual = vec![0u8; expected.len()];
    client.read_exact(&mut actual).expect("read reply");
    assert_eq!(actual, expected);
}

fn expect_eof(client: &mut TcpStream) {
    let mut byte = [0u8; 1];
    assert_eq!(client.read(&mut byte).expect("read at eof"), 0);
}

#[test]
fn set_get_del_exists_round_trip() {
    let addr = start_server(Arc::new(Store::new()));
    let mut client = connect(addr);

    send(&mut client, &command(&[b"SET", b"k", b"v"]));
    expect_reply(&mut client, b"+OK\r\n");

    send(&mut client, &command(&[b"GET", b"k"]));
    expect_reply(&mut client, b"$1\r\nv\r\n");

    // `EXISTS` counts duplicates; `DEL` removes once.
    send(&mut client, &command(&[b"EXISTS", b"k", b"k", b"missing"]));
    expect_reply(&mut client, b":2\r\n");

    send(&mut client, &command(&[b"DEL", b"k", b"k"]));
    expect_reply(&mut client, b":1\r\n");

    send(&mut client, &command(&[b"GET", b"k"]));
    expect_reply(&mut client, b"$-1\r\n");

    send(&mut client, &command(&[b"EXISTS", b"k"]));
    expect_reply(&mut client, b":0\r\n");
}

/// An empty value is a hit, not a miss.
#[test]
fn empty_value_is_not_a_miss() {
    let addr = start_server(Arc::new(Store::new()));
    let mut client = connect(addr);

    send(&mut client, &command(&[b"SET", b"k", b""]));
    expect_reply(&mut client, b"+OK\r\n");

    send(&mut client, &command(&[b"GET", b"k"]));
    expect_reply(&mut client, b"$0\r\n\r\n");

    send(&mut client, &command(&[b"EXISTS", b"k"]));
    expect_reply(&mut client, b":1\r\n");
}

#[test]
fn binary_keys_and_values_round_trip() {
    let addr = start_server(Arc::new(Store::new()));
    let mut client = connect(addr);

    let key: &[u8] = &[0x00, 0x80, 0xFF];
    let value: &[u8] = &[0xFF, b'\r', b'\n', 0x00];

    send(&mut client, &command(&[b"SET", key, value]));
    expect_reply(&mut client, b"+OK\r\n");

    send(&mut client, &command(&[b"GET", key]));
    let mut expected = b"$4\r\n".to_vec();
    expected.extend_from_slice(value);
    expected.extend_from_slice(b"\r\n");
    expect_reply(&mut client, &expected);
}

/// Every request in one write is answered, in order, from one read pass.
#[test]
fn pipelined_requests_reply_in_order() {
    let addr = start_server(Arc::new(Store::new()));
    let mut client = connect(addr);

    let mut batch = command(&[b"SET", b"a", b"1"]);
    batch.extend(command(&[b"GET", b"a"]));
    batch.extend(command(&[b"DEL", b"a"]));
    send(&mut client, &batch);

    expect_reply(&mut client, b"+OK\r\n$1\r\n1\r\n:1\r\n");
}

/// A request arriving in pieces still parses as one frame.
#[test]
fn request_split_across_writes_still_parses() {
    let addr = start_server(Arc::new(Store::new()));
    let mut client = connect(addr);

    send(&mut client, b"*3\r\n$3\r\nSET\r\n$5\r\nsp");
    send(&mut client, b"lit\r\n$5\r\nva");
    send(&mut client, b"lue\r\n");
    expect_reply(&mut client, b"+OK\r\n");

    send(&mut client, &command(&[b"GET", b"split"]));
    expect_reply(&mut client, b"$5\r\nvalue\r\n");
}

#[test]
fn clients_share_one_store() {
    let store = Arc::new(Store::new());
    let addr = start_server(Arc::clone(&store));
    let mut writer = connect(addr);
    let mut reader = connect(addr);

    send(
        &mut writer,
        &command(&[b"SET", b"shared", b"across clients"]),
    );
    expect_reply(&mut writer, b"+OK\r\n");

    send(&mut reader, &command(&[b"GET", b"shared"]));
    expect_reply(&mut reader, b"$14\r\nacross clients\r\n");

    // The very store the threads use is visible to the test.
    assert_eq!(
        store.get(b"shared").as_deref(),
        Some(b"across clients".as_slice())
    );
}

/// A value far larger than one socket read is reassembled.
#[test]
fn value_larger_than_a_read_buffer_round_trips() {
    let addr = start_server(Arc::new(Store::new()));
    let mut client = connect(addr);
    let value: Vec<u8> = (0..64 * 1024 + 11).map(|i| (i % 251) as u8).collect();

    send(&mut client, &command(&[b"SET", b"big", &value]));
    expect_reply(&mut client, b"+OK\r\n");

    send(&mut client, &command(&[b"GET", b"big"]));
    let mut expected = format!("${}\r\n", value.len()).into_bytes();
    expected.extend_from_slice(&value);
    expected.extend_from_slice(b"\r\n");
    expect_reply(&mut client, &expected);
}

#[test]
fn unknown_command_does_not_close_the_connection() {
    let addr = start_server(Arc::new(Store::new()));
    let mut client = connect(addr);

    send(&mut client, &command(&[b"FOO", b"bar"]));
    expect_reply(&mut client, b"-ERR unknown command 'FOO'\r\n");

    send(&mut client, &command(&[b"PING"]));
    expect_reply(&mut client, b"+PONG\r\n");
}

/// A command name is a bulk string, so it may contain CRLF. It must not split
/// the error into a second frame.
#[test]
fn unknown_command_escapes_control_bytes() {
    let addr = start_server(Arc::new(Store::new()));
    let mut client = connect(addr);

    // The name is the 8-byte bulk "X\r\n+Fake".
    send(&mut client, b"*1\r\n$8\r\nX\r\n+Fake\r\n");
    expect_reply(&mut client, b"-ERR unknown command 'X\\r\\n+Fake'\r\n");

    // Unknown commands keep the connection open, and framing is intact.
    send(&mut client, &command(&[b"PING"]));
    expect_reply(&mut client, b"+PONG\r\n");
}

#[test]
fn ping_echoes_its_argument() {
    let addr = start_server(Arc::new(Store::new()));
    let mut client = connect(addr);

    send(&mut client, &command(&[b"PING", b"hello"]));
    expect_reply(&mut client, b"$5\r\nhello\r\n");
}

#[test]
fn arity_and_syntax_errors() {
    let addr = start_server(Arc::new(Store::new()));
    let mut client = connect(addr);

    send(&mut client, &command(&[b"GET"]));
    expect_reply(
        &mut client,
        b"-ERR wrong number of arguments for 'get' command\r\n",
    );

    send(&mut client, &command(&[b"SET", b"k"]));
    expect_reply(
        &mut client,
        b"-ERR wrong number of arguments for 'set' command\r\n",
    );

    // Names are case-insensitive; the error echoes the lowercased name.
    send(&mut client, &command(&[b"GeT", b"a", b"b"]));
    expect_reply(
        &mut client,
        b"-ERR wrong number of arguments for 'get' command\r\n",
    );

    send(&mut client, &command(&[b"SET", b"k", b"v", b"EX", b"10"]));
    expect_reply(&mut client, b"-ERR syntax error\r\n");

    // The rejected `SET` stored nothing.
    send(&mut client, &command(&[b"GET", b"k"]));
    expect_reply(&mut client, b"$-1\r\n");
}

#[test]
fn quit_replies_then_closes() {
    let addr = start_server(Arc::new(Store::new()));
    let mut client = connect(addr);

    send(&mut client, &command(&[b"QUIT"]));
    expect_reply(&mut client, b"+OK\r\n");
    expect_eof(&mut client);
}

#[test]
fn protocol_error_replies_then_closes() {
    let addr = start_server(Arc::new(Store::new()));
    let mut client = connect(addr);

    send(&mut client, b"*1\r\n$x\r\n");
    expect_reply(&mut client, b"-ERR Protocol error: invalid bulk length\r\n");
    expect_eof(&mut client);
}

#[test]
fn inline_commands_are_rejected() {
    let addr = start_server(Arc::new(Store::new()));
    let mut client = connect(addr);

    send(&mut client, b"GET k\r\n");
    expect_reply(
        &mut client,
        b"-ERR Protocol error: expected '*', got 'G'\r\n",
    );
    expect_eof(&mut client);
}

/// `*0` carries no command: no reply, and the connection stays up.
#[test]
fn empty_array_is_ignored() {
    let addr = start_server(Arc::new(Store::new()));
    let mut client = connect(addr);

    send(&mut client, b"*0\r\n");
    send(&mut client, &command(&[b"PING"]));
    expect_reply(&mut client, b"+PONG\r\n");
}

/// Closing the write half is a clean EOF, not an error.
#[test]
fn client_half_close_ends_the_connection() {
    use std::net::Shutdown;

    let addr = start_server(Arc::new(Store::new()));
    let mut client = connect(addr);

    client.shutdown(Shutdown::Write).expect("shutdown write");
    expect_eof(&mut client);
}

/// Idle connections are closed, and the listener keeps serving after.
#[test]
fn idle_connection_is_closed() {
    let addr = start_server_with(Arc::new(Store::new()), |server| {
        server.idle_timeout(Some(Duration::from_millis(100)))
    });

    let mut first = connect(addr);
    let mut second = connect(addr);
    expect_eof(&mut first);
    expect_eof(&mut second);

    let mut fresh = connect(addr);
    send(&mut fresh, &command(&[b"PING"]));
    expect_reply(&mut fresh, b"+PONG\r\n");
}

/// Traffic re-arms the socket timeout, so it postpones the close.
#[test]
fn traffic_postpones_the_timeout() {
    let addr = start_server_with(Arc::new(Store::new()), |server| {
        server.idle_timeout(Some(Duration::from_millis(200)))
    });
    let mut client = connect(addr);

    // Each gap is half the timeout and the total outlives it, so a connection
    // closed on total age would die before the later `+PONG`.
    for _ in 0..4 {
        thread::sleep(Duration::from_millis(100));
        send(&mut client, &command(&[b"PING"]));
        expect_reply(&mut client, b"+PONG\r\n");
    }
}

/// Traffic does not make a connection immortal.
#[test]
fn idle_after_traffic_is_closed() {
    let addr = start_server_with(Arc::new(Store::new()), |server| {
        server.idle_timeout(Some(Duration::from_millis(100)))
    });
    let mut client = connect(addr);

    send(&mut client, &command(&[b"PING"]));
    expect_reply(&mut client, b"+PONG\r\n");
    expect_eof(&mut client);
}

/// `None` and `Duration::ZERO` both mean "never time out".
#[test]
fn disabled_timeout_keeps_connections_open() {
    for idle_timeout in [None, Some(Duration::ZERO)] {
        let addr = start_server_with(Arc::new(Store::new()), |server| {
            server.idle_timeout(idle_timeout)
        });
        let mut client = connect(addr);
        client
            .set_read_timeout(Some(Duration::from_millis(300)))
            .expect("set read timeout");

        // Nothing arrives and nothing closes, so only the client-side timeout
        // can end this read.
        let mut byte = [0u8; 1];
        let err = client
            .read(&mut byte)
            .expect_err("server closed an idle connection");
        assert!(
            matches!(err.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut),
            "unexpected error kind: {err:?}"
        );

        // The connection is still usable afterwards.
        send(&mut client, &command(&[b"PING"]));
        expect_reply(&mut client, b"+PONG\r\n");
    }
}

/// The frame a peer over the cap receives before the close.
const LIMIT_ERROR: &[u8] = b"-ERR max number of clients reached\r\n";

#[test]
fn connection_limit_rejects_extra_clients() {
    let addr = start_server_with(Arc::new(Store::new()), |server| {
        server.max_connections(Some(1))
    });
    let mut holder = connect(addr);

    // The slot is taken before the thread spawns, so a live holder proves the
    // only one is occupied.
    send(&mut holder, &command(&[b"PING"]));
    expect_reply(&mut holder, b"+PONG\r\n");

    let mut rejected = connect(addr);
    expect_reply(&mut rejected, LIMIT_ERROR);
    expect_eof(&mut rejected);

    // Rejecting the extra peer left the holder untouched.
    send(&mut holder, &command(&[b"PING"]));
    expect_reply(&mut holder, b"+PONG\r\n");
}

#[test]
fn default_max_connections_is_unlimited() {
    let addr = start_server(Arc::new(Store::new()));
    let mut first = connect(addr);
    let mut second = connect(addr);

    send(&mut first, &command(&[b"PING"]));
    expect_reply(&mut first, b"+PONG\r\n");
    send(&mut second, &command(&[b"PING"]));
    expect_reply(&mut second, b"+PONG\r\n");
}
