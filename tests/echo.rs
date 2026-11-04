use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::thread;
use std::time::Duration;

use rust_redis::Server;

/// Generous, but finite: a regression must fail, not hang `cargo test`.
const TIMEOUT: Duration = Duration::from_secs(5);

/// Binds port 0 and serves from a detached thread.
///
/// - the listener is live before return, so no sleep or retry is needed
fn start_server() -> SocketAddr {
    let server = Server::bind("127.0.0.1:0").expect("bind");
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

fn round_trip(stream: &mut TcpStream, payload: &[u8]) -> Vec<u8> {
    stream.write_all(payload).expect("write");
    let mut echoed = vec![0u8; payload.len()];
    stream.read_exact(&mut echoed).expect("read echo");
    echoed
}

#[test]
fn echo_round_trip_and_concurrent_clients() {
    let addr = start_server();

    // 1. A connects and stays silent: a silent client is legal.
    let mut a = connect(addr);

    // 2. B echoes while A is still connected. Under a sequential accept loop
    //    B's connect() would still succeed (backlog) but this read would time
    //    out -- which is exactly the regression this test guards against.
    let mut b = connect(addr);
    assert_eq!(round_trip(&mut b, b"hello"), b"hello");

    // 3. A echoes too.
    assert_eq!(round_trip(&mut a, b"ping"), b"ping");

    // 4. Closing the write half must surface as clean EOF, not an error.
    a.shutdown(Shutdown::Write).expect("shutdown write");
    let mut eof = [0u8; 1];
    assert_eq!(a.read(&mut eof).expect("read at eof"), 0);

    // 5. A is gone; B is unaffected.
    drop(a);
    assert_eq!(round_trip(&mut b, b"still here"), b"still here");
}

/// A payload spanning many read chunks round-trips verbatim.
#[test]
fn echoes_payloads_larger_than_the_read_buffer() {
    let mut stream = connect(start_server());
    let payload: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();

    // Read from a clone while writing; a single large write can deadlock.
    let mut writer = stream.try_clone().expect("clone stream");
    let len = payload.len();
    let reader = thread::spawn(move || {
        let mut echoed = vec![0u8; len];
        stream.read_exact(&mut echoed).expect("read echo");
        echoed
    });
    writer.write_all(&payload).expect("write");
    assert_eq!(reader.join().expect("reader thread panicked"), payload);
}

/// Each write is echoed as it arrives, not after disconnect.
#[test]
fn echoes_each_write_as_it_arrives() {
    let mut stream = connect(start_server());
    for chunk in [b"one".as_slice(), b"two".as_slice()] {
        assert_eq!(round_trip(&mut stream, chunk), chunk);
    }
}
