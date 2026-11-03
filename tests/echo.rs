use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::thread;
use std::time::Duration;

use rust_redis::Server;

/// Generous, but finite: a regression must fail, not hang `cargo test`.
const READ_TIMEOUT: Duration = Duration::from_secs(5);

fn connect(addr: SocketAddr) -> TcpStream {
    let stream = TcpStream::connect(addr).expect("connect");
    stream
        .set_read_timeout(Some(READ_TIMEOUT))
        .expect("set read timeout");
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
    let server = Server::bind("127.0.0.1:0").expect("bind");
    let addr = server.local_addr().expect("local_addr");

    // Detached on purpose: this thread is never joined and dies with the test
    // process. The listener already exists, so no sleep/retry is needed.
    thread::spawn(move || {
        let _ = server.serve();
    });

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
