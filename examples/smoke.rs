//! Manual smoke test over a real socket.
//!
//! - `cargo run --example smoke` targets `127.0.0.1:6379`
//! - `cargo run --example smoke -- ADDR` targets another running server
//! - exits non-zero on the first reply that breaks the wire contract

use std::env;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::process::ExitCode;
use std::time::Duration;

/// The address `src/main.rs` binds by default.
const DEFAULT_ADDR: &str = "127.0.0.1:6379";

/// A stuck server fails the smoke test instead of hanging it.
const TIMEOUT: Duration = Duration::from_secs(5);

/// Key the checks write and remove.
const KEY: &str = "smoke";

/// Value the checks write and read back.
const VALUE: &str = "hello from rust";

fn main() -> ExitCode {
    match run() {
        Ok(()) => {
            println!("smoke test passed");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("smoke test failed: {error}");
            ExitCode::FAILURE
        }
    }
}

/// Connects to the requested address.
fn run() -> io::Result<()> {
    let addr = address()?;
    println!("smoke testing {addr}");

    let stream = TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(TIMEOUT))?;
    stream.set_write_timeout(Some(TIMEOUT))?;
    let mut client = BufReader::new(stream);

    round_trip(&mut client, &["SET", KEY, VALUE], Some("+OK"), "SET")?;
    round_trip(&mut client, &["GET", KEY], Some(VALUE), "GET")?;
    round_trip(&mut client, &["EXISTS", KEY], Some(":1"), "EXISTS")?;
    round_trip(&mut client, &["DEL", KEY], Some(":1"), "DEL")?;
    round_trip(&mut client, &["GET", KEY], None, "GET after DEL")?;

    Ok(())
}

/// Parses an optional address argument, defaulting to [`DEFAULT_ADDR`].
fn address() -> io::Result<SocketAddr> {
    let raw = match env::args_os().nth(1) {
        Some(raw) => raw
            .into_string()
            .map_err(|_| invalid("address is not valid Unicode"))?,
        None => DEFAULT_ADDR.to_owned(),
    };
    raw.parse()
        .map_err(|_| invalid(format!("invalid address '{raw}'")))
}

/// Sends one request and checks the next reply.
fn round_trip(
    client: &mut BufReader<TcpStream>,
    parts: &[&str],
    expected: Option<&str>,
    step: &str,
) -> io::Result<()> {
    send(client, parts)?;
    expect(client, expected, step)
}

/// Writes `parts` as an array of bulk strings.
fn send(client: &mut BufReader<TcpStream>, parts: &[&str]) -> io::Result<()> {
    let mut request = format!("*{}\r\n", parts.len()).into_bytes();
    for part in parts {
        request.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        request.extend_from_slice(part.as_bytes());
        request.extend_from_slice(b"\r\n");
    }
    client.get_mut().write_all(&request)
}

/// Compares the next reply with `expected` and prints the outcome.
fn expect(client: &mut BufReader<TcpStream>, expected: Option<&str>, step: &str) -> io::Result<()> {
    let actual = read_reply(client)?;
    if actual.as_deref() != expected {
        return Err(invalid(format!(
            "{step}: expected {}, got {}",
            show(expected),
            show(actual.as_deref())
        )));
    }
    println!("OK: {step} -> {}", show(actual.as_deref()));
    Ok(())
}

/// Reads one RESP2 reply, decoding a bulk payload to its text.
///
/// - `None` is a bulk miss (`$-1`)
/// - anything else keeps its wire marker, e.g. `+OK` or `:1`
fn read_reply(client: &mut BufReader<TcpStream>) -> io::Result<Option<String>> {
    let line = read_line(client)?;
    let Some(length) = line.strip_prefix('$') else {
        return Ok(Some(line));
    };
    let length: i64 = length
        .parse()
        .map_err(|_| invalid(format!("invalid bulk length '{line}'")))?;
    if length < 0 {
        return Ok(None);
    }
    let length =
        usize::try_from(length).map_err(|_| invalid(format!("bulk length too large '{line}'")))?;

    let mut payload = vec![0u8; length + 2];
    client.read_exact(&mut payload)?;
    if !payload.ends_with(b"\r\n") {
        return Err(invalid("bulk payload is missing its CRLF"));
    }
    payload.truncate(length);
    String::from_utf8(payload)
        .map(Some)
        .map_err(|_| invalid("bulk payload is not UTF-8"))
}

/// Reads one CRLF-terminated line without its terminator.
fn read_line(client: &mut BufReader<TcpStream>) -> io::Result<String> {
    let mut line = Vec::new();
    client.read_until(b'\n', &mut line)?;
    if line.pop() != Some(b'\n') {
        return Err(invalid("server closed before a reply line completed"));
    }
    if line.last() == Some(&b'\r') {
        line.pop();
    }
    String::from_utf8(line).map_err(|_| invalid("reply line is not UTF-8"))
}

/// Renders a reply for the log, using `(nil)` for a miss.
fn show(reply: Option<&str>) -> String {
    match reply {
        Some(text) => format!("'{text}'"),
        None => "(nil)".to_owned(),
    }
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}
