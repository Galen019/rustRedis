//! Manual smoke test over a real socket using `redis-rs`.
//!
//! - `cargo run --example smoke` targets `127.0.0.1:6379`
//! - `cargo run --example smoke -- ADDR` targets another running server
//! - exits non-zero on the first reply that breaks the wire contract

use std::env;
use std::error::Error;
use std::fmt::Debug;
use std::io;
use std::net::SocketAddr;
use std::process::ExitCode;
use std::thread;
use std::time::Duration;

use redis::{Client, Cmd, Connection, FromRedisValue};

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
fn run() -> Result<(), Box<dyn Error>> {
    let addr = address()?;
    println!("smoke testing {addr}");

    let client = Client::open(format!("redis://{addr}"))?;
    let mut conn = client.get_connection_with_timeout(TIMEOUT)?;
    conn.set_read_timeout(Some(TIMEOUT))?;
    conn.set_write_timeout(Some(TIMEOUT))?;

    round_trip(
        &mut conn,
        redis::cmd("SET").arg(KEY).arg(VALUE),
        "OK".to_owned(),
        "SET",
    )?;
    round_trip(
        &mut conn,
        redis::cmd("GET").arg(KEY),
        Some(VALUE.to_owned()),
        "GET",
    )?;
    round_trip(&mut conn, redis::cmd("EXISTS").arg(KEY), 1i64, "EXISTS")?;
    round_trip(&mut conn, redis::cmd("DEL").arg(KEY), 1i64, "DEL")?;
    round_trip(
        &mut conn,
        redis::cmd("GET").arg(KEY),
        None::<String>,
        "GET after DEL",
    )?;

    // Expiry: TTL commands report the deadline, and a short one really elapses.
    round_trip(
        &mut conn,
        redis::cmd("TTL").arg(KEY),
        -2i64,
        "TTL after DEL",
    )?;
    round_trip(
        &mut conn,
        redis::cmd("SET").arg(KEY).arg(VALUE).arg("EX").arg(60),
        "OK".to_owned(),
        "SET with EX",
    )?;
    expect_ttl_rounds(&mut conn, 60)?;
    expect_pttl(&mut conn)?;

    round_trip(
        &mut conn,
        redis::cmd("SET").arg(KEY).arg(VALUE).arg("PX").arg(50),
        "OK".to_owned(),
        "SET with short PX",
    )?;
    thread::sleep(Duration::from_millis(200));
    round_trip(
        &mut conn,
        redis::cmd("GET").arg(KEY),
        None::<String>,
        "GET after expiry",
    )?;
    round_trip(
        &mut conn,
        redis::cmd("TTL").arg(KEY),
        -2i64,
        "TTL after expiry",
    )?;
    round_trip(
        &mut conn,
        redis::cmd("DEL").arg(KEY),
        0i64,
        "DEL after expiry",
    )?;

    Ok(())
}

/// Checks `TTL` reports a fresh 60-second deadline within rounding.
fn expect_ttl_rounds(conn: &mut Connection, expected: i64) -> Result<(), Box<dyn Error>> {
    let ttl: i64 = redis::cmd("TTL").arg(KEY).query(conn)?;
    if !(expected - 1..=expected).contains(&ttl) {
        return Err(invalid(format!("TTL: expected {expected}, got {ttl}")).into());
    }
    println!("OK: TTL -> {ttl}");
    Ok(())
}

/// Checks `PTTL` reports roughly the same deadline in milliseconds.
fn expect_pttl(conn: &mut Connection) -> Result<(), Box<dyn Error>> {
    let pttl: i64 = redis::cmd("PTTL").arg(KEY).query(conn)?;
    if !(58_000..=60_000).contains(&pttl) {
        return Err(invalid(format!("PTTL: expected 58000-60000, got {pttl}")).into());
    }
    println!("OK: PTTL -> {pttl}");
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

/// Sends one request and checks the decoded reply.
fn round_trip<T>(
    conn: &mut Connection,
    command: &Cmd,
    expected: T,
    step: &str,
) -> Result<(), Box<dyn Error>>
where
    T: FromRedisValue + PartialEq + Debug,
{
    let actual: T = command.query(conn)?;
    if actual != expected {
        return Err(invalid(format!("{step}: expected {expected:?}, got {actual:?}")).into());
    }
    println!("OK: {step} -> {actual:?}");
    Ok(())
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}
