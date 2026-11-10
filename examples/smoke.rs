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

    // Reruns start from an absent key.
    round_trip_in_range(
        &mut conn,
        redis::cmd("DEL").arg(KEY),
        0,
        1,
        "DEL before checks",
    )?;

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

    // NX must not overwrite, XX must not create.
    round_trip(
        &mut conn,
        redis::cmd("SET").arg(KEY).arg("blocked").arg("NX"),
        None::<String>,
        "SET NX on an existing key",
    )?;
    round_trip(
        &mut conn,
        redis::cmd("GET").arg(KEY),
        Some(VALUE.to_owned()),
        "GET after rejected NX",
    )?;
    round_trip(
        &mut conn,
        redis::cmd("SET").arg(KEY).arg("xx-ok").arg("XX"),
        "OK".to_owned(),
        "SET XX on an existing key",
    )?;
    round_trip(
        &mut conn,
        redis::cmd("GET").arg(KEY),
        Some("xx-ok".to_owned()),
        "GET after XX",
    )?;

    // GET replies with the old value, never OK.
    round_trip(
        &mut conn,
        redis::cmd("SET").arg(KEY).arg("get-old").arg("GET"),
        Some("xx-ok".to_owned()),
        "SET GET",
    )?;
    round_trip(
        &mut conn,
        redis::cmd("SET")
            .arg(KEY)
            .arg("blocked")
            .arg("NX")
            .arg("GET"),
        Some("get-old".to_owned()),
        "SET NX GET",
    )?;
    round_trip(
        &mut conn,
        redis::cmd("GET").arg(KEY),
        Some("get-old".to_owned()),
        "GET after rejected NX GET",
    )?;

    round_trip(
        &mut conn,
        redis::cmd("DEL").arg(KEY),
        1i64,
        "DEL before missing-key checks",
    )?;
    round_trip(
        &mut conn,
        redis::cmd("SET").arg(KEY).arg("blocked").arg("XX"),
        None::<String>,
        "SET XX on a missing key",
    )?;
    round_trip(
        &mut conn,
        redis::cmd("GET").arg(KEY),
        None::<String>,
        "GET after XX miss",
    )?;
    round_trip(
        &mut conn,
        redis::cmd("SET").arg(KEY).arg("fresh").arg("GET"),
        None::<String>,
        "SET GET on a missing key",
    )?;

    // KEEPTTL keeps the existing deadline; a plain SET clears it.
    round_trip(
        &mut conn,
        redis::cmd("SET").arg(KEY).arg("ttl").arg("PX").arg(5000),
        "OK".to_owned(),
        "SET PX",
    )?;
    round_trip(
        &mut conn,
        redis::cmd("SET").arg(KEY).arg("kept").arg("KEEPTTL"),
        "OK".to_owned(),
        "SET KEEPTTL",
    )?;
    round_trip_in_range(
        &mut conn,
        redis::cmd("PTTL").arg(KEY),
        4000,
        5000,
        "PTTL after KEEPTTL",
    )?;
    round_trip(
        &mut conn,
        redis::cmd("SET").arg(KEY).arg("plain"),
        "OK".to_owned(),
        "SET after KEEPTTL",
    )?;
    round_trip(
        &mut conn,
        redis::cmd("TTL").arg(KEY),
        -1i64,
        "TTL after plain SET",
    )?;

    round_trip(
        &mut conn,
        redis::cmd("EXPIRE").arg(KEY).arg(10),
        1i64,
        "EXPIRE",
    )?;
    round_trip(&mut conn, redis::cmd("TTL").arg(KEY), 10i64, "TTL")?;
    round_trip(&mut conn, redis::cmd("PERSIST").arg(KEY), 1i64, "PERSIST")?;
    round_trip(
        &mut conn,
        redis::cmd("TTL").arg(KEY),
        -1i64,
        "TTL after PERSIST",
    )?;

    round_trip(&mut conn, redis::cmd("DEL").arg(KEY), 1i64, "DEL")?;
    round_trip(
        &mut conn,
        redis::cmd("GET").arg(KEY),
        None::<String>,
        "GET after DEL",
    )?;

    // The only real-time check: a short TTL must elapse on the wire.
    round_trip(
        &mut conn,
        redis::cmd("SET").arg(KEY).arg(VALUE).arg("PX").arg(50),
        "OK".to_owned(),
        "SET PX 50",
    )?;
    thread::sleep(Duration::from_millis(100));
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

    // An expired key counts as absent, so NX writes.
    round_trip(
        &mut conn,
        redis::cmd("SET").arg(KEY).arg("after-expiry").arg("NX"),
        "OK".to_owned(),
        "SET NX after expiry",
    )?;
    round_trip(
        &mut conn,
        redis::cmd("DEL").arg(KEY),
        1i64,
        "DEL at the end",
    )?;

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

/// Sends one request and checks an integer reply falls within `min..=max`.
fn round_trip_in_range(
    conn: &mut Connection,
    command: &Cmd,
    min: i64,
    max: i64,
    step: &str,
) -> Result<(), Box<dyn Error>> {
    let actual: i64 = command.query(conn)?;
    if !(min..=max).contains(&actual) {
        return Err(invalid(format!("{step}: expected {min}..={max}, got {actual}")).into());
    }
    println!("OK: {step} -> {actual}");
    Ok(())
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}
