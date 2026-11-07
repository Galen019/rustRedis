use std::env;
use std::io;
use std::sync::Arc;

use rust_redis::{Server, Store};

/// Loopback only: keeps the half-built server off the LAN.
const ADDR: &str = "127.0.0.1:6379";

/// Default cap on live connection threads; extra peers are rejected.
const MAX_CONNECTIONS: usize = 10_000;

/// Overrides [`MAX_CONNECTIONS`]; `0` means unlimited.
const MAX_CONNECTIONS_ENV: &str = "REDIS_MAX_CONNECTIONS";

fn main() -> io::Result<()> {
    let store = Arc::new(Store::new());
    let server = Server::bind(ADDR, store)?.max_connections(max_connections()?);
    eprintln!("listening on {}", server.local_addr()?);
    server.serve()
}

/// Reads the cap from [`MAX_CONNECTIONS_ENV`], falling back to the default.
///
/// - an unparsable value aborts startup instead of silently using the default
fn max_connections() -> io::Result<Option<usize>> {
    match env::var(MAX_CONNECTIONS_ENV) {
        Ok(raw) => parse_max_connections(&raw),
        Err(env::VarError::NotPresent) => Ok(Some(MAX_CONNECTIONS)),
        Err(env::VarError::NotUnicode(_)) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{MAX_CONNECTIONS_ENV} is not valid Unicode"),
        )),
    }
}

/// Parses [`MAX_CONNECTIONS_ENV`]'s value; `0` means unlimited.
///
/// - an unparsable value is an error, not a silent fallback
fn parse_max_connections(raw: &str) -> io::Result<Option<usize>> {
    let max: usize = raw.parse().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{MAX_CONNECTIONS_ENV} must be a non-negative integer, got '{raw}'"),
        )
    })?;
    Ok((max != 0).then_some(max))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_positive_cap() {
        assert_eq!(parse_max_connections("10000").expect("parse"), Some(10_000));
    }

    #[test]
    fn zero_means_unlimited() {
        assert_eq!(parse_max_connections("0").expect("parse"), None);
    }

    #[test]
    fn rejects_invalid_values() {
        for raw in ["", "-1", "nope", "1.5"] {
            assert!(parse_max_connections(raw).is_err(), "accepted {raw:?}");
        }
    }

    #[test]
    fn rejects_values_over_usize() {
        let raw = "9".repeat(usize::MAX.to_string().len() + 1);
        assert!(parse_max_connections(&raw).is_err());
    }
}
