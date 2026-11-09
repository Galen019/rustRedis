//! Command dispatch over the store.
//!
//! - socket-free: name matching, arity checks, store access only
//! - expiry is relative only: `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`, and `SET ... EX/PX`
//! - the caller passes one `now` per request to drive lazy eviction
//! - other `SET` options are syntax errors

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::resp::{Reply, escaped};
use crate::store::{Store, Ttl};

/// A reply plus whether the connection closes after sending it.
pub struct Executed {
    pub reply: Reply,
    pub close: bool,
}

impl Executed {
    fn reply(reply: Reply) -> Self {
        Self {
            reply,
            close: false,
        }
    }

    fn close(reply: Reply) -> Self {
        Self { reply, close: true }
    }
}

/// Executes one request, or `None` when there is nothing to reply with (`*0`).
pub fn execute(store: &Store, args: &[Vec<u8>], now: Instant) -> Option<Executed> {
    let (name, rest) = args.split_first()?;

    let executed = if name.eq_ignore_ascii_case(b"get") {
        Executed::reply(get(store, rest, now))
    } else if name.eq_ignore_ascii_case(b"set") {
        Executed::reply(set(store, rest, now))
    } else if name.eq_ignore_ascii_case(b"del") {
        Executed::reply(del(store, rest, now))
    } else if name.eq_ignore_ascii_case(b"exists") {
        Executed::reply(exists(store, rest, now))
    } else if name.eq_ignore_ascii_case(b"expire") {
        Executed::reply(expire(store, rest, now, TimeUnit::Seconds))
    } else if name.eq_ignore_ascii_case(b"pexpire") {
        Executed::reply(expire(store, rest, now, TimeUnit::Millis))
    } else if name.eq_ignore_ascii_case(b"ttl") {
        Executed::reply(ttl(store, rest, now, TimeUnit::Seconds))
    } else if name.eq_ignore_ascii_case(b"pttl") {
        Executed::reply(ttl(store, rest, now, TimeUnit::Millis))
    } else if name.eq_ignore_ascii_case(b"persist") {
        Executed::reply(persist(store, rest, now))
    } else if name.eq_ignore_ascii_case(b"ping") {
        Executed::reply(ping(rest))
    } else if name.eq_ignore_ascii_case(b"quit") {
        // Redis declares `QUIT` with arity -1: extra arguments are ignored.
        Executed::close(Reply::Simple("OK"))
    } else {
        Executed::reply(Reply::Error(format!(
            "ERR unknown command '{}'",
            escaped(name)
        )))
    };
    Some(executed)
}

fn get(store: &Store, args: &[Vec<u8>], now: Instant) -> Reply {
    match args {
        [key] => Reply::Bulk(store.get(key, now)),
        _ => arity_error("get"),
    }
}

fn set(store: &Store, args: &[Vec<u8>], now: Instant) -> Reply {
    let [key, value, options @ ..] = args else {
        return arity_error("set");
    };

    let Some(options) = parse_set_options(options) else {
        return Reply::Error("ERR syntax error".into());
    };

    let deadline = match options {
        SetOptions::NoExpiry => None,
        SetOptions::Expiry { unit, raw } => match expire_deadline(unit, raw, now) {
            Ok(deadline) => Some(deadline),
            Err(reply) => return reply,
        },
    };

    store.set(key, value, deadline);
    Reply::Simple("OK")
}

fn del(store: &Store, args: &[Vec<u8>], now: Instant) -> Reply {
    if args.is_empty() {
        return arity_error("del");
    }
    Reply::Integer(store.del_many(args.iter().map(Vec::as_slice), now) as i64)
}

fn exists(store: &Store, args: &[Vec<u8>], now: Instant) -> Reply {
    if args.is_empty() {
        return arity_error("exists");
    }
    Reply::Integer(store.exists_many(args.iter().map(Vec::as_slice), now) as i64)
}

fn expire(store: &Store, args: &[Vec<u8>], now: Instant, unit: TimeUnit) -> Reply {
    let name = match unit {
        TimeUnit::Seconds => "expire",
        TimeUnit::Millis => "pexpire",
    };
    let [key, raw] = args else {
        return arity_error(name);
    };

    let Some(value) = parse_i64(raw) else {
        return integer_error();
    };

    // Redis rejects seconds that overflow when scaled to milliseconds.
    if unit == TimeUnit::Seconds && !(i64::MIN / 1000..=i64::MAX / 1000).contains(&value) {
        return invalid_expire_time(name);
    }

    // A non-positive TTL deletes the key, so the exact past instant does not matter.
    let deadline = if value <= 0 {
        now
    } else {
        let Some(duration) = expiry_duration(unit, value) else {
            return invalid_expire_time(name);
        };
        match now.checked_add(duration) {
            Some(deadline) => deadline,
            None => return invalid_expire_time(name),
        }
    };

    Reply::Integer(store.set_expiry(key, deadline, now) as i64)
}

fn ttl(store: &Store, args: &[Vec<u8>], now: Instant, unit: TimeUnit) -> Reply {
    let name = match unit {
        TimeUnit::Seconds => "ttl",
        TimeUnit::Millis => "pttl",
    };
    let [key] = args else {
        return arity_error(name);
    };

    let value = match store.ttl(key, now) {
        Ttl::Missing => -2,
        Ttl::Persistent => -1,
        Ttl::Expiring(remaining) => {
            let remaining = remaining.as_millis().min(i64::MAX as u128) as i64;
            if unit == TimeUnit::Millis {
                remaining
            } else {
                remaining.saturating_add(500) / 1000
            }
        }
    };
    Reply::Integer(value)
}

fn persist(store: &Store, args: &[Vec<u8>], now: Instant) -> Reply {
    match args {
        [key] => Reply::Integer(store.persist(key, now) as i64),
        _ => arity_error("persist"),
    }
}

fn ping(args: &[Vec<u8>]) -> Reply {
    match args {
        [] => Reply::Simple("PONG"),
        [message] => Reply::Bulk(Some(Arc::from(message.as_slice()))),
        _ => arity_error("ping"),
    }
}

/// Expiry unit for `EXPIRE`/`PEXPIRE`, `TTL`/`PTTL`, and `SET ... EX/PX`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum TimeUnit {
    Seconds,
    Millis,
}

/// Parsed `SET` options.
enum SetOptions<'a> {
    NoExpiry,
    Expiry { unit: TimeUnit, raw: &'a [u8] },
}

/// Parses `SET` options.
///
/// - `None` is a syntax error
/// - duplicate `EX`/`PX` is allowed; the last one wins
/// - mixing `EX` and `PX` is a syntax error
fn parse_set_options(options: &[Vec<u8>]) -> Option<SetOptions<'_>> {
    let mut expiry: Option<(TimeUnit, &[u8])> = None;
    let mut index = 0;

    while index < options.len() {
        let option = &options[index];
        let unit = if option.eq_ignore_ascii_case(b"ex") {
            TimeUnit::Seconds
        } else if option.eq_ignore_ascii_case(b"px") {
            TimeUnit::Millis
        } else {
            return None;
        };

        if expiry.is_some_and(|(seen, _)| seen != unit) {
            return None;
        }
        let raw = options.get(index + 1).map(Vec::as_slice)?;
        expiry = Some((unit, raw));
        index += 2;
    }

    Some(match expiry {
        None => SetOptions::NoExpiry,
        Some((unit, raw)) => SetOptions::Expiry { unit, raw },
    })
}

/// Converts a positive expiry value to a duration.
///
/// - `None` for a non-positive value or seconds that overflow milliseconds
fn expiry_duration(unit: TimeUnit, value: i64) -> Option<Duration> {
    if value <= 0 {
        return None;
    }
    match unit {
        TimeUnit::Seconds => (value <= i64::MAX / 1000).then(|| Duration::from_secs(value as u64)),
        TimeUnit::Millis => Some(Duration::from_millis(value as u64)),
    }
}

/// Validates a `SET` expiry value and turns it into a deadline.
fn expire_deadline(unit: TimeUnit, raw: &[u8], now: Instant) -> Result<Instant, Reply> {
    let Some(value) = parse_i64(raw) else {
        return Err(integer_error());
    };
    let duration = expiry_duration(unit, value).ok_or_else(|| invalid_expire_time("set"))?;
    now.checked_add(duration)
        .ok_or_else(|| invalid_expire_time("set"))
}

/// Parses an ASCII integer the way Redis's `string2ll` does.
///
/// - optional leading `-`, no `+`, whitespace, or leading zeros
/// - rejects `-0` and values that do not fit `i64`
fn parse_i64(bytes: &[u8]) -> Option<i64> {
    let (negative, digits) = match bytes.split_first() {
        Some((&b'-', digits)) => (true, digits),
        Some(_) => (false, bytes),
        None => return None,
    };

    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    if digits == b"0" {
        return (!negative).then_some(0);
    }
    if !(b'1'..=b'9').contains(&digits[0]) {
        return None;
    }

    std::str::from_utf8(bytes).ok()?.parse::<i64>().ok()
}

fn integer_error() -> Reply {
    Reply::Error("ERR value is not an integer or out of range".into())
}

fn invalid_expire_time(name: &str) -> Reply {
    Reply::Error(format!("ERR invalid expire time in '{name}' command"))
}

fn arity_error(name: &str) -> Reply {
    Reply::Error(format!(
        "ERR wrong number of arguments for '{name}' command"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dispatch(store: &Store, parts: &[&[u8]], now: Instant) -> Executed {
        let args: Vec<Vec<u8>> = parts.iter().map(|part| part.to_vec()).collect();
        execute(store, &args, now).expect("a reply")
    }

    fn run(store: &Store, parts: &[&[u8]]) -> Reply {
        dispatch(store, parts, Instant::now()).reply
    }

    fn run_at(store: &Store, parts: &[&[u8]], now: Instant) -> Reply {
        dispatch(store, parts, now).reply
    }

    fn bulk(bytes: &[u8]) -> Reply {
        Reply::Bulk(Some(Arc::from(bytes)))
    }

    fn at(now: Instant, seconds: u64) -> Instant {
        now + Duration::from_secs(seconds)
    }

    #[test]
    fn crud_lifecycle_through_dispatch() {
        let store = Store::new();

        assert_eq!(run(&store, &[b"GET", b"k"]), Reply::Bulk(None));
        assert_eq!(run(&store, &[b"SET", b"k", b"v"]), Reply::Simple("OK"));
        assert_eq!(run(&store, &[b"GET", b"k"]), bulk(b"v"));
        assert_eq!(run(&store, &[b"EXISTS", b"k"]), Reply::Integer(1));
        assert_eq!(run(&store, &[b"DEL", b"k"]), Reply::Integer(1));
        assert_eq!(run(&store, &[b"DEL", b"k"]), Reply::Integer(0));
        assert_eq!(run(&store, &[b"GET", b"k"]), Reply::Bulk(None));
    }

    #[test]
    fn multi_key_exists_counts_duplicates_and_del_removes_once() {
        let store = Store::new();
        store.set(b"a", b"1", None);

        assert_eq!(
            run(&store, &[b"EXISTS", b"a", b"a", b"missing"]),
            Reply::Integer(2)
        );
        assert_eq!(run(&store, &[b"DEL", b"a", b"a"]), Reply::Integer(1));
        assert_eq!(run(&store, &[b"EXISTS", b"a"]), Reply::Integer(0));
    }

    #[test]
    fn empty_value_is_not_a_miss() {
        let store = Store::new();
        assert_eq!(run(&store, &[b"SET", b"k", b""]), Reply::Simple("OK"));
        assert_eq!(run(&store, &[b"GET", b"k"]), bulk(b""));
        assert_eq!(run(&store, &[b"GET", b"other"]), Reply::Bulk(None));
    }

    #[test]
    fn command_names_are_case_insensitive() {
        let store = Store::new();
        assert_eq!(run(&store, &[b"SeT", b"k", b"v"]), Reply::Simple("OK"));
        assert_eq!(run(&store, &[b"gEt", b"k"]), bulk(b"v"));
        assert_eq!(run(&store, &[b"Ping"]), Reply::Simple("PONG"));
    }

    #[test]
    fn set_rejects_arity_errors_and_unknown_options() {
        let store = Store::new();
        assert_eq!(
            run(&store, &[b"SET", b"k"]),
            Reply::Error("ERR wrong number of arguments for 'set' command".into())
        );
        assert_eq!(
            run(&store, &[b"SET", b"k", b"v", b"KEEPTTL"]),
            Reply::Error("ERR syntax error".into())
        );
        // The rejected SET stored nothing.
        assert_eq!(run(&store, &[b"GET", b"k"]), Reply::Bulk(None));

        // `EX` is now a supported option.
        assert_eq!(
            run(&store, &[b"SET", b"k", b"v", b"EX", b"10"]),
            Reply::Simple("OK")
        );
    }

    #[test]
    fn set_ex_and_px_install_deadlines() {
        let store = Store::new();
        let now = Instant::now();

        assert_eq!(
            run_at(&store, &[b"SET", b"k", b"v", b"EX", b"10"], now),
            Reply::Simple("OK")
        );
        assert_eq!(run_at(&store, &[b"TTL", b"k"], now), Reply::Integer(10));
        assert_eq!(
            run_at(&store, &[b"TTL", b"k"], at(now, 9)),
            Reply::Integer(1)
        );
        assert_eq!(
            run_at(&store, &[b"TTL", b"k"], at(now, 10)),
            Reply::Integer(-2)
        );
        assert_eq!(
            run_at(&store, &[b"GET", b"k"], at(now, 10)),
            Reply::Bulk(None)
        );

        assert_eq!(
            run_at(&store, &[b"SET", b"m", b"v", b"PX", b"1500"], now),
            Reply::Simple("OK")
        );
        assert_eq!(run_at(&store, &[b"PTTL", b"m"], now), Reply::Integer(1500));
    }

    #[test]
    fn ttl_rounds_like_redis() {
        let store = Store::new();
        let now = Instant::now();

        for (millis, ttl) in [(1499, 1), (1500, 2), (499, 0)] {
            let raw = millis.to_string();
            run_at(&store, &[b"SET", b"k", b"v", b"PX", raw.as_bytes()], now);
            assert_eq!(
                run_at(&store, &[b"TTL", b"k"], now),
                Reply::Integer(ttl),
                "TTL for {millis}ms"
            );
        }
    }

    #[test]
    fn plain_set_clears_a_deadline() {
        let store = Store::new();
        let now = Instant::now();

        run_at(&store, &[b"SET", b"k", b"v", b"EX", b"10"], now);
        run_at(&store, &[b"SET", b"k", b"other"], now);
        assert_eq!(run_at(&store, &[b"TTL", b"k"], now), Reply::Integer(-1));
    }

    #[test]
    fn set_option_grammar_matches_redis() {
        let store = Store::new();
        let now = Instant::now();
        let syntax = Reply::Error("ERR syntax error".into());

        // Duplicate same-unit options: last one wins.
        run_at(
            &store,
            &[b"SET", b"k", b"v", b"EX", b"10", b"EX", b"20"],
            now,
        );
        assert_eq!(run_at(&store, &[b"TTL", b"k"], now), Reply::Integer(20));
        run_at(
            &store,
            &[b"SET", b"k", b"v", b"PX", b"10", b"PX", b"20"],
            now,
        );
        assert_eq!(run_at(&store, &[b"PTTL", b"k"], now), Reply::Integer(20));

        // Only the last same-unit option is validated, as in Redis.
        run_at(
            &store,
            &[b"SET", b"k", b"v", b"EX", b"abc", b"EX", b"10"],
            now,
        );
        assert_eq!(run_at(&store, &[b"TTL", b"k"], now), Reply::Integer(10));

        for parts in [
            &[b"SET".as_slice(), b"k", b"v", b"EX", b"10", b"PX", b"1000"][..],
            &[b"SET".as_slice(), b"k", b"v", b"PX", b"10", b"EX", b"1"][..],
            &[b"SET".as_slice(), b"k", b"v", b"KEEPTTL"][..],
            &[b"SET".as_slice(), b"k", b"v", b"EX"][..],
        ] {
            assert_eq!(run_at(&store, parts, now), syntax, "{parts:?}");
        }

        assert_eq!(
            run_at(&store, &[b"SET", b"k", b"v", b"EX", b"abc"], now),
            Reply::Error("ERR value is not an integer or out of range".into())
        );
        for raw in [b"0".as_slice(), b"-1", b"9999999999999999"] {
            assert_eq!(
                run_at(&store, &[b"SET", b"k", b"v", b"EX", raw], now),
                Reply::Error("ERR invalid expire time in 'set' command".into()),
                "EX {}",
                String::from_utf8_lossy(raw)
            );
        }

        // Syntax is checked across the whole option list before the number.
        assert_eq!(
            run_at(&store, &[b"SET", b"k", b"v", b"EX", b"0", b"foo"], now),
            syntax
        );
        assert_eq!(
            run_at(&store, &[b"SET", b"k", b"v", b"EX", b"abc", b"foo"], now),
            syntax
        );
        assert_eq!(
            run_at(&store, &[b"SET", b"k", b"v", b"EX", b"PX", b"10"], now),
            syntax
        );
    }

    #[test]
    fn failed_set_leaves_key_and_ttl_untouched() {
        let store = Store::new();
        let now = Instant::now();

        run_at(&store, &[b"SET", b"k", b"old", b"EX", b"10"], now);
        assert_eq!(
            run_at(&store, &[b"SET", b"k", b"new", b"EX", b"0"], now),
            Reply::Error("ERR invalid expire time in 'set' command".into())
        );
        assert_eq!(run_at(&store, &[b"GET", b"k"], now), bulk(b"old"));
        assert_eq!(run_at(&store, &[b"TTL", b"k"], now), Reply::Integer(10));
    }

    #[test]
    fn expire_sets_replaces_and_deletes() {
        let store = Store::new();
        let now = Instant::now();

        run_at(&store, &[b"SET", b"k", b"v"], now);
        assert_eq!(
            run_at(&store, &[b"EXPIRE", b"k", b"10"], now),
            Reply::Integer(1)
        );
        assert_eq!(run_at(&store, &[b"TTL", b"k"], now), Reply::Integer(10));
        assert_eq!(
            run_at(&store, &[b"EXPIRE", b"k", b"100"], now),
            Reply::Integer(1)
        );
        assert_eq!(run_at(&store, &[b"TTL", b"k"], now), Reply::Integer(100));
        assert_eq!(
            run_at(&store, &[b"EXPIRE", b"missing", b"10"], now),
            Reply::Integer(0)
        );

        assert_eq!(
            run_at(&store, &[b"PEXPIRE", b"k", b"1500"], now),
            Reply::Integer(1)
        );
        assert_eq!(run_at(&store, &[b"PTTL", b"k"], now), Reply::Integer(1500));

        for raw in [b"0".as_slice(), b"-1"] {
            run_at(&store, &[b"SET", b"k", b"v"], now);
            assert_eq!(
                run_at(&store, &[b"EXPIRE", b"k", raw], now),
                Reply::Integer(1)
            );
            assert_eq!(run_at(&store, &[b"GET", b"k"], now), Reply::Bulk(None));
        }
    }

    #[test]
    fn expire_errors_match_redis() {
        let store = Store::new();
        let now = Instant::now();

        for parts in [
            &[b"EXPIRE".as_slice(), b"k", b"abc"][..],
            &[b"EXPIRE".as_slice(), b"missing", b"abc"][..],
            &[b"PEXPIRE".as_slice(), b"k", b"abc"][..],
        ] {
            assert_eq!(
                run_at(&store, parts, now),
                Reply::Error("ERR value is not an integer or out of range".into()),
                "{parts:?}"
            );
        }

        assert_eq!(
            run_at(&store, &[b"EXPIRE", b"k", b"9999999999999999"], now),
            Reply::Error("ERR invalid expire time in 'expire' command".into())
        );
        assert_eq!(
            run_at(&store, &[b"EXPIRE", b"k", b"-9223372036854775808"], now),
            Reply::Error("ERR invalid expire time in 'expire' command".into())
        );
        assert_eq!(
            run_at(&store, &[b"EXPIRE", b"k", b"10", b"extra"], now),
            Reply::Error("ERR wrong number of arguments for 'expire' command".into())
        );
        assert_eq!(
            run_at(&store, &[b"PEXPIRE", b"k"], now),
            Reply::Error("ERR wrong number of arguments for 'pexpire' command".into())
        );
        assert_eq!(
            run_at(&store, &[b"EXPIRE", b"k"], now),
            Reply::Error("ERR wrong number of arguments for 'expire' command".into())
        );
    }

    #[test]
    fn persist_removes_a_deadline() {
        let store = Store::new();
        let now = Instant::now();

        run_at(&store, &[b"SET", b"k", b"v", b"EX", b"10"], now);
        assert_eq!(run_at(&store, &[b"PERSIST", b"k"], now), Reply::Integer(1));
        assert_eq!(run_at(&store, &[b"TTL", b"k"], now), Reply::Integer(-1));
        assert_eq!(run_at(&store, &[b"PERSIST", b"k"], now), Reply::Integer(0));
        assert_eq!(
            run_at(&store, &[b"PERSIST", b"missing"], now),
            Reply::Integer(0)
        );
        assert_eq!(
            run_at(&store, &[b"PERSIST"], now),
            Reply::Error("ERR wrong number of arguments for 'persist' command".into())
        );
    }

    #[test]
    fn expired_keys_are_absent_everywhere() {
        let now = Instant::now();
        let after = now + Duration::from_millis(100);

        for command in [
            &[b"GET".as_slice(), b"k"][..],
            &[b"EXISTS".as_slice(), b"k", b"k"][..],
            &[b"DEL".as_slice(), b"k", b"k"][..],
            &[b"TTL".as_slice(), b"k"][..],
            &[b"PTTL".as_slice(), b"k"][..],
            &[b"PERSIST".as_slice(), b"k"][..],
        ] {
            let store = Store::new();
            run_at(&store, &[b"SET", b"k", b"v", b"PX", b"100"], now);
            let expected = match command[0] {
                b"GET" => Reply::Bulk(None),
                b"EXISTS" | b"PERSIST" => Reply::Integer(0),
                b"DEL" => Reply::Integer(0),
                b"TTL" | b"PTTL" => Reply::Integer(-2),
                _ => unreachable!(),
            };
            assert_eq!(run_at(&store, command, after), expected, "{command:?}");
        }

        let store = Store::new();
        run_at(&store, &[b"SET", b"k", b"v", b"PX", b"100"], now);
        assert_eq!(
            run_at(&store, &[b"EXPIRE", b"k", b"10"], after),
            Reply::Integer(0)
        );
    }

    #[test]
    fn get_requires_exactly_one_key() {
        let store = Store::new();
        assert_eq!(
            run(&store, &[b"GET"]),
            Reply::Error("ERR wrong number of arguments for 'get' command".into())
        );
        assert_eq!(
            run(&store, &[b"GeT", b"a", b"b"]),
            Reply::Error("ERR wrong number of arguments for 'get' command".into())
        );
    }

    #[test]
    fn del_and_exists_require_a_key() {
        let store = Store::new();
        assert_eq!(
            run(&store, &[b"DEL"]),
            Reply::Error("ERR wrong number of arguments for 'del' command".into())
        );
        assert_eq!(
            run(&store, &[b"EXISTS"]),
            Reply::Error("ERR wrong number of arguments for 'exists' command".into())
        );
    }

    #[test]
    fn ttl_commands_require_exactly_one_key() {
        let store = Store::new();
        for (command, name) in [(b"TTL".as_slice(), "ttl"), (b"PTTL", "pttl")] {
            assert_eq!(
                run(&store, &[command]),
                Reply::Error(format!(
                    "ERR wrong number of arguments for '{name}' command"
                ))
            );
            assert_eq!(
                run(&store, &[command, b"k", b"extra"]),
                Reply::Error(format!(
                    "ERR wrong number of arguments for '{name}' command"
                ))
            );
        }
    }

    #[test]
    fn ping_echoes_one_argument_only() {
        assert_eq!(run(&Store::new(), &[b"PING"]), Reply::Simple("PONG"));
        assert_eq!(run(&Store::new(), &[b"PING", b"hi"]), bulk(b"hi"));
        assert_eq!(
            run(&Store::new(), &[b"PING", b"a", b"b"]),
            Reply::Error("ERR wrong number of arguments for 'ping' command".into())
        );
    }

    #[test]
    fn quit_replies_ok_and_closes_while_other_commands_stay_open() {
        let store = Store::new();

        let quit = dispatch(&store, &[b"QUIT"], Instant::now());
        assert_eq!(quit.reply, Reply::Simple("OK"));
        assert!(quit.close);

        let ping = dispatch(&store, &[b"PING"], Instant::now());
        assert_eq!(ping.reply, Reply::Simple("PONG"));
        assert!(!ping.close);
    }

    #[test]
    fn unknown_command_echoes_its_name() {
        let store = Store::new();
        assert_eq!(
            run(&store, &[b"FOO", b"bar"]),
            Reply::Error("ERR unknown command 'FOO'".into())
        );
        assert!(
            execute(&store, &[], Instant::now()).is_none(),
            "an empty request has no reply"
        );
    }

    #[test]
    fn unknown_command_escapes_control_bytes() {
        let store = Store::new();
        assert_eq!(
            run(&store, &[b"X\r\n+Fake"]),
            Reply::Error("ERR unknown command 'X\\r\\n+Fake'".into())
        );
    }

    #[test]
    fn integer_parsing_matches_string2ll() {
        assert_eq!(parse_i64(b"0"), Some(0));
        assert_eq!(parse_i64(b"10"), Some(10));
        assert_eq!(parse_i64(b"-10"), Some(-10));
        assert_eq!(parse_i64(b"9223372036854775807"), Some(i64::MAX));

        for raw in [
            b"".as_slice(),
            b"+1",
            b"01",
            b"-0",
            b"00",
            b" 1",
            b"1 ",
            b"1.0",
            b"9223372036854775808",
        ] {
            assert_eq!(parse_i64(raw), None, "{raw:?}");
        }
    }
}
