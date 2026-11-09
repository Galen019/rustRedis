//! Command dispatch over the store.
//!
//! - socket-free: name matching, arity checks, `SET` option parsing, store access
//! - `SET` takes an optional `EX <seconds>` or `PX <milliseconds>`
//! - no value types or transactions

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
    } else if name.eq_ignore_ascii_case(b"ttl") {
        Executed::reply(ttl(store, rest, now))
    } else if name.eq_ignore_ascii_case(b"pttl") {
        Executed::reply(pttl(store, rest, now))
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
    match args {
        [key, value] => {
            store.set(key, value, None);
            Reply::Simple("OK")
        }
        [key, value, option, amount] => match parse_expiry(option, amount, now) {
            Ok(expires_at) => {
                store.set(key, value, Some(expires_at));
                Reply::Simple("OK")
            }
            Err(reply) => reply,
        },
        [_, _, _, ..] => syntax_error(),
        _ => arity_error("set"),
    }
}

/// Resolves one `EX`/`PX` option into an absolute deadline.
fn parse_expiry(option: &[u8], amount: &[u8], now: Instant) -> Result<Instant, Reply> {
    let duration = if option.eq_ignore_ascii_case(b"ex") {
        let seconds = parse_positive(amount)?;
        // Redis: reject seconds overflowing the i64 millisecond form.
        if seconds > i64::MAX as u64 / 1000 {
            return Err(invalid_expire_error());
        }
        Duration::from_secs(seconds)
    } else if option.eq_ignore_ascii_case(b"px") {
        Duration::from_millis(parse_positive(amount)?)
    } else {
        return Err(syntax_error());
    };

    now.checked_add(duration).ok_or_else(invalid_expire_error)
}

/// Parses a positive integer, matching Redis's `string2ll` digit rules.
fn parse_positive(amount: &[u8]) -> Result<u64, Reply> {
    let text = std::str::from_utf8(amount).map_err(|_| integer_error())?;
    let (negative, digits) = match text.strip_prefix('-') {
        Some(digits) => (true, digits),
        None => (false, text),
    };
    let valid = match digits.as_bytes().first() {
        // Only the bare `"0"` is accepted; `"-0"` and leading zeros are errors.
        Some(b'0') => digits.len() == 1 && !negative,
        Some(b'1'..=b'9') => digits.bytes().all(|byte| byte.is_ascii_digit()),
        _ => false,
    };
    if !valid {
        return Err(integer_error());
    }
    let amount: i64 = text.parse().map_err(|_| integer_error())?;
    if amount <= 0 {
        return Err(invalid_expire_error());
    }
    Ok(amount as u64)
}

fn ttl(store: &Store, args: &[Vec<u8>], now: Instant) -> Reply {
    ttl_reply(store, args, now, "ttl", seconds)
}

fn pttl(store: &Store, args: &[Vec<u8>], now: Instant) -> Reply {
    ttl_reply(store, args, now, "pttl", millis)
}

/// Formats `TTL`/`PTTL` from the store's remaining lifetime.
fn ttl_reply(
    store: &Store,
    args: &[Vec<u8>],
    now: Instant,
    name: &str,
    format: fn(Duration) -> i64,
) -> Reply {
    match args {
        [key] => Reply::Integer(match store.ttl(key, now) {
            Ttl::Missing => -2,
            Ttl::Persistent => -1,
            Ttl::Remaining(remaining) => format(remaining),
        }),
        _ => arity_error(name),
    }
}

/// Remaining milliseconds, clamped to what a RESP integer can carry.
fn millis(remaining: Duration) -> i64 {
    i64::try_from(remaining.as_millis()).unwrap_or(i64::MAX)
}

/// Remaining seconds, rounded to nearest as Redis reports them.
fn seconds(remaining: Duration) -> i64 {
    millis(remaining).saturating_add(500) / 1000
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

fn ping(args: &[Vec<u8>]) -> Reply {
    match args {
        [] => Reply::Simple("PONG"),
        [message] => Reply::Bulk(Some(Arc::from(message.as_slice()))),
        _ => arity_error("ping"),
    }
}

fn arity_error(name: &str) -> Reply {
    Reply::Error(format!(
        "ERR wrong number of arguments for '{name}' command"
    ))
}

fn syntax_error() -> Reply {
    Reply::Error("ERR syntax error".into())
}

fn integer_error() -> Reply {
    Reply::Error("ERR value is not an integer or out of range".into())
}

fn invalid_expire_error() -> Reply {
    Reply::Error("ERR invalid expire time in 'set' command".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_executed(store: &Store, now: Instant, parts: &[&[u8]]) -> Executed {
        let args: Vec<Vec<u8>> = parts.iter().map(|part| part.to_vec()).collect();
        execute(store, &args, now).expect("a reply")
    }

    fn run(store: &Store, now: Instant, parts: &[&[u8]]) -> Reply {
        run_executed(store, now, parts).reply
    }

    fn bulk(bytes: &[u8]) -> Reply {
        Reply::Bulk(Some(Arc::from(bytes)))
    }

    #[test]
    fn crud_lifecycle_through_dispatch() {
        let store = Store::new();
        let now = Instant::now();

        assert_eq!(run(&store, now, &[b"GET", b"k"]), Reply::Bulk(None));
        assert_eq!(run(&store, now, &[b"SET", b"k", b"v"]), Reply::Simple("OK"));
        assert_eq!(run(&store, now, &[b"GET", b"k"]), bulk(b"v"));
        assert_eq!(run(&store, now, &[b"EXISTS", b"k"]), Reply::Integer(1));
        assert_eq!(run(&store, now, &[b"DEL", b"k"]), Reply::Integer(1));
        assert_eq!(run(&store, now, &[b"DEL", b"k"]), Reply::Integer(0));
        assert_eq!(run(&store, now, &[b"GET", b"k"]), Reply::Bulk(None));
    }

    #[test]
    fn multi_key_exists_counts_duplicates_and_del_removes_once() {
        let store = Store::new();
        let now = Instant::now();
        store.set(b"a", b"1", None);

        assert_eq!(
            run(&store, now, &[b"EXISTS", b"a", b"a", b"missing"]),
            Reply::Integer(2)
        );
        assert_eq!(run(&store, now, &[b"DEL", b"a", b"a"]), Reply::Integer(1));
        assert_eq!(run(&store, now, &[b"EXISTS", b"a"]), Reply::Integer(0));
    }

    #[test]
    fn empty_value_is_not_a_miss() {
        let store = Store::new();
        let now = Instant::now();
        assert_eq!(run(&store, now, &[b"SET", b"k", b""]), Reply::Simple("OK"));
        assert_eq!(run(&store, now, &[b"GET", b"k"]), bulk(b""));
        assert_eq!(run(&store, now, &[b"GET", b"other"]), Reply::Bulk(None));
    }

    #[test]
    fn command_names_are_case_insensitive() {
        let store = Store::new();
        let now = Instant::now();
        assert_eq!(run(&store, now, &[b"SeT", b"k", b"v"]), Reply::Simple("OK"));
        assert_eq!(run(&store, now, &[b"gEt", b"k"]), bulk(b"v"));
        assert_eq!(run(&store, now, &[b"Ping"]), Reply::Simple("PONG"));
        assert_eq!(run(&store, now, &[b"TtL", b"k"]), Reply::Integer(-1));
        assert_eq!(run(&store, now, &[b"PtTl", b"k"]), Reply::Integer(-1));
    }

    #[test]
    fn set_accepts_ex_and_px() {
        let store = Store::new();
        let now = Instant::now();

        assert_eq!(
            run(&store, now, &[b"SET", b"k", b"v", b"ex", b"10"]),
            Reply::Simple("OK")
        );
        assert_eq!(
            run(&store, now + Duration::from_secs(9), &[b"GET", b"k"]),
            bulk(b"v")
        );
        assert_eq!(
            run(&store, now + Duration::from_secs(10), &[b"GET", b"k"]),
            Reply::Bulk(None)
        );
        assert_eq!(
            run(&store, now + Duration::from_secs(10), &[b"TTL", b"k"]),
            Reply::Integer(-2)
        );

        assert_eq!(
            run(&store, now, &[b"SET", b"p", b"v", b"PX", b"250"]),
            Reply::Simple("OK")
        );
        assert_eq!(
            run(&store, now + Duration::from_millis(200), &[b"GET", b"p"]),
            bulk(b"v")
        );
        assert_eq!(
            run(&store, now + Duration::from_millis(250), &[b"GET", b"p"]),
            Reply::Bulk(None)
        );
    }

    #[test]
    fn plain_set_clears_an_existing_expiry() {
        let store = Store::new();
        let now = Instant::now();

        assert_eq!(
            run(&store, now, &[b"SET", b"k", b"v", b"PX", b"100000"]),
            Reply::Simple("OK")
        );
        assert_eq!(run(&store, now, &[b"TTL", b"k"]), Reply::Integer(100));

        assert_eq!(
            run(&store, now, &[b"SET", b"k", b"v2"]),
            Reply::Simple("OK")
        );
        assert_eq!(run(&store, now, &[b"TTL", b"k"]), Reply::Integer(-1));
        assert_eq!(run(&store, now, &[b"PTTL", b"k"]), Reply::Integer(-1));
        assert_eq!(
            run(&store, now + Duration::from_secs(200), &[b"GET", b"k"]),
            bulk(b"v2")
        );
    }

    #[test]
    fn ttl_reports_missing_persistent_and_remaining() {
        let store = Store::new();
        let now = Instant::now();

        assert_eq!(run(&store, now, &[b"TTL", b"missing"]), Reply::Integer(-2));
        assert_eq!(run(&store, now, &[b"PTTL", b"missing"]), Reply::Integer(-2));

        run(&store, now, &[b"SET", b"k", b"v"]);
        assert_eq!(run(&store, now, &[b"TTL", b"k"]), Reply::Integer(-1));
        assert_eq!(run(&store, now, &[b"PTTL", b"k"]), Reply::Integer(-1));

        run(&store, now, &[b"SET", b"k", b"v", b"PX", b"2500"]);
        assert_eq!(run(&store, now, &[b"PTTL", b"k"]), Reply::Integer(2500));
        assert_eq!(run(&store, now, &[b"TTL", b"k"]), Reply::Integer(3));
        assert_eq!(
            run(&store, now + Duration::from_millis(2000), &[b"PTTL", b"k"]),
            Reply::Integer(500)
        );
        assert_eq!(
            run(&store, now + Duration::from_millis(2000), &[b"TTL", b"k"]),
            Reply::Integer(1)
        );
    }

    #[test]
    fn ttl_rounds_to_the_nearest_second() {
        let store = Store::new();
        let now = Instant::now();

        for (millis, expected) in [(499, 0), (500, 1), (1499, 1), (1500, 2)] {
            run(
                &store,
                now,
                &[b"SET", b"k", b"v", b"PX", millis.to_string().as_bytes()],
            );
            assert_eq!(
                run(&store, now, &[b"TTL", b"k"]),
                Reply::Integer(expected),
                "PX {millis}"
            );
        }
    }

    #[test]
    fn set_rejects_arity_errors() {
        let store = Store::new();
        let now = Instant::now();

        let cases: &[&[&[u8]]] = &[&[b"SET"], &[b"SET", b"k"]];
        for parts in cases {
            assert_eq!(
                run(&store, now, parts),
                Reply::Error("ERR wrong number of arguments for 'set' command".into())
            );
        }
        assert_eq!(run(&store, now, &[b"GET", b"k"]), Reply::Bulk(None));
    }

    #[test]
    fn set_rejects_bad_expiry_arguments() {
        let store = Store::new();
        let now = Instant::now();

        let syntax: &[&[&[u8]]] = &[
            &[b"SET", b"k", b"v", b"EX"],
            &[b"SET", b"k", b"v", b"EX", b"10", b"PX", b"5"],
            &[b"SET", b"k", b"v", b"EX", b"10", b"EX", b"20"],
            &[b"SET", b"k", b"v", b"KEEPTTL"],
            &[b"SET", b"k", b"v", b"EX", b"10", b"junk"],
        ];
        for parts in syntax {
            assert_eq!(
                run(&store, now, parts),
                Reply::Error("ERR syntax error".into()),
                "{parts:?}"
            );
        }

        let integer: &[&[&[u8]]] = &[
            &[b"SET", b"k", b"v", b"EX", b"abc"],
            &[b"SET", b"k", b"v", b"EX", b"+10"],
            &[b"SET", b"k", b"v", b"EX", b"007"],
            &[b"SET", b"k", b"v", b"EX", b"00"],
            &[b"SET", b"k", b"v", b"EX", b"-0"],
            &[b"SET", b"k", b"v", b"EX", b"9223372036854775808"],
            &[b"SET", b"k", b"v", b"PX", b""],
        ];
        for parts in integer {
            assert_eq!(
                run(&store, now, parts),
                Reply::Error("ERR value is not an integer or out of range".into()),
                "{parts:?}"
            );
        }

        let invalid: &[&[&[u8]]] = &[
            &[b"SET", b"k", b"v", b"EX", b"0"],
            &[b"SET", b"k", b"v", b"EX", b"-1"],
            &[b"SET", b"k", b"v", b"PX", b"0"],
            &[b"SET", b"k", b"v", b"EX", b"9223372036854775807"],
        ];
        for parts in invalid {
            assert_eq!(
                run(&store, now, parts),
                Reply::Error("ERR invalid expire time in 'set' command".into()),
                "{parts:?}"
            );
        }

        // No rejected form stored anything.
        assert_eq!(run(&store, now, &[b"GET", b"k"]), Reply::Bulk(None));
    }

    #[test]
    fn expired_keys_are_absent_for_del_and_exists() {
        let store = Store::new();
        let now = Instant::now();
        run(&store, now, &[b"SET", b"k", b"v", b"PX", b"100"]);

        let later = now + Duration::from_millis(200);
        assert_eq!(run(&store, later, &[b"EXISTS", b"k"]), Reply::Integer(0));
        assert_eq!(run(&store, later, &[b"DEL", b"k"]), Reply::Integer(0));
        assert_eq!(run(&store, later, &[b"TTL", b"k"]), Reply::Integer(-2));
    }

    #[test]
    fn get_requires_exactly_one_key() {
        let store = Store::new();
        let now = Instant::now();
        assert_eq!(
            run(&store, now, &[b"GET"]),
            Reply::Error("ERR wrong number of arguments for 'get' command".into())
        );
        assert_eq!(
            run(&store, now, &[b"GeT", b"a", b"b"]),
            Reply::Error("ERR wrong number of arguments for 'get' command".into())
        );
    }

    #[test]
    fn ttl_commands_require_exactly_one_key() {
        let store = Store::new();
        let now = Instant::now();
        for (name, lower) in [(&b"TTL"[..], "ttl"), (&b"PTTL"[..], "pttl")] {
            let expected = Reply::Error(format!(
                "ERR wrong number of arguments for '{lower}' command"
            ));
            assert_eq!(run(&store, now, &[name]), expected);
            assert_eq!(run(&store, now, &[name, b"a", b"b"]), expected);
        }
    }

    #[test]
    fn del_and_exists_require_a_key() {
        let store = Store::new();
        let now = Instant::now();
        assert_eq!(
            run(&store, now, &[b"DEL"]),
            Reply::Error("ERR wrong number of arguments for 'del' command".into())
        );
        assert_eq!(
            run(&store, now, &[b"EXISTS"]),
            Reply::Error("ERR wrong number of arguments for 'exists' command".into())
        );
    }

    #[test]
    fn ping_echoes_one_argument_only() {
        let now = Instant::now();
        assert_eq!(run(&Store::new(), now, &[b"PING"]), Reply::Simple("PONG"));
        assert_eq!(run(&Store::new(), now, &[b"PING", b"hi"]), bulk(b"hi"));
        assert_eq!(
            run(&Store::new(), now, &[b"PING", b"a", b"b"]),
            Reply::Error("ERR wrong number of arguments for 'ping' command".into())
        );
    }

    #[test]
    fn quit_replies_ok_and_closes_while_other_commands_stay_open() {
        let store = Store::new();
        let now = Instant::now();

        let quit = run_executed(&store, now, &[b"QUIT"]);
        assert_eq!(quit.reply, Reply::Simple("OK"));
        assert!(quit.close);

        let ping = run_executed(&store, now, &[b"PING"]);
        assert_eq!(ping.reply, Reply::Simple("PONG"));
        assert!(!ping.close);
    }

    #[test]
    fn unknown_command_echoes_its_name() {
        let store = Store::new();
        let now = Instant::now();
        assert_eq!(
            run(&store, now, &[b"FOO", b"bar"]),
            Reply::Error("ERR unknown command 'FOO'".into())
        );
        assert!(
            execute(&store, &[], now).is_none(),
            "an empty request has no reply"
        );
    }

    #[test]
    fn unknown_command_escapes_control_bytes() {
        let store = Store::new();
        let now = Instant::now();
        assert_eq!(
            run(&store, now, &[b"X\r\n+Fake"]),
            Reply::Error("ERR unknown command 'X\\r\\n+Fake'".into())
        );
    }
}
