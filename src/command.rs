//! Command dispatch over the store.
//!
//! - socket-free: name matching, arity checks, store access only
//! - expiry is relative only: `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`, and `SET ... EX/PX`
//! - `SET` also accepts `NX`, `XX`, `KEEPTTL`, and `GET`; `EXAT`/`PXAT` stay syntax errors
//! - the caller passes one `now` per request to drive lazy eviction

use std::sync::Arc;
use std::time::Instant;

use crate::expiry::{self, TimeUnit};
use crate::resp::{Reply, escaped};
use crate::store::{Condition, Store, TtlPolicy};

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

    let ttl = match ttl_policy(options.ttl, now) {
        Ok(ttl) => ttl,
        Err(reply) => return reply,
    };

    let outcome = store.set_if(key, value, ttl, options.condition, now);

    if options.get {
        Reply::Bulk(outcome.previous)
    } else if outcome.applied {
        Reply::Simple("OK")
    } else {
        Reply::Bulk(None)
    }
}

/// Resolves `SET`'s TTL option into a store policy.
fn ttl_policy(ttl: SetTtl<'_>, now: Instant) -> Result<TtlPolicy, Reply> {
    match ttl {
        SetTtl::Clear => Ok(TtlPolicy::Clear),
        SetTtl::Keep => Ok(TtlPolicy::Keep),
        SetTtl::Expire(unit, raw) => {
            let value = expiry::parse_int(raw).ok_or_else(integer_error)?;
            let deadline = expiry::deadline(unit, value, now)
                .ok()
                .flatten()
                .ok_or_else(|| invalid_expire_time("set"))?;
            Ok(TtlPolicy::Expire(deadline))
        }
    }
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

    let Some(value) = expiry::parse_int(raw) else {
        return integer_error();
    };

    // A non-positive TTL deletes the key, so the exact past instant does not matter.
    let deadline = match expiry::deadline(unit, value, now) {
        Ok(Some(deadline)) => deadline,
        Ok(None) => now,
        Err(()) => return invalid_expire_time(name),
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

    Reply::Integer(expiry::ttl_value(store.ttl(key, now), unit))
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

/// Parsed `SET` options.
struct SetOptions<'a> {
    condition: Condition,
    ttl: SetTtl<'a>,
    get: bool,
}

/// What `SET` does with an existing deadline.
#[derive(Clone, Copy)]
enum SetTtl<'a> {
    /// Drop any deadline.
    Clear,
    /// Keep the surviving entry's deadline.
    Keep,
    /// Install a new deadline from the raw `EX`/`PX` argument.
    Expire(TimeUnit, &'a [u8]),
}

/// Parses `SET` options with Redis's order-sensitive rules.
///
/// - `NX` and `XX` may repeat but conflict with each other in either order
/// - `KEEPTTL` conflicts with `EX`/`PX` in either order
/// - duplicate `EX`/`PX` is allowed; the last one wins; mixing units is a syntax error
/// - `None` is a syntax error
fn parse_set_options(options: &[Vec<u8>]) -> Option<SetOptions<'_>> {
    let mut condition = Condition::Always;
    let mut ttl = SetTtl::Clear;
    let mut get = false;
    let mut index = 0;

    while index < options.len() {
        let option = &options[index];
        let unit = if option.eq_ignore_ascii_case(b"ex") {
            Some(TimeUnit::Seconds)
        } else if option.eq_ignore_ascii_case(b"px") {
            Some(TimeUnit::Millis)
        } else {
            None
        };

        if let Some(unit) = unit {
            if matches!(ttl, SetTtl::Keep) {
                return None;
            }
            if let SetTtl::Expire(seen, _) = ttl
                && seen != unit
            {
                return None;
            }
            let raw = options.get(index + 1).map(Vec::as_slice)?;
            ttl = SetTtl::Expire(unit, raw);
            index += 2;
            continue;
        }

        if option.eq_ignore_ascii_case(b"nx") {
            if condition == Condition::Xx {
                return None;
            }
            condition = Condition::Nx;
        } else if option.eq_ignore_ascii_case(b"xx") {
            if condition == Condition::Nx {
                return None;
            }
            condition = Condition::Xx;
        } else if option.eq_ignore_ascii_case(b"get") {
            get = true;
        } else if option.eq_ignore_ascii_case(b"keepttl") {
            if matches!(ttl, SetTtl::Expire(..)) {
                return None;
            }
            ttl = SetTtl::Keep;
        } else {
            return None;
        }
        index += 1;
    }

    Some(SetOptions {
        condition,
        ttl,
        get,
    })
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
    use std::time::Duration;

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
            run(&store, &[b"SET", b"k", b"v", b"EXAT", b"10"]),
            Reply::Error("ERR syntax error".into())
        );
        // The rejected SET stored nothing.
        assert_eq!(run(&store, &[b"GET", b"k"]), Reply::Bulk(None));
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
            &[b"SET".as_slice(), b"k", b"v", b"NX", b"XX"][..],
            &[b"SET".as_slice(), b"k", b"v", b"XX", b"NX"][..],
            &[b"SET".as_slice(), b"k", b"v", b"KEEPTTL", b"EX", b"10"][..],
            &[b"SET".as_slice(), b"k", b"v", b"EX", b"10", b"KEEPTTL"][..],
            &[b"SET".as_slice(), b"k", b"v", b"EXAT", b"10"][..],
            &[b"SET".as_slice(), b"k", b"v", b"PXAT", b"10"][..],
            &[b"SET".as_slice(), b"k", b"v", b"FOO"][..],
            &[b"SET".as_slice(), b"k", b"v", b"EX"][..],
        ] {
            assert_eq!(run_at(&store, parts, now), syntax, "{parts:?}");
        }

        // Duplicate flags are accepted, as in Redis.
        assert_eq!(
            run_at(&store, &[b"SET", b"dup-nx", b"v", b"NX", b"NX"], now),
            Reply::Simple("OK")
        );
        assert_eq!(
            run_at(&store, &[b"SET", b"dup-nx", b"v2", b"GET", b"GET"], now),
            bulk(b"v")
        );
        assert_eq!(
            run_at(
                &store,
                &[b"SET", b"dup-nx", b"v3", b"KEEPTTL", b"KEEPTTL"],
                now
            ),
            Reply::Simple("OK")
        );

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
    fn set_nx_and_xx_gate_writes() {
        let store = Store::new();

        assert_eq!(
            run(&store, &[b"SET", b"k", b"v", b"NX"]),
            Reply::Simple("OK")
        );
        assert_eq!(
            run(&store, &[b"SET", b"k", b"other", b"NX"]),
            Reply::Bulk(None)
        );
        assert_eq!(run(&store, &[b"GET", b"k"]), bulk(b"v"));

        assert_eq!(
            run(&store, &[b"SET", b"k", b"other", b"XX"]),
            Reply::Simple("OK")
        );
        assert_eq!(run(&store, &[b"GET", b"k"]), bulk(b"other"));

        assert_eq!(
            run(&store, &[b"SET", b"missing", b"v", b"XX"]),
            Reply::Bulk(None)
        );
        assert_eq!(run(&store, &[b"GET", b"missing"]), Reply::Bulk(None));
    }

    #[test]
    fn set_nx_treats_an_expired_key_as_absent() {
        let store = Store::new();
        let now = Instant::now();

        run_at(&store, &[b"SET", b"k", b"dead", b"PX", b"100"], now);
        assert_eq!(
            run_at(&store, &[b"SET", b"k", b"fresh", b"NX"], at(now, 1)),
            Reply::Simple("OK")
        );
        assert_eq!(run_at(&store, &[b"GET", b"k"], at(now, 1)), bulk(b"fresh"));
    }

    #[test]
    fn set_get_returns_the_previous_value_and_not_ok() {
        let store = Store::new();

        assert_eq!(
            run(&store, &[b"SET", b"k", b"v", b"GET"]),
            Reply::Bulk(None)
        );
        assert_eq!(run(&store, &[b"GET", b"k"]), bulk(b"v"));

        assert_eq!(run(&store, &[b"SET", b"k", b"other", b"GET"]), bulk(b"v"));
        assert_eq!(run(&store, &[b"GET", b"k"]), bulk(b"other"));
    }

    #[test]
    fn set_nx_get_returns_the_old_value_without_writing() {
        let store = Store::new();
        run(&store, &[b"SET", b"k", b"v"]);

        assert_eq!(
            run(&store, &[b"SET", b"k", b"other", b"NX", b"GET"]),
            bulk(b"v")
        );
        assert_eq!(run(&store, &[b"GET", b"k"]), bulk(b"v"));

        // XX on a missing key replies nil even with GET.
        assert_eq!(
            run(&store, &[b"SET", b"missing", b"other", b"XX", b"GET"]),
            Reply::Bulk(None)
        );
        assert_eq!(run(&store, &[b"GET", b"missing"]), Reply::Bulk(None));

        // XX GET on a live key returns the old value and writes.
        assert_eq!(
            run(&store, &[b"SET", b"k", b"third", b"XX", b"GET"]),
            bulk(b"v")
        );
        assert_eq!(run(&store, &[b"GET", b"k"]), bulk(b"third"));
    }

    #[test]
    fn set_keepttl_preserves_a_deadline_and_plain_set_clears_it() {
        let store = Store::new();
        let now = Instant::now();

        run_at(&store, &[b"SET", b"k", b"v", b"PX", b"1500"], now);
        assert_eq!(
            run_at(&store, &[b"SET", b"k", b"other", b"KEEPTTL"], now),
            Reply::Simple("OK")
        );
        assert_eq!(run_at(&store, &[b"PTTL", b"k"], now), Reply::Integer(1500));

        assert_eq!(
            run_at(&store, &[b"SET", b"new", b"v", b"KEEPTTL"], now),
            Reply::Simple("OK")
        );
        assert_eq!(run_at(&store, &[b"TTL", b"new"], now), Reply::Integer(-1));

        assert_eq!(
            run_at(&store, &[b"SET", b"k", b"fresh"], now),
            Reply::Simple("OK")
        );
        assert_eq!(run_at(&store, &[b"TTL", b"k"], now), Reply::Integer(-1));
    }

    #[test]
    fn set_conditions_gate_the_deadline_too() {
        let store = Store::new();
        let now = Instant::now();

        // NX + PX on an existing key: no write and no TTL change.
        run_at(&store, &[b"SET", b"k", b"v", b"EX", b"10"], now);
        assert_eq!(
            run_at(&store, &[b"SET", b"k", b"other", b"NX", b"PX", b"99"], now),
            Reply::Bulk(None)
        );
        assert_eq!(run_at(&store, &[b"GET", b"k"], now), bulk(b"v"));
        assert_eq!(run_at(&store, &[b"TTL", b"k"], now), Reply::Integer(10));

        // NX + PX on a missing key installs the new deadline.
        assert_eq!(
            run_at(
                &store,
                &[b"SET", b"missing", b"v", b"NX", b"PX", b"1500"],
                now
            ),
            Reply::Simple("OK")
        );
        assert_eq!(
            run_at(&store, &[b"PTTL", b"missing"], now),
            Reply::Integer(1500)
        );

        // XX + EX on a missing key creates nothing.
        assert_eq!(
            run_at(&store, &[b"SET", b"absent", b"v", b"XX", b"EX", b"10"], now),
            Reply::Bulk(None)
        );
        assert_eq!(
            run_at(&store, &[b"EXISTS", b"absent"], now),
            Reply::Integer(0)
        );
    }

    #[test]
    fn set_expiry_errors_win_over_a_failing_condition() {
        let store = Store::new();
        let now = Instant::now();
        run_at(&store, &[b"SET", b"k", b"v", b"EX", b"10"], now);

        // NX would fail, but the invalid expiry is reported first.
        assert_eq!(
            run_at(&store, &[b"SET", b"k", b"other", b"EX", b"0", b"NX"], now),
            Reply::Error("ERR invalid expire time in 'set' command".into())
        );
        assert_eq!(
            run_at(&store, &[b"SET", b"k", b"other", b"EX", b"abc", b"NX"], now),
            Reply::Error("ERR value is not an integer or out of range".into())
        );

        // Without a condition, the failed write still leaves value and TTL untouched.
        assert_eq!(
            run_at(&store, &[b"SET", b"k", b"new", b"EX", b"0"], now),
            Reply::Error("ERR invalid expire time in 'set' command".into())
        );
        assert_eq!(run_at(&store, &[b"GET", b"k"], now), bulk(b"v"));
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
}
