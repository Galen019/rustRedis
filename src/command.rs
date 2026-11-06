//! Command dispatch over the store.
//!
//! - socket-free: name matching, arity checks, store access only
//! - `SET` has no options yet, so a fourth argument is a syntax error
//! - no expiry, value types, or transactions

use std::sync::Arc;

use crate::resp::{Reply, escaped};
use crate::store::Store;

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
pub fn execute(store: &Store, args: &[Vec<u8>]) -> Option<Executed> {
    let (name, rest) = args.split_first()?;

    let executed = if name.eq_ignore_ascii_case(b"get") {
        Executed::reply(get(store, rest))
    } else if name.eq_ignore_ascii_case(b"set") {
        Executed::reply(set(store, rest))
    } else if name.eq_ignore_ascii_case(b"del") {
        Executed::reply(del(store, rest))
    } else if name.eq_ignore_ascii_case(b"exists") {
        Executed::reply(exists(store, rest))
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

fn get(store: &Store, args: &[Vec<u8>]) -> Reply {
    match args {
        [key] => Reply::Bulk(store.get(key)),
        _ => arity_error("get"),
    }
}

fn set(store: &Store, args: &[Vec<u8>]) -> Reply {
    match args {
        [key, value] => {
            store.set(key, value);
            Reply::Simple("OK")
        }
        [_, _, _, ..] => Reply::Error("ERR syntax error".into()),
        _ => arity_error("set"),
    }
}

fn del(store: &Store, args: &[Vec<u8>]) -> Reply {
    if args.is_empty() {
        return arity_error("del");
    }
    Reply::Integer(store.del_many(args.iter().map(Vec::as_slice)) as i64)
}

fn exists(store: &Store, args: &[Vec<u8>]) -> Reply {
    if args.is_empty() {
        return arity_error("exists");
    }
    Reply::Integer(store.exists_many(args.iter().map(Vec::as_slice)) as i64)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn run_executed(store: &Store, parts: &[&[u8]]) -> Executed {
        let args: Vec<Vec<u8>> = parts.iter().map(|part| part.to_vec()).collect();
        execute(store, &args).expect("a reply")
    }

    fn run(store: &Store, parts: &[&[u8]]) -> Reply {
        run_executed(store, parts).reply
    }

    fn bulk(bytes: &[u8]) -> Reply {
        Reply::Bulk(Some(Arc::from(bytes)))
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
        store.set(b"a", b"1");

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
    fn set_rejects_arity_errors_and_options() {
        let store = Store::new();
        assert_eq!(
            run(&store, &[b"SET", b"k"]),
            Reply::Error("ERR wrong number of arguments for 'set' command".into())
        );
        assert_eq!(
            run(&store, &[b"SET", b"k", b"v", b"EX", b"10"]),
            Reply::Error("ERR syntax error".into())
        );
        // The rejected SET stored nothing.
        assert_eq!(run(&store, &[b"GET", b"k"]), Reply::Bulk(None));
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

        let quit = run_executed(&store, &[b"QUIT"]);
        assert_eq!(quit.reply, Reply::Simple("OK"));
        assert!(quit.close);

        let ping = run_executed(&store, &[b"PING"]);
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
            execute(&store, &[]).is_none(),
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
