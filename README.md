# rustRedis

A minimal RESP2 Redis-like server in Rust.

## What works now

- Binds `127.0.0.1:6379` and accepts connections forever: blocking `std::net`, one detached thread per connection, no deps, no async runtime.
- Speaks RESP2: requests are arrays of bulk strings; replies are simple strings, bulk strings, integers, and errors. No inline commands, no RESP3.
- Commands: `GET`, `SET`, `DEL`, `EXISTS`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`, `PING`, `QUIT`. `SET` accepts optional `NX`, `XX`, `KEEPTTL`, and `GET`, plus `EX <seconds>` or `PX <milliseconds>`. `NX`/`XX` gate the write, `KEEPTTL` preserves the existing deadline, and `GET` returns the previous value instead of `+OK`. `EXAT`/`PXAT` are syntax errors.
- An in-memory key/value store (`src/store.rs`): `get`, `set`, `set_if`, `del`, `del_many`, `exists`, `exists_many`, `ttl`, `set_expiry`, `persist` over binary-safe `Vec<u8>` keys and `Arc<[u8]>` values, backed by a single `RwLock<HashMap<_, Entry>>`, shared across connection threads as `Arc<Store>`. Each entry carries an optional monotonic `Instant` deadline; `set` takes that deadline as a third argument (`None` clears any old one), while `set_if` gates the write on a condition and keeps or replaces the deadline under one lock.
- Lazy expiry (`src/store.rs`): a key is dead once `now` reaches its deadline. Commands that touch a key treat an expired entry as absent and physically remove it on access, so there is no background sweeper and keys that are never accessed can linger. `TTL` returns `-2` for a missing/expired key and `-1` for a key with no deadline, rounding to the nearest second like Redis; `PTTL` reports exact milliseconds.
- Optional idle timeout (`src/lib.rs`): `Server::idle_timeout(Some(duration))` arms read and write timeouts, so a connection that stalls mid-request or stops draining replies is closed. `None` or a zero duration leaves sockets blocking, and the binary leaves it disabled.
- A connection cap (`src/lib.rs`): `Server::max_connections(Some(n))` serves at most `n` connections at once; extra peers get `-ERR max number of clients reached` and are closed. `None` (the default) or `Some(0)` means unlimited, and the binary caps at `Some(10_000)` unless `REDIS_MAX_CONNECTIONS` overrides it. A silent client holds a slot until it closes or an idle timeout fires.

## Run it

```
cargo run
```

Then in another shell:

```
redis-cli SET greeting hello
redis-cli GET greeting
redis-cli DEL greeting
```

`redis-cli` works, but its startup `COMMAND DOCS` hint request is answered with an unknown-command error, so tab completion stays empty.

Or, with the server running in another shell, run the raw-socket smoke test (sends RESP2 bytes and checks each reply):

```
cargo run --example smoke
```

It targets `127.0.0.1:6379` by default; pass another address with `cargo run --example smoke -- ADDR`.

## Not yet

- Absolute-time expiry (`EXPIREAT`/`PEXPIREAT`, `SET ... EXAT/PXAT`).
- Persistence, and value types beyond opaque bytes (lists, hashes, sets).
- `MULTI`/transactions, keyspace notifications, pub/sub, clustering.
- RESP3 (`HELLO`), inline commands, and the `COMMAND` introspection commands.
- Graceful shutdown (Ctrl-C severs in-flight connections), and anything beyond thread-per-connection (silent clients each pin a thread up to the connection cap unless an idle timeout is set).
