# rustRedis

A minimal RESP2 server in Rust, built as a learning foundation for a Redis-like server.

## What works now

- Binds `127.0.0.1:6379` and accepts connections forever: blocking `std::net`, one detached thread per connection, no deps, no async runtime.
- Speaks RESP2: requests are arrays of bulk strings; replies are simple strings, bulk strings, integers, and errors. No inline commands, no RESP3.
- Commands: `GET`, `SET`, `DEL`, `EXISTS`, `PING`, `QUIT`. `SET` takes exactly a key and a value: no `EX`/`PX`/`NX`/`XX`, so a fourth argument is a syntax error.
- An in-memory key/value store (`src/store.rs`): `get`, `set`, `del`, `del_many`, `exists`, `exists_many` over binary-safe `Vec<u8>` keys and `Arc<[u8]>` values, backed by a single `RwLock<HashMap<_, _>>`, shared across connection threads as `Arc<Store>`. No TTL.
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

Or run the raw-socket smoke test (a `System.Net.Sockets.TcpClient` that sends RESP2 bytes and checks each reply byte for byte):

```
./scripts/resp.ps1
```

## Not yet

- TTL/expiry, persistence, and value types beyond opaque bytes (lists, hashes, sets).
- `SET` options, `MULTI`/transactions, keyspace notifications, pub/sub, clustering.
- RESP3 (`HELLO`), inline commands, and the `COMMAND` introspection commands.
- Graceful shutdown (Ctrl-C severs in-flight connections), and anything beyond thread-per-connection (silent clients each pin a thread up to the connection cap unless an idle timeout is set).
