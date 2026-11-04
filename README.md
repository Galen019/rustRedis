# rustRedis

A minimal TCP server in Rust, built as a learning foundation for a Redis-like server.

## What works now

- Binds `127.0.0.1:6379` and accepts connections forever: blocking `std::net`, one detached thread per connection, no deps, no async runtime.
- Echoes every byte a client sends back, verbatim, until the client closes its write half.
- An in-memory key/value store (`src/store.rs`): `get`, `set`, `del`, `exists` over binary-safe `Vec<u8>` keys and `Arc<[u8]>` values, backed by a single `RwLock<HashMap<_, _>>`, shareable across connection threads as `Arc<Store>`. No TTL. **Nothing on the network can call it yet.**

## Run it

```
cargo run
```

Then in another shell, run the raw-socket smoke test (a `System.Net.Sockets.TcpClient` that sends a string and checks it comes back unchanged):

```
./scripts/echo.ps1
```

## Not yet

- RESP parsing and real command semantics. The store from the previous milestone exists but is unreachable from a client: that gap is exactly what RESP parsing plus a command layer closes, which is why they are the next milestone rather than a later one.
- `redis-cli` interop: **not yet, next milestone.** It fails today because the server faithfully echoes `redis-cli`'s request instead of parsing it. That failure is expected, not a bug.
- Graceful shutdown (Ctrl-C severs in-flight connections), per-client timeouts, and anything beyond thread-per-connection (silent clients each pin a thread).
- Persistence, expiry, value types, pub/sub, clustering: the fun stuff.
