# rustRedis

A minimal TCP server in Rust, built as a learning foundation for a Redis-like server.

## What works now

- Binds `127.0.0.1:6379` and accepts connections forever: blocking `std::net`, one detached thread per connection, no deps, no async runtime.
- Echoes every byte a client sends back, verbatim, until the client closes its write half.

## Run it

```
cargo run
```

Then in another shell, run the raw-socket smoke test (a `System.Net.Sockets.TcpClient` that sends a string and checks it comes back unchanged):

```
./scripts/echo.ps1
```

## Not yet

- RESP parsing and real command semantics.
- `redis-cli` interop: **not yet, next milestone.** It fails today because the server faithfully echoes `redis-cli`'s request instead of parsing it. That failure is expected, not a bug.
- Graceful shutdown (Ctrl-C severs in-flight connections), per-client timeouts, and anything beyond thread-per-connection (silent clients each pin a thread).
- Persistence, expiry, pub/sub, clustering: the fun stuff.
