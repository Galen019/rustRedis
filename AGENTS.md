# AGENTS.md

Learning project: a Redis-like server built in deliberate milestones. Read this before changing behavior, not just before writing code.

## Current state (do not "fix" this)

- The server is a blocking, thread-per-connection **RESP2** server. Every connection thread shares one `Arc<Store>`; the command layer translates requests into store calls.
- Command surface is deliberately small: `GET`, `SET`, `DEL`, `EXISTS`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`, `PING`, `QUIT`. `SET` accepts optional `EX <seconds>` or `PX <milliseconds>`; every other option is `ERR syntax error`. No absolute-time expiry, conditional set, or value types.
- Expiry is lazy: `EXPIRE`/`PEXPIRE`/`SET ... EX/PX` store a monotonic `Instant` deadline, and any command that touches a key treats a past deadline as absent and physically removes the entry. There is no sweeper, so never-accessed expired keys linger. A key is expired when `deadline <= now`; `TTL` rounds to the nearest second like Redis.
- Requests must be arrays of bulk strings. Inline commands are a protocol error. `*0` is ignored with no reply. Unknown commands reply with an error and keep the connection open; protocol errors reply and close.
- `redis-cli` interop works for the commands above. Its `COMMAND DOCS` hint request is answered with an unknown-command error, so tab completion is empty.
- Idle timeouts exist but stay off unless `Server::idle_timeout` is given a non-zero duration; each socket is armed with read and write timeouts, and the binary never sets one.
- `Server::max_connections` caps live connections; `None` (the default) or `Some(0)` means unlimited and the binary sets `Some(10_000)`. `REDIS_MAX_CONNECTIONS` overrides the binary's cap (`0` = unlimited); an unparsable value aborts startup. A peer over the cap is sent `-ERR max number of clients reached` and closed without a thread.
- README's "Not yet" list is the intended scope: no absolute-time expiry, persistence, value types, graceful shutdown, RESP3, or async runtime.

## Constraints

- **No dependencies.** `Cargo.toml` has an empty `[dependencies]` on purpose. Do not add crates (tokio, redis, bytes, …) without explicit approval; the project is std-only.
- Edition 2024, so a recent toolchain is required (verified with Rust 1.99).
- Crate/package name is `rust_redis` (underscore), not `rustRedis`; import it as `rust_redis`.
- The bind address lives in `src/main.rs` only. `Server::bind(addr, store)` never hardcodes an address; tests rely on `bind("127.0.0.1:0", …)` and never on port 6379.
- Protocol limits are `MAX_LINE`, `MAX_BULK`, and `MAX_ARGS` in `src/resp.rs`. They bound allocation from peer-controlled lengths; do not remove or bypass them.
- `Cargo.lock` is gitignored and untracked even though this builds a binary. Leave it that way.

## Layout

- `src/lib.rs` — `Server { listener, store, idle_timeout, max_connections }`: `bind`, `idle_timeout`, `max_connections`, `local_addr`, `serve`. Thread-per-connection, TCP_NODELAY best-effort, transient accept errors retried. A private `Connections` counter and `ConnectionPermit` guard release a slot on drop, covering rejection, spawn failure, and panics. The connection loop is `read_request` → `execute` → `encode` into a reused buffer → one `write_all` per request; `Executed::close`, clean EOF, and a socket timeout each end it with a `Disconnected` reason, which `serve` logs as exactly one line per connection end.
- `src/resp.rs` — `Reply` and `encode`; `read_request` over any `BufRead`; `ProtocolError`/`RequestError`; the three caps. Header reads are capped with `Read::take` because `read_until` grows without bound; bulk payloads grow with received bytes; error text escapes peer bytes.
- `src/command.rs` — `execute(&Store, &[Vec<u8>], Instant) -> Option<Executed>`; `Executed { reply, close }` pairs a frame with whether the connection closes. Socket-free: name matching (ASCII case-insensitive), arity checks, strict integer parsing, store access. The connection loop passes one `Instant::now()` per request to drive lazy eviction; `SET` options are parsed in two passes so syntax errors win over numeric ones. `None` means "no reply" (`*0`).
- `src/store.rs` — `Store`: `RwLock<HashMap<Vec<u8>, Entry>>`, where `Entry { value: Arc<[u8]>, expires_at: Option<Instant> }`. Binary-safe keys/values, empty key and empty value are valid. `set` takes an optional deadline. Every operation takes the write lock through one `live` helper that evicts a past deadline; `get` returns an owned `Arc<[u8]>` snapshot; `del_many`/`exists_many` accept any iterator of key slices and use one guard per batch. `Store` is not `Clone` (share an `Arc<Store>`). A poisoned lock panics deliberately.
- `src/main.rs` — binary entrypoint; binds `127.0.0.1:6379` with a fresh store and reads `REDIS_MAX_CONNECTIONS`.
- `tests/resp.rs` — network tests for the server: commands over a socket, framing, idle timeouts, and the connection cap. Parser, dispatcher, store expiry, and limiter unit tests live next to their code in each `src/*.rs`; the `src/lib.rs` tests module keeps only the limiter tests.
- `examples/smoke.rs` — manual smoke test over a socket; expects a running server and defaults to `127.0.0.1:6379`. It includes the only real-time expiry check (a 50ms TTL plus a 100ms sleep).

## Commands

```powershell
cargo run                                   # listens on 127.0.0.1:6379
cargo test                                  # all tests
cargo test --test resp                      # network suite only
cargo test store::                          # one module
cargo test crud_lifecycle                   # one test by name
cargo clippy --all-targets -- -D warnings   # currently clean; keep it that way
cargo fmt
cargo run --example smoke                   # with the server running
```

No rustfmt or clippy config exists, so tool defaults apply. `.github/workflows/ci.yml` runs `cargo test`, the smoke example, and clippy on pull requests. Before finishing, run `cargo test` and clippy.

## Testing quirks

- Network tests bind port `0`, read `local_addr()` before connecting, and serve from a detached thread. There are no sleeps or retries; preserve that pattern so tests stay fast and non-flaky.
- Idle-timeout tests are one exception: they use a 100–200ms timeout and at most four 100ms sleeps, bounded so a regression fails instead of hanging. Async waits stay on blocking reads.
- Expiry tests are deterministic: store and command tests pass synthetic `Instant`s, and `cargo test` has no sleeps. Elapsed-time expiry is proven only by the bounded sleep in `examples/smoke.rs`.
- Replies are asserted as **exact bytes** (`expect_reply`), not decoded: the byte literals are the contract. Integer replies whose exact value depends on scheduling use `expect_integer_in_range`. The `tests/resp.rs` suite builds requests with the `command` helper.
- Pass the same `Arc<Store>` into `start_server` and into the test when a case needs to check store state directly; that proves the wire reached the shared map.
- Cover framing by writing raw bytes: pipelining, a request split across writes, `*0`, bare line feeds, and protocol errors. Parser caps belong in the unit tests in `src/resp.rs`, not over a socket.
- Tests never need a running server; only `examples/smoke.rs` does. Never point tests at port 6379.
- Store tests assert binary safety and that empty keys/values are distinct from absence. Keep those semantics when extending the store.

## Style Guide

- Keep module doc and function doc comment short and succint, focused on implementation details only, do not let planning decisions leak into comments
- Use bullet point styles, not full english sentences
- Do not repeat details in both module doc and function doc
