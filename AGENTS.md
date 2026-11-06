# AGENTS.md

Learning project: a Redis-like server built in deliberate milestones. Read this before changing behavior, not just before writing code.

## Current state (do not "fix" this)

- The server is a blocking, thread-per-connection **RESP2** server. Every connection thread shares one `Arc<Store>`; the command layer translates requests into store calls.
- Command surface is deliberately small: `GET`, `SET`, `DEL`, `EXISTS`, `PING`, `QUIT`. `SET` accepts exactly two arguments; any fourth argument is `ERR syntax error`. No expiry, no conditional set, no value types.
- Requests must be arrays of bulk strings. Inline commands are a protocol error. `*0` is ignored with no reply. Unknown commands reply with an error and keep the connection open; protocol errors reply and close.
- `redis-cli` interop works for the commands above. Its `COMMAND DOCS` hint request is answered with an unknown-command error, so tab completion is empty.
- README's "Not yet" list is the intended scope: no TTL, persistence, value types, graceful shutdown, RESP3, or async runtime.

## Constraints

- **No dependencies.** `Cargo.toml` has an empty `[dependencies]` on purpose. Do not add crates (tokio, redis, bytes, …) without explicit approval; the project is std-only.
- Edition 2024, so a recent toolchain is required (verified with Rust 1.99).
- Crate/package name is `rust_redis` (underscore), not `rustRedis`; import it as `rust_redis`.
- The bind address lives in `src/main.rs` only. `Server::bind(addr, store)` never hardcodes an address; tests rely on `bind("127.0.0.1:0", …)` and never on port 6379.
- Protocol limits are `MAX_LINE`, `MAX_BULK`, and `MAX_ARGS` in `src/resp.rs`. They bound allocation from peer-controlled lengths; do not remove or bypass them.
- `Cargo.lock` is gitignored and untracked even though this builds a binary. Leave it that way.

## Layout

- `src/lib.rs` — `Server { listener, store }`: `bind`, `local_addr`, `serve`. Thread-per-connection, TCP_NODELAY best-effort, transient accept errors retried. The connection loop is `read_request` → `execute` → `encode` into a reused buffer → one `write_all` per request; `Executed::close` and clean EOF end it with a `Disconnected` reason, which `serve` logs as exactly one line per connection end.
- `src/resp.rs` — `Reply` and `encode`; `read_request` over any `BufRead`; `ProtocolError`/`RequestError`; the three caps. Header reads are capped with `Read::take` because `read_until` grows without bound; bulk payloads grow with received bytes; error text escapes peer bytes.
- `src/command.rs` — `execute(&Store, &[Vec<u8>]) -> Option<Executed>`; `Executed { reply, close }` pairs a frame with whether the connection closes. Socket-free: name matching (ASCII case-insensitive), arity checks, store access. `None` means "no reply" (`*0`).
- `src/store.rs` — `Store`: `RwLock<HashMap<Vec<u8>, Arc<[u8]>>>`. Binary-safe keys/values, empty key and empty value are valid. `get` returns an owned `Arc<[u8]>` snapshot; `del_many`/`exists_many` accept any iterator of key slices and hold one guard each, so a batch is a snapshot. `Store` is not `Clone` (share an `Arc<Store>`). A poisoned lock panics deliberately.
- `src/main.rs` — binary entrypoint; binds `127.0.0.1:6379` with a fresh store.
- `tests/resp.rs`, `tests/store.rs` — integration tests. Unit tests for the parser and the dispatcher live in `src/resp.rs` and `src/command.rs`.
- `scripts/resp.ps1` — manual smoke test (PowerShell/Windows); no bash equivalent.

## Commands

```powershell
cargo run                                   # listens on 127.0.0.1:6379
cargo test                                  # unit + integration tests
cargo test --test resp                      # one suite
cargo test crud_lifecycle                   # one test by name
cargo clippy --all-targets -- -D warnings   # currently clean; keep it that way
cargo fmt
./scripts/resp.ps1                          # run while cargo run is active
```

No CI, rustfmt, or clippy config exists, so tool defaults apply. Before finishing, run `cargo test` and clippy.

## Testing quirks

- Network tests bind port `0`, read `local_addr()` before connecting, and serve from a detached thread. There are no sleeps or retries; preserve that pattern so tests stay fast and non-flaky.
- Replies are asserted as **exact bytes** (`expect_reply`), not decoded: the byte literals are the contract. `tests/resp.rs` also builds requests with the `command` helper.
- Pass the same `Arc<Store>` into `start_server` and into the test when a case needs to check store state directly; that proves the wire reached the shared map.
- Cover framing by writing raw bytes: pipelining, a request split across writes, `*0`, bare line feeds, and protocol errors. Parser caps belong in the unit tests in `src/resp.rs`, not over a socket.
- Tests never need a running server; only `scripts/resp.ps1` does. Never point tests at port 6379.
- Store tests assert binary safety and that empty keys/values are distinct from absence. Keep those semantics when extending the store.

## Style Guide

- Keep module doc and function doc comment short and succint, focused on implementation details only, do not let planning decisions leak into comments
- Use bullet point styles, not full english sentences
- Do not repeat details in both module doc and function doc
