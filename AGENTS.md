# AGENTS.md

Learning project: a Redis-like server built in deliberate milestones. Read this before changing behavior, not just before writing code.

## Current state (do not "fix" this)

- The server is a blocking TCP **echo** server. It reflects every byte verbatim; it does not parse RESP or execute commands.
- `redis-cli` interop is a future milestone. Failure against this server is expected, not a bug.
- `Store` (`src/store.rs`) is complete but **not reachable from the network**. Wiring it up is the next milestone (RESP parsing + command layer); do not bolt on an ad-hoc command layer.
- README's "Not yet" list is the intended scope: no TTL, persistence, value types, graceful shutdown, or async runtime.

## Constraints

- **No dependencies.** `Cargo.toml` has an empty `[dependencies]` on purpose. Do not add crates (tokio, redis, bytes, …) without explicit approval; the project is std-only.
- Edition 2024, so a recent toolchain is required (verified with Rust 1.99).
- Crate/package name is `rust_redis` (underscore), not `rustRedis`; import it as `rust_redis`.
- The bind address lives in `src/main.rs` only. `Server` never hardcodes an address; tests rely on `bind("127.0.0.1:0")`.
- `Cargo.lock` is gitignored and untracked even though this builds a binary. Leave it that way.

## Layout

- `src/lib.rs` — `Server`: `bind`, `local_addr`, `serve`. Thread-per-connection, 4096-byte read buffer, TCP_NODELAY best-effort, transient accept errors retried. No protocol.
- `src/store.rs` — `Store`: `RwLock<HashMap<Vec<u8>, Arc<[u8]>>>`. Binary-safe keys/values, empty key and empty value are valid. `get` returns an owned `Arc<[u8]>` snapshot; `Store` is not `Clone` (share an `Arc<Store>`). A poisoned lock panics deliberately.
- `src/main.rs` — binary entrypoint; binds `127.0.0.1:6379`.
- `tests/echo.rs`, `tests/store.rs` — integration tests; there are no unit tests in `src/`.
- `scripts/echo.ps1` — manual smoke test (PowerShell/Windows); no bash equivalent.

## Commands

```powershell
cargo run                                   # listens on 127.0.0.1:6379
cargo test                                  # all integration tests
cargo test --test store                     # one suite
cargo test crud_lifecycle                   # one test by name
cargo clippy --all-targets -- -D warnings   # currently clean; keep it that way
cargo fmt
./scripts/echo.ps1                          # run while cargo run is active
```

No CI, rustfmt, or clippy config exists, so tool defaults apply. Before finishing, run `cargo test` and clippy.

## Testing quirks

- Network tests bind port `0`, read `local_addr()` before connecting, and serve from a detached thread. There are no sleeps or retries; preserve that pattern so tests stay fast and non-flaky.
- Tests never need a running server; only `scripts/echo.ps1` does. Never point tests at port 6379.
- Store tests assert binary safety and that empty keys/values are distinct from absence. Keep those semantics when extending the store.

## Style Guide

- Keep module doc and function doc comment short and succint, focused on implementation details only, do not let planning decisions leak into comments
- Use bullet point styles, not full english sentences
- Do not repeat details in both module doc and funciton doc