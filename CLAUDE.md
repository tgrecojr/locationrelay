# locationrelay

## Overview
A single-purpose, security-hardened HTTP receiver for Overland iOS location
beacons. It authenticates a `POST`, validates the GeoJSON batch, and appends it
to disk as NDJSON. It intentionally does nothing else — no DB, no dynamic
routes, no read-back/query API, no admin endpoints.

## Tech Stack
- Language: Rust (edition 2024, pinned to 1.94 via `rust-toolchain.toml`)
- HTTP: axum 0.8 (hyper / tokio)
- Middleware: tower / tower-http (timeout, catch-panic, trace), tower_governor (rate limit)
- Crypto: subtle (constant-time token comparison)
- Storage: append-only NDJSON on the local filesystem

## Commands
- `cargo run` — start the server (reads `.env` via dotenvy)
- `cargo build --release` — production build
- `cargo test` — run the integration test suite
- `cargo clippy --all-targets -- -D warnings` — lint (CI treats warnings as errors)
- `cargo fmt --all` — format
- `cargo deny check` — supply-chain / license audit

## Architecture
- `src/lib.rs` — `build_app(config)` assembles the core router + middleware (unit-testable).
- `src/main.rs` — config load, rate-limit + concurrency layers, retention sweep, hand-off to `server::serve`.
- `src/server.rs` — hyper accept loop with an HTTP/1 header-read timeout (slowloris bound), `ConnectInfo` injection, graceful shutdown.
- `src/config.rs` — env-driven `Config` with validation (token strength, tolerant bool parsing, etc.).
- `src/auth.rs` — constant-time Bearer/query-token middleware (`route_layer`, pre-body).
- `src/models.rs` — Overland payload shape + strict GeoJSON validation.
- `src/storage.rs` — server-side date filename, append-only `0600` NDJSON writes, global write lock (no interleaving), retention pruning.
- `src/handlers.rs` — `receive` (POST-only; non-POST black-holed) / `not_found` (no health/status endpoint).
- `src/security.rs` — response security headers.
- `src/error.rs` — non-leaky `AppError` -> HTTP responses.
- `src/observability.rs` — throttled rejection counter + time/date helpers.
- `tests/integration.rs` — auth, validation, storage, headers, method-probe black hole, retention.
- `tests/server.rs` — live serve loop over TCP: happy path, method-probe, slowloris timeout.

## Security invariants (do not regress)
- Exactly one ingest route; no path/route parameters anywhere (designs out IDOR / traversal).
- The route is registered for `any` method so a non-POST is black-holed identically to an unknown path — **never reintroduce `post(...)`**, which leaks an `Allow` header and fingerprints the route.
- Auth runs **before** the body is read; comparison is constant-time.
- Storage filename is derived server-side from UTC date — never from client input; retention only ever deletes strict `YYYY-MM-DD.ndjson` files.
- Error bodies are generic; the token is never logged or echoed (including the query-string form).
- Rejections are rate-limited in the logs (aggregated, not per-event at info).
- The HTTP/1 header-read timeout (slowloris bound) must stay set; it requires a hyper `Timer` (`TokioTimer`) to arm.
- Any new dependency must pass `cargo deny check` and keep the tree minimal.
- Keep files < 300 lines and functions < 50 lines.

## Environment Variables
See `README.md` and `.env.example`. Required: `LOCATIONRELAY_TOKEN` (≥24 chars, minimum variety).
Optional: `LOCATIONRELAY_BIND`, `LOCATIONRELAY_DATA_DIR`, `LOCATIONRELAY_MAX_BODY_BYTES`,
`LOCATIONRELAY_REQUEST_TIMEOUT_SECS`, `LOCATIONRELAY_HEADER_TIMEOUT_SECS`,
`LOCATIONRELAY_MAX_CONCURRENCY`, `LOCATIONRELAY_RATE_PER_SECOND`, `LOCATIONRELAY_RATE_BURST`,
`LOCATIONRELAY_RETENTION_DAYS`, `LOCATIONRELAY_TRUST_PROXY`, `LOCATIONRELAY_FSYNC`,
`LOCATIONRELAY_LOG`.

## CI/CD (.github/workflows)
- `ci.yml` — build/lint/test, `cargo-deny` audit, the `supply-chain` scan, then
  (on push only) builds the distroless image, pushes to
  `ghcr.io/tgrecojr/locationrelay`, cosign keyless-signs it, and attests an SPDX
  SBOM + build provenance to the registry.
- `supply-chain.yml` — reusable: Socket Security scan (org `grecolabs`) + OSV.
  Requires repo/org secret **`SOCKET_SECURITY_API_KEY`**.
- `ghcr-retention.yml` — weekly GHCR prune (keep `latest` + 5 newest tagged).
  Uses `dataaxiom/ghcr-cleanup-action` (multi-arch + sigstore aware) — do not
  swap for `actions/delete-package-versions`.
- `renovate.json` — Mend Renovate: groups Rust minor/patch, pins Docker + Action
  digests, auto-merges non-major after a stability window.

## Deployment
Plain HTTP on localhost behind a TLS-terminating reverse proxy. Rate limiting
keys on the TCP peer IP by default (spoof-proof); set `LOCATIONRELAY_TRUST_PROXY=true`
only behind a proxy that normalizes forwarded headers, for per-device limits.
Never expose the port publicly.
