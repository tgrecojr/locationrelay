# locationrelay

## Overview
A single-purpose, security-hardened HTTP receiver for iOS location beacons from
both **Overland** and **OwnTracks**. It authenticates a `POST`, validates the
payload, and writes the **raw request body byte-for-byte** to disk as one
immutable file per POST. It intentionally does nothing else on the inbound
surface — no DB, no dynamic routes, no read-back/query API, no admin endpoints.

Two fixed ingest routes, one per source (no path parameters anywhere). Each
accepted POST is stored verbatim (exact body bytes, unmodified) under a
server-derived path `{stream}/dt=YYYY-MM-DD/{received_unix_ms}_{shortid}.json`:
- `POST /overland` — an Overland GeoJSON batch (`{"locations":[Point features]}`);
  response `{"result":"ok"}`; stored under `overland/dt=…/`. An **empty** batch is
  accepted but not stored.
- `POST /owntracks` — a single OwnTracks message (`{"_type":"location",…}`);
  response `[]` (the array OwnTracks expects); stored under `owntracks/dt=…/`.

The on-disk files are a durable **staging** layer for a downstream consumer (a
bronze/medallion promoter): the body is stored byte-exact, and the server-stamped
receipt time is carried in the filename (`received_unix_ms`), never injected into
the payload.

Both authenticate with the **same** `LOCATIONRELAY_TOKEN` (Bearer or query-token;
OwnTracks iOS sends it as an `Authorization: Bearer` custom header).

Optionally it also **relays** each payload onward to a Dawarich instance via a
decoupled background worker: Overland batches → `/api/v1/overland/batches`,
OwnTracks messages → `/api/v1/owntracks/points`, both with the same reused
`LOCATIONRELAY_DAWARICH_TOKEN`. Forwarding is off unless
`LOCATIONRELAY_DAWARICH_URL` + `LOCATIONRELAY_DAWARICH_TOKEN` are both set; when
off the service is byte-for-byte the original pure disk sink.

## Tech Stack
- Language: Rust (edition 2024, pinned to 1.94 via `rust-toolchain.toml`)
- HTTP: axum 0.8 (hyper / tokio)
- Middleware: tower / tower-http (timeout, catch-panic, trace), tower_governor (rate limit)
- Crypto: subtle (constant-time token comparison)
- Outbound HTTP (Dawarich relay): reqwest (rustls/aws-lc-rs TLS, no default features)
- Storage: append-only NDJSON on the local filesystem

## Commands
- `cargo run` — start the server (reads `.env` via dotenvy)
- `cargo build --release` — production build
- `cargo test` — run the integration test suite
- `cargo clippy --all-targets -- -D warnings` — lint (CI treats warnings as errors)
- `cargo fmt --all` — format
- `cargo deny check` — supply-chain / license audit

## Architecture
- `src/lib.rs` — `build_app(config, forwarder)` assembles the core router (two ingest routes: `/overland`, `/owntracks`) + middleware (unit-testable) and the `AppState { config, forwarder }` (FromRef-extractable); `apply_rate_limit(base, config)` attaches the governor layer with the black-hole error handler.
- `src/main.rs` — config load, `forwarder::start`, concurrency layer + `apply_rate_limit`, retention sweep, rejection + forward-failure reporters, hand-off to `server::serve`.
- `src/forwarder.rs` — optional outbound relay to Dawarich. `ForwardHandle` (cloneable, `disabled()` when off) exposes `enqueue_overland`/`enqueue_owntracks`, feeding a bounded in-memory queue of `Forward` items; a single background worker POSTs each to its endpoint (`/api/v1/overland/batches` wrapped as `{"locations":…}`, or `/api/v1/owntracks/points` verbatim) with a Bearer header, retry/backoff, no redirects. Decoupled so the inbound handler never blocks.
- `src/server.rs` — hyper accept loop with an HTTP/1 header-read timeout (slowloris bound), `ConnectInfo` injection, graceful shutdown.
- `src/config.rs` — env-driven `Config` with validation (token strength, tolerant bool parsing, etc.); `DawarichConfig` derives **both** endpoints from one base URL and one reused key.
- `src/auth.rs` — constant-time Bearer/query-token middleware (`route_layer`, pre-body). Shared by both ingest routes, one token.
- `src/models.rs` — Overland payload + strict GeoJSON validation; OwnTracks lenient validation (`validate_owntracks`: any `_type` accepted, but any `lat`/`lon` present must be finite + in range).
- `src/storage.rs` — `Stream` (Overland | Owntracks) picks the server-side stream subdir + date partition (`{stream}/dt=YYYY-MM-DD/`); `capture()` writes the raw body verbatim to `{received_unix_ms}_{shortid}.json` via temp-file + atomic rename (`0600` files in `0700` dirs), no lock needed (one unique file per POST); retention prunes whole `dt=YYYY-MM-DD` partitions older than the window (strict-shape match only). Short id via `getrandom`.
- `src/handlers.rs` — `receive_overland` (→ `{"result":"ok"}`, skips empty batches) and `receive_owntracks` (→ `[]`); both POST-only (non-POST black-holed); validate as the accept/reject gate, then persist the **raw body** and enqueue the **parsed** form to the forwarder (after the durable write, never blocking) / `not_found` (no health/status endpoint).
- `src/security.rs` — response security headers.
- `src/error.rs` — non-leaky `AppError` -> HTTP responses.
- `src/observability.rs` — throttled rejection + forward-failure counters/reporters + time/date helpers.
- `tests/integration.rs` — auth, Overland + OwnTracks byte-exact raw capture (verbatim body, no mutation, batch not exploded), empty-batch-skipped, filename/permission shape, headers, method-probe black hole (both routes), retention of `dt=` partitions (router built with forwarding disabled).
- `tests/server.rs` — live serve loop over TCP: happy path (`/overland`), method-probe, slowloris timeout, rate-limit flood black-holed as 404 (not 429).
- `tests/forwarder.rs` — mock Dawarich: Bearer header (no query/body api_key), correct path/method/body for Overland and OwnTracks (verbatim, no envelope), transient retry, no-retry on 4xx, slow-Dawarich decoupling. Plus `config.rs` unit tests for `build_dawarich` validation.

## Security invariants (do not regress)
- Exactly two fixed ingest routes (`/overland`, `/owntracks`); no path/route parameters anywhere (designs out IDOR / traversal). Do not add more inbound surface.
- Each route is registered for `any` method so a non-POST is black-holed identically to an unknown path — **never reintroduce `post(...)`**, which leaks an `Allow` header and fingerprints the route.
- Both routes share the one `LOCATIONRELAY_TOKEN` via the same pre-body auth layer; OwnTracks and Overland must never diverge in auth handling.
- Auth runs **before** the body is read; comparison is constant-time.
- Storage path is derived server-side (fixed stream subdir + UTC date + a `{received_unix_ms}_{shortid}.json` filename) — never from client input; the stored payload is the raw request body byte-for-byte (no mutation/injection/reserialization), so no server metadata or secret can leak into it. Retention only ever deletes whole strict `{stream}/dt=YYYY-MM-DD/` partitions. Writes are temp-file + atomic rename (no partial file under the final name).
- Error bodies are generic; the token is never logged or echoed (including the query-string form).
- Rejections are rate-limited in the logs (aggregated, not per-event at info).
- The rate limiter's rejection must stay a bare `404` (via `apply_rate_limit`'s `error_handler`) — **never let tower_governor emit its default `429 + Retry-After`**, which fingerprints the limiter and leaks its timing, breaking the black-hole property.
- The HTTP/1 header-read timeout (slowloris bound) must stay set; it requires a hyper `Timer` (`TokioTimer`) to arm.
- Dawarich relay invariants: the Dawarich key is sent **Bearer-only** (never a query parameter or body field) for **both** the Overland and OwnTracks endpoints, and is never logged; the same key serves both (do not add a second Dawarich credential); `LOCATIONRELAY_DAWARICH_TOKEN` must differ from `LOCATIONRELAY_TOKEN` (enforced at startup); OwnTracks messages are forwarded **verbatim** (no `{"locations":…}` envelope) to `/api/v1/owntracks/points`; forwarding is **decoupled** and must never block or fail the inbound handler (enqueue is non-blocking `try_send` after the durable write); the outbound client follows **no redirects**; forwarding stays **outbound-only** — never add an inbound route or surface for it.
- Any new dependency must pass `cargo deny check` and keep the tree minimal. (reqwest's rustls/aws-lc-rs needs `cmake` + a C toolchain in the Docker builder stage, and the `CDLA-Permissive-2.0` license allowance in `deny.toml` for the Mozilla root bundle.)
- Keep files < 300 lines and functions < 50 lines.

## Environment Variables
See `README.md` and `.env.example`. Required: `LOCATIONRELAY_TOKEN` (≥24 chars, minimum variety).
Optional: `LOCATIONRELAY_BIND`, `LOCATIONRELAY_DATA_DIR`, `LOCATIONRELAY_MAX_BODY_BYTES`,
`LOCATIONRELAY_REQUEST_TIMEOUT_SECS`, `LOCATIONRELAY_HEADER_TIMEOUT_SECS`,
`LOCATIONRELAY_MAX_CONCURRENCY`, `LOCATIONRELAY_RATE_PER_SECOND`, `LOCATIONRELAY_RATE_BURST`,
`LOCATIONRELAY_RETENTION_DAYS`, `LOCATIONRELAY_TRUST_PROXY`, `LOCATIONRELAY_FSYNC`,
`LOCATIONRELAY_LOG`.
Optional Dawarich relay (forwarding off unless the first two are both set):
`LOCATIONRELAY_DAWARICH_URL`, `LOCATIONRELAY_DAWARICH_TOKEN`,
`LOCATIONRELAY_FORWARD_TIMEOUT_SECS`, `LOCATIONRELAY_FORWARD_QUEUE_CAPACITY`,
`LOCATIONRELAY_FORWARD_MAX_ATTEMPTS`.

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
