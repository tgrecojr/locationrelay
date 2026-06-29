# locationrelay

A deliberately tiny, hardened HTTP receiver for [Overland iOS](https://github.com/aaronpk/Overland-iOS)
location beacons. It authenticates a `POST`, validates the GeoJSON batch, and
appends it to disk as NDJSON. No database, no dynamic routes, no admin surface,
no read-back API.

Optionally, it can also **relay** each batch onward to a
[Dawarich](https://dawarich.app) instance (see
[Relay to Dawarich](#relay-to-dawarich-optional)). Forwarding is off by default;
with it disabled the service behaves exactly as a pure disk sink.

## Why it's shaped this way

The entire externally reachable surface is **one** authenticated route. There is
no health/status endpoint and no path parameters anywhere, so there is nothing to
enumerate or tamper with — IDOR and path traversal are designed out rather than
filtered. Storage filenames are derived server-side from the UTC date; the client
never names a file or record.

```
POST /          -> auth -> validate -> append NDJSON -> {"result":"ok"}
everything else -> 404
```

An auth failure (missing **or** wrong token) returns a `404` that is
byte-identical to hitting an unknown route — same status, empty body, no
`content-type`. The service is a **black hole**: an unauthenticated probe cannot
even confirm that an ingest endpoint exists here.

## Request flow & hardening

Layers run outermost-first, so cheap rejections happen before expensive work:

1. **Rate limit** (per-client) — a flood is dropped before anything else. The
   rejection is a bare `404`, identical to every other miss: the limiter never
   emits a `429` / `Retry-After`, so a flood probe can't fingerprint it or learn
   its timing.
2. **Body-size limit** (`413`) — oversized payloads never reach the parser.
3. **Auth** — constant-time Bearer/query-token check, **before the body is read**.
   Failure returns a black-hole `404` (indistinguishable from an unknown route).
4. **Validate** — strict GeoJSON `Point` + coordinate-range checks (`422`).
5. **Append** — lock-serialized `O_APPEND` write to `data/YYYY-MM-DD.ndjson`,
   mode `0600` (a global write lock prevents concurrent batches interleaving).

Other controls: per-request timeout, an HTTP/1 header-read timeout that drops
slow-header (slowloris) connections, concurrency cap, panic isolation (a panicked
request returns `500`, never crashes the worker), locked-down security headers,
and generic non-leaky error bodies. A non-POST request to `/` is black-holed with
the same bare `404` as any unknown path (no `Allow` header), so method probing
can't fingerprint the ingest route either.

Brute-force attempts do **not** flood the logs: each rejection bumps an atomic
counter and a background task emits at most one summary line per minute
(`rejected N requests in the last 60s`). Per-event detail is `debug`-only.

See [`docs/THREAT_MODEL.md`](docs/THREAT_MODEL.md) for the OWASP Top 10 mapping.

## Configure Overland

In the Overland iOS app:

- **Receiver Endpoint URL**: `https://your-host/` (HTTPS, via your reverse proxy)
- **Access Token**: the value of `LOCATIONRELAY_TOKEN`

Overland sends the token as `Authorization: Bearer <token>`; this service also
accepts `?access_token=<token>` for templated-URL setups.

> ⚠️ **Prefer the Bearer header.** A token in the query string is not logged by
> this service, but a TLS-terminating reverse proxy (nginx/Caddy/Cloudflare) will
> record the **full URL — including `?access_token=` — in its access logs** by
> default, and it persists in client-side URL history. If you must use the query
> form, configure the proxy to strip/scrub the query string from its logs and
> treat any leaked URL as a token compromise (rotate it).

## Relay to Dawarich (optional)

Set `LOCATIONRELAY_DAWARICH_URL` **and** `LOCATIONRELAY_DAWARICH_TOKEN` to also
forward every received batch to a [Dawarich](https://dawarich.app) instance via
its Overland endpoint (`{url}/api/v1/overland/batches`). Leave them unset and the
service stays a pure disk sink — nothing changes.

```
Overland -> locationrelay -> append NDJSON (durable)  ──┐
                                                        └─> queue ─> worker ─> Dawarich
```

Design notes:

- **Decoupled.** Persisting to local NDJSON happens first and is the durable
  source of truth. The batch is then handed to a bounded in-memory queue drained
  by a background worker, so a slow or unavailable Dawarich **never blocks the
  inbound request** from Overland, and the iPhone is never made to retry (which
  would double-write the local file).
- **At-most-once.** The worker retries transient failures (network, timeout,
  `5xx`, `429`) with exponential backoff up to `LOCATIONRELAY_FORWARD_MAX_ATTEMPTS`
  total tries, then drops that batch (it remains on disk). Hard rejections (`4xx`,
  e.g. a bad key) are not retried. If the process restarts or the queue overflows
  during a Dawarich outage, those batches are not auto-forwarded — they stay on
  disk for manual replay.
- **Bearer, never a query parameter.** The Dawarich key is sent as
  `Authorization: Bearer <key>`. Dawarich's docs show `?api_key=`, but its API
  also accepts the Bearer header — using it keeps the key out of Dawarich's
  URL/access logs. The key is never logged here either.
- **Separate credential.** `LOCATIONRELAY_DAWARICH_TOKEN` must differ from the
  inbound `LOCATIONRELAY_TOKEN`; the service refuses to start if they match.
- Failed/dropped forwards are aggregated like rejections — at most one summary
  line per minute (`N batches not forwarded to Dawarich in the last 60s`).

> Is a queue overkill since Dawarich has Sidekiq? Sidekiq only absorbs Dawarich's
> *internal* work after it has accepted a request; it does nothing for the
> network hop between this service and Dawarich (latency, redeploys, outages).
> The queue here is what keeps those conditions off the inbound path — it stays
> lightweight precisely because the disk write already guarantees durability.

## Run locally

```bash
cp .env.example .env        # then edit LOCATIONRELAY_TOKEN
cargo run                   # reads .env automatically
```

## Deployment

This service speaks **plain HTTP on localhost** and expects a reverse proxy
(Caddy / nginx / Cloudflare Tunnel) to terminate TLS and forward to it. The
rate limiter trusts the proxy-set `X-Forwarded-For` header, so only expose the
service through the proxy — never bind it to a public interface directly.

### Docker

```bash
docker build -t locationrelay .
docker run --rm -p 8080:8080 \
  --read-only --tmpfs /tmp \
  --cap-drop=ALL --security-opt=no-new-privileges \
  -e LOCATIONRELAY_TOKEN="$(openssl rand -base64 36)" \
  -v "$PWD/data:/data" \
  locationrelay
```

The image is multi-stage on distroless (`nonroot`, uid 65532). The mounted
`/data` volume must be writable by that uid. The hardening flags above run the
container read-only (only `/data` and an ephemeral `/tmp` are writable), drop all
Linux capabilities, and block privilege escalation — the binary needs none of
them.

### Rate-limit keying: `LOCATIONRELAY_TRUST_PROXY`

The per-client rate limiter needs a key to count requests against. How that key
is derived is the single most important deployment choice, because it decides
whether a client can spoof its way around the limit. There are two modes.

**`false` (default) — key on the TCP peer IP (`PeerIpKeyExtractor`).**
The key is the address of whoever actually opened the socket. It is taken from
the connection, **not** from any header, so a client cannot change its key by
sending `X-Forwarded-For`. This is the safe default.

- *What to expect behind a reverse proxy:* the peer is always the proxy, so
  **all traffic shares one rate-limit bucket.** For a single-phone sink that is
  exactly right — you want one global cap. The trade-off is that you get no
  per-device granularity (irrelevant with one device).
- *What to expect with no proxy (direct exposure):* the peer is the real client,
  so you get genuine per-client limiting for free.
- *Security:* headers can neither evade the limit nor inflate the limiter's
  in-memory key map (a DDoS lever) — the key space is bounded by real source IPs.
- ⚠️ *Shared-bucket caveat behind a proxy:* because the proxy is the only peer,
  the legit phone and **any** attacker arriving through the same proxy share one
  bucket. An attacker who can reach the proxy can therefore consume the rate
  budget and starve the device. For internet-facing deployments, enforce
  per-real-client-IP rate limiting **at the proxy** (where the true source IP is
  known) in addition to this in-process cap.

**`true` — key on the forwarded header (`SmartIpKeyExtractor`).**
The key is read from `X-Forwarded-For` / `X-Real-IP` / `Forwarded`, falling back
to the peer IP. Use this **only** when you are behind a proxy and want true
per-device limiting *and* you trust the proxy.

- *What to expect:* each distinct client IP (as reported by the proxy) gets its
  own bucket, so one noisy device can be throttled without affecting others.
- ⚠️ *Requirement:* the proxy **must overwrite or strip** any client-supplied
  forwarded headers. If it passes them through, an attacker can set a fresh
  `X-Forwarded-For` per request to (a) sidestep the per-IP limit entirely and
  (b) grow the limiter's key map unbounded between cleanups — a memory-pressure
  DoS. Never enable this on a directly-exposed port.

Rule of thumb: leave it `false` unless you are behind a hardened proxy that
normalizes forwarded headers and you specifically need per-device limits.

## Environment variables

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `LOCATIONRELAY_TOKEN` | **yes** | — | Shared secret (≥24 chars) |
| `LOCATIONRELAY_BIND` | no | `127.0.0.1:8080` | Listen address |
| `LOCATIONRELAY_DATA_DIR` | no | `./data` | NDJSON output directory |
| `LOCATIONRELAY_MAX_BODY_BYTES` | no | `1048576` | Max request body |
| `LOCATIONRELAY_REQUEST_TIMEOUT_SECS` | no | `15` | Per-request timeout |
| `LOCATIONRELAY_HEADER_TIMEOUT_SECS` | no | `10` | Slow-header (slowloris) timeout |
| `LOCATIONRELAY_MAX_CONCURRENCY` | no | `64` | Max in-flight requests |
| `LOCATIONRELAY_RATE_PER_SECOND` | no | `5` | Sustained per-client rate |
| `LOCATIONRELAY_RATE_BURST` | no | `10` | Per-client burst allowance |
| `LOCATIONRELAY_RETENTION_DAYS` | no | `14` | Prune day-files older than this (`0` = keep forever) |
| `LOCATIONRELAY_TRUST_PROXY` | no | `false` | Rate-limit keying (see below) |
| `LOCATIONRELAY_FSYNC` | no | `true` | fsync each batch before replying |
| `LOCATIONRELAY_DAWARICH_URL` | no | — | Dawarich base URL (http/https); enables forwarding |
| `LOCATIONRELAY_DAWARICH_TOKEN` | no | — | Dawarich API key (Bearer); must differ from `LOCATIONRELAY_TOKEN` |
| `LOCATIONRELAY_FORWARD_TIMEOUT_SECS` | no | `10` | Outbound POST timeout to Dawarich |
| `LOCATIONRELAY_FORWARD_QUEUE_CAPACITY` | no | `256` | Bounded forward queue size |
| `LOCATIONRELAY_FORWARD_MAX_ATTEMPTS` | no | `3` | Total tries per batch before dropping |
| `LOCATIONRELAY_LOG` | no | `locationrelay=info,tower_http=warn` | Tracing filter |

Booleans (`LOCATIONRELAY_FSYNC`, `LOCATIONRELAY_TRUST_PROXY`) accept
`true/false`, `1/0`, `yes/no`, `on/off` (case-insensitive); any other value is a
startup error rather than a silent default. The shared secret must be at least
24 characters with reasonable variety — generate one with `openssl rand -base64 36`.

### Data retention

A background sweep runs at startup and every 6 hours, deleting
`YYYY-MM-DD.ndjson` day-files older than `LOCATIONRELAY_RETENTION_DAYS` (default
14). Only files matching that exact server-generated name are ever considered, so
nothing else in the data directory is touched. Set `0` to disable pruning. This
bounds disk growth for the realistic single-device workload; for a hard ceiling
against a *compromised* token, also size/quota the `/data` volume.

## Development

```bash
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test
cargo deny check        # supply-chain / license audit (needs cargo-deny)
```
