# Threat model & OWASP Top 10 mapping

`locationrelay` is a two-route ingest service (`/overland` and `/owntracks`, one
per source app). Its security posture comes from having almost no surface area,
then hardening the little that remains.

## Trust boundaries
- The phone (Overland and/or OwnTracks) holds a shared bearer token. The **same**
  token authenticates both routes and is the only thing authenticating a writer.
- A TLS-terminating reverse proxy sits in front. The service binds localhost and
  trusts the proxy-set `X-Forwarded-For` for rate-limit keying.
- The filesystem under `LOCATIONRELAY_DATA_DIR` is the only persistence.
- *(Optional)* A downstream **Dawarich** instance. When configured, the service
  forwards each validated payload to the matching Dawarich endpoint under one
  operator-set base URL (`LOCATIONRELAY_DAWARICH_URL`) using a single operator-set
  key (`LOCATIONRELAY_DAWARICH_TOKEN`) for both. Both come from trusted
  environment config, not from any request; an untrusted client cannot influence
  the destination — the fixed endpoint *paths* are compiled in, and only the base
  URL is configurable.

## Assets
- Confidentiality & integrity of stored location history.
- Availability of the ingest endpoint (so the phone's buffer drains).

## OWASP Top 10 (2021) mapping

| ID | Risk | Mitigation in this service |
|----|------|----------------------------|
| A01 | Broken Access Control / IDOR | Two fixed routes, **zero** path/route parameters. Client cannot reference any object or file. Storage filename derived server-side from UTC date (with a fixed per-source suffix). |
| A02 | Cryptographic Failures | TLS terminated at the proxy (transport encryption mandatory in prod). Token compared in constant time (`subtle`) to avoid timing leaks. Token never logged. The optional Dawarich key is sent **only** as an `Authorization: Bearer` header (never in the URL/query/body) to both endpoints, over a rustls-verified connection (cert validation is **not** disabled); it is never logged. |
| A03 | Injection | No SQL, no shell, no template engine, no `eval`. Body parsed by `serde_json`; Overland features validated (GeoJSON `Point`, finite in-range coords) and OwnTracks messages validated (any `lat`/`lon` finite + in range) before write. Output is append-only NDJSON. |
| A04 | Insecure Design | Minimal-by-design: receive-only, no read-back API, no admin surface. Fail-closed (any error => non-2xx so the phone retries; no silent data loss). |
| A05 | Security Misconfiguration | No CORS, no directory listing, no verbose errors. Locked-down security headers. Non-root distroless container, recommended run is read-only with `--cap-drop=ALL --security-opt=no-new-privileges` (only the data volume + `/tmp` writable). Config validated at startup (refuses weak/short token; boolean env vars are strictly parsed, no silent fallback). |
| A06 | Vulnerable & Outdated Components | Minimal dependency tree, pinned `Cargo.lock`, `cargo deny check` in CI (vulnerabilities + unmaintained always denied, plus bans, licenses, sources). |
| A07 | Identification & Auth Failures | Bearer/query token required, constant-time compare, ≥24-char minimum with a minimum-variety check enforced at startup. Auth failures **and any non-POST method on either ingest route** return a `404` byte-identical to an unknown route — same status, empty body, **no `Allow` header** — so neither a token probe nor a method probe can confirm the endpoint exists (black hole). Per-client rate limiting throttles brute force; failures aggregated in logs (no log flooding). |
| A08 | Software & Data Integrity Failures | Reproducible builds via `--locked`. Distroless runtime. No remote code / plugin loading. Records are append-only and server-stamped (`received_at`); a global write lock serializes appends so concurrent batches cannot interleave or corrupt a day-file. Deeply-nested JSON is bounded by `serde_json`'s built-in 128-level recursion limit (no stack exhaustion). |
| A09 | Logging & Monitoring Failures | Structured `tracing`. Secrets and full bodies are never logged. Rejections counted and summarized once per minute (signal without flooding). |
| A10 | Server-Side Request Forgery | The only outbound requests are the optional Dawarich relays, whose **host and protocol come solely from trusted env config** (`LOCATIONRELAY_DAWARICH_URL`, validated `http`/`https` at startup) with fixed compiled-in endpoint paths — never from request input, so a client cannot redirect them. The client follows **no redirects** (`redirect::Policy::none()`), so a malicious 3xx cannot bounce the bearer key to another host. When forwarding is unconfigured the service makes no outbound requests at all. |

## Residual risks / operator responsibilities
- **Token leakage**: a leaked token allows writing junk beacons (still shape- and
  range-validated, rate-limited, size-capped). Rotate the token by changing the
  env var and restarting. Weak/short tokens are rejected at startup (≥24 chars,
  minimum character variety).
- **Query-string token in proxy logs**: the service never logs the token, but a
  TLS-terminating proxy logs full request URLs (including `?access_token=`) by
  default. Prefer the `Authorization: Bearer` header; if you must use the query
  form, scrub the query string from proxy access logs and treat a leaked URL as a
  token compromise. See the README "Configure Overland" / "Configure OwnTracks" notes.
- **At-rest encryption**: NDJSON files are plaintext on disk. Use an encrypted
  volume / full-disk encryption if the host is untrusted.
- **Dawarich relay (optional)**: forwarding is off unless both
  `LOCATIONRELAY_DAWARICH_URL` and `LOCATIONRELAY_DAWARICH_TOKEN` are set, and the
  Dawarich key must differ from the inbound token (enforced at startup). The key
  travels Bearer-header-only and is never logged. Prefer an `https://` Dawarich
  URL — an `http://` URL is accepted but would send the key and location data in
  cleartext, so only use it over a trusted local network. Delivery is
  at-most-once: the local NDJSON write is the source of truth, and a batch dropped
  on a Dawarich outage (queue overflow or restart) remains on disk for replay
  rather than being lost.
- **Disk growth**: a background retention sweep prunes day-files older than
  `LOCATIONRELAY_RETENTION_DAYS` (default 14), bounding growth for the normal
  single-device workload. A *compromised* token can still write at the
  rate/size-capped ingest rate within the window, so also size/quota the data
  volume on internet-facing hosts.
- **Slowloris / slow-header DoS**: the HTTP/1 server enforces a header-read
  timeout (`LOCATIONRELAY_HEADER_TIMEOUT_SECS`, default 10) that drops peers which
  dribble request headers — covering the gap the per-request timeout layer cannot.
  Still provision connection limits at the proxy for internet exposure.
- **Proxy trust / rate-limit keying**: by default (`LOCATIONRELAY_TRUST_PROXY=false`)
  the limiter keys on the **TCP peer IP**, which headers cannot influence — safe
  on any topology. Setting it `true` switches to forwarded-header keying
  (`SmartIpKeyExtractor`) for per-device limits; that is only safe when the proxy
  overwrites client-supplied forwarded headers, since otherwise a client can spoof
  the header to evade per-IP limits and balloon the limiter's key map (memory
  DoS). The `retain_recent()` sweep bounds map growth either way. With the safe
  default behind a proxy, all traffic shares one bucket, so an attacker reaching
  the proxy can consume the rate budget and starve the device — enforce
  per-real-client-IP limiting at the proxy for internet exposure. See the README
  "Rate-limit keying" section.
- **DoS beyond rate limits**: large sustained volume is bounded by the rate
  limit, body cap, concurrency cap, and timeout, but provision proxy-level
  protection (e.g. Cloudflare) for internet-facing deployments.
