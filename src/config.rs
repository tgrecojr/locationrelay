//! Runtime configuration, loaded once from the environment at startup.
//!
//! The auth token is never logged and never echoed in errors.

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, bail};

/// Minimum acceptable shared-secret length. Short tokens are trivially
/// brute-forceable, so we refuse to start with one. A random 32-char token has
/// far more entropy than this floor; the floor only rejects obviously weak ones.
const MIN_TOKEN_LEN: usize = 24;

/// Minimum number of *distinct* bytes a token must contain. Catches degenerate
/// secrets like `aaaaaaaa...` or `12121212...` that pass the length check but
/// have almost no entropy.
const MIN_TOKEN_DISTINCT: usize = 8;

#[derive(Clone)]
pub struct Config {
    pub bind_addr: SocketAddr,
    pub token: String,
    pub data_dir: PathBuf,
    pub max_body_bytes: usize,
    pub request_timeout_secs: u64,
    pub header_read_timeout_secs: u64,
    pub max_concurrency: usize,
    pub rate_per_second: u64,
    pub rate_burst: u32,
    /// Day-files older than this many days are pruned by a background sweep.
    /// `0` disables pruning (keep forever).
    pub retention_days: u64,
    pub fsync: bool,
    pub trust_proxy: bool,
    /// `Some` enables forwarding each received batch to a Dawarich instance.
    /// `None` (the default — Dawarich URL unset) keeps the original behavior:
    /// validate, write to disk, and do nothing else.
    pub dawarich: Option<DawarichConfig>,
    /// Per-request timeout for the outbound POST to Dawarich.
    pub forward_timeout_secs: u64,
    /// Bounded in-memory forward queue size. Overflow drops (data is on disk).
    pub forward_queue_capacity: usize,
    /// Total tries per batch before giving up (transient failures only).
    pub forward_max_attempts: u32,
}

/// Settings for relaying batches onward to a Dawarich instance. The API key is
/// sent as an `Authorization: Bearer` header (never a query parameter), so it
/// stays out of Dawarich's URL/access logs.
#[derive(Clone)]
pub struct DawarichConfig {
    /// Fully-built Overland ingest endpoint (`{base}/api/v1/overland/batches`).
    pub endpoint: String,
    /// Dawarich API key. Never logged. Must differ from the inbound token.
    pub token: String,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let token = require("LOCATIONRELAY_TOKEN")?;
        validate_token(&token)?;

        let bind_addr = or_default("LOCATIONRELAY_BIND", "127.0.0.1:8080")
            .parse()
            .context("LOCATIONRELAY_BIND must be a valid socket address (host:port)")?;

        // Resolved before the struct literal so the `token` field can still move
        // the owned string afterwards.
        let dawarich = parse_dawarich(&token)?;

        let config = Self {
            bind_addr,
            token,
            data_dir: PathBuf::from(or_default("LOCATIONRELAY_DATA_DIR", "./data")),
            max_body_bytes: parse("LOCATIONRELAY_MAX_BODY_BYTES", 1_048_576)?,
            request_timeout_secs: parse("LOCATIONRELAY_REQUEST_TIMEOUT_SECS", 15)?,
            // Bounds slow-header (slowloris) connections: hyper drops a peer that
            // dribbles request headers slower than this. Covers the gap before
            // the per-request timeout layer (which only starts post-parse).
            header_read_timeout_secs: parse("LOCATIONRELAY_HEADER_TIMEOUT_SECS", 10)?,
            max_concurrency: parse("LOCATIONRELAY_MAX_CONCURRENCY", 64)?,
            rate_per_second: parse("LOCATIONRELAY_RATE_PER_SECOND", 5)?,
            rate_burst: parse("LOCATIONRELAY_RATE_BURST", 10)?,
            retention_days: parse("LOCATIONRELAY_RETENTION_DAYS", 14)?,
            fsync: parse_bool("LOCATIONRELAY_FSYNC", true)?,
            // Default false: key rate limiting on the TCP peer IP, which cannot
            // be spoofed via headers. Set true only behind a proxy that
            // overwrites client-supplied forwarded headers.
            trust_proxy: parse_bool("LOCATIONRELAY_TRUST_PROXY", false)?,
            dawarich,
            forward_timeout_secs: parse("LOCATIONRELAY_FORWARD_TIMEOUT_SECS", 10)?,
            forward_queue_capacity: parse("LOCATIONRELAY_FORWARD_QUEUE_CAPACITY", 256)?,
            forward_max_attempts: parse("LOCATIONRELAY_FORWARD_MAX_ATTEMPTS", 3)?,
        };
        Ok(config)
    }
}

/// Reject obviously weak shared secrets at startup (fail-fast). We can't measure
/// true entropy, but we can refuse the degenerate cases: too short, or too few
/// distinct characters.
fn validate_token(token: &str) -> anyhow::Result<()> {
    if token.len() < MIN_TOKEN_LEN {
        bail!("LOCATIONRELAY_TOKEN must be at least {MIN_TOKEN_LEN} characters");
    }
    let distinct = token
        .bytes()
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    if distinct < MIN_TOKEN_DISTINCT {
        bail!(
            "LOCATIONRELAY_TOKEN is too low-variety (needs ≥{MIN_TOKEN_DISTINCT} distinct \
             characters); generate one with e.g. `openssl rand -base64 36`"
        );
    }
    Ok(())
}

/// Build the optional Dawarich forwarding config from the environment.
///
/// Forwarding is opt-in: with neither variable set we return `None` and the
/// service stays a pure disk sink. Setting only one of the pair is a fail-fast
/// misconfiguration. The URL must be `http`/`https`; the Dawarich key must not
/// be the same secret as the inbound token.
fn parse_dawarich(inbound_token: &str) -> anyhow::Result<Option<DawarichConfig>> {
    let url = std::env::var("LOCATIONRELAY_DAWARICH_URL")
        .ok()
        .filter(|v| !v.is_empty());
    let token = std::env::var("LOCATIONRELAY_DAWARICH_TOKEN")
        .ok()
        .filter(|v| !v.is_empty());
    build_dawarich(url, token, inbound_token)
}

/// Pure validation/assembly half of [`parse_dawarich`], separated so it is unit-
/// testable without touching process-wide environment variables.
fn build_dawarich(
    url: Option<String>,
    token: Option<String>,
    inbound_token: &str,
) -> anyhow::Result<Option<DawarichConfig>> {
    let (url, token) = match (url, token) {
        (None, None) => return Ok(None),
        (Some(url), Some(token)) => (url, token),
        (Some(_), None) => bail!(
            "LOCATIONRELAY_DAWARICH_URL is set but LOCATIONRELAY_DAWARICH_TOKEN is not; \
             set both to enable forwarding or neither to disable it"
        ),
        (None, Some(_)) => bail!(
            "LOCATIONRELAY_DAWARICH_TOKEN is set but LOCATIONRELAY_DAWARICH_URL is not; \
             set both to enable forwarding or neither to disable it"
        ),
    };

    if token == inbound_token {
        bail!(
            "LOCATIONRELAY_DAWARICH_TOKEN must differ from LOCATIONRELAY_TOKEN \
             (the inbound and Dawarich credentials must not be reused)"
        );
    }

    let parsed = reqwest::Url::parse(&url)
        .with_context(|| format!("LOCATIONRELAY_DAWARICH_URL is not a valid URL: {url}"))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        bail!(
            "LOCATIONRELAY_DAWARICH_URL must use http or https, got scheme {:?}",
            parsed.scheme()
        );
    }

    // Append the fixed Overland path to the configured base, tolerating an
    // optional trailing slash. The path is fixed server-side, never client input.
    let endpoint = format!("{}/api/v1/overland/batches", url.trim_end_matches('/'));
    Ok(Some(DawarichConfig { endpoint, token }))
}

fn require(key: &str) -> anyhow::Result<String> {
    match std::env::var(key) {
        Ok(v) if !v.is_empty() => Ok(v),
        _ => bail!("required environment variable {key} is not set"),
    }
}

fn or_default(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn parse<T>(key: &str, default: T) -> anyhow::Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match std::env::var(key) {
        Ok(v) if !v.is_empty() => v
            .parse()
            .map_err(|e| anyhow::anyhow!("{key} is invalid: {e}")),
        _ => Ok(default),
    }
}

/// Parse a boolean env var tolerantly (case-insensitive `true/false`, `1/0`,
/// `yes/no`, `on/off`). An unrecognized value is a startup error rather than a
/// silent fallback — so `FSYNC=True` can never quietly disable durability.
fn parse_bool(key: &str, default: bool) -> anyhow::Result<bool> {
    match std::env::var(key) {
        Ok(v) if !v.is_empty() => match v.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => Ok(true),
            "false" | "0" | "no" | "off" => Ok(false),
            other => bail!("{key} must be a boolean (true/false), got {other:?}"),
        },
        _ => Ok(default),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const INBOUND: &str = "inbound-token-0123456789";

    #[test]
    fn forwarding_disabled_when_unset() {
        let result = build_dawarich(None, None, INBOUND).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn builds_endpoint_and_keeps_token() {
        let cfg = build_dawarich(
            Some("https://dawarich.example.com".into()),
            Some("dawarich-key".into()),
            INBOUND,
        )
        .unwrap()
        .expect("forwarding enabled");
        assert_eq!(
            cfg.endpoint,
            "https://dawarich.example.com/api/v1/overland/batches"
        );
        assert_eq!(cfg.token, "dawarich-key");
    }

    #[test]
    fn trailing_slash_in_base_is_tolerated() {
        let cfg = build_dawarich(
            Some("http://localhost:3000/".into()),
            Some("k".into()),
            INBOUND,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            cfg.endpoint,
            "http://localhost:3000/api/v1/overland/batches"
        );
    }

    #[test]
    fn url_without_token_is_rejected() {
        assert!(build_dawarich(Some("https://x.example".into()), None, INBOUND).is_err());
    }

    #[test]
    fn token_without_url_is_rejected() {
        assert!(build_dawarich(None, Some("k".into()), INBOUND).is_err());
    }

    #[test]
    fn dawarich_token_equal_to_inbound_is_rejected() {
        // `.map(|_| ())` drops the Ok value so we don't require `DawarichConfig:
        // Debug` (we intentionally don't derive it — it holds the token).
        let err = build_dawarich(
            Some("https://x.example".into()),
            Some(INBOUND.into()),
            INBOUND,
        )
        .map(|_| ())
        .unwrap_err();
        assert!(err.to_string().contains("must differ"));
    }

    #[test]
    fn non_http_scheme_is_rejected() {
        assert!(build_dawarich(Some("ftp://x.example".into()), Some("k".into()), INBOUND).is_err());
    }
}
