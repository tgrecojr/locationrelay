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
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let token = require("LOCATIONRELAY_TOKEN")?;
        validate_token(&token)?;

        let bind_addr = or_default("LOCATIONRELAY_BIND", "127.0.0.1:8080")
            .parse()
            .context("LOCATIONRELAY_BIND must be a valid socket address (host:port)")?;

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
