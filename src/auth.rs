//! Bearer / query-token authentication middleware.
//!
//! Runs as a `route_layer` on the ingest route, so it executes *before* the
//! request body is read or deserialized — a wrong or missing token is rejected
//! without touching the parser or the disk. The comparison is constant-time to
//! avoid leaking the secret through timing.

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::header::AUTHORIZATION;
use axum::http::{HeaderMap, Uri};
use axum::middleware::Next;
use axum::response::Response;
use subtle::ConstantTimeEq;

use crate::config::Config;
use crate::error::AppError;
use crate::observability;

pub async fn require_auth(
    State(config): State<Arc<Config>>,
    request: Request,
    next: Next,
) -> Result<Response, AppError> {
    if token_is_valid(&config, request.headers(), request.uri()) {
        Ok(next.run(request).await)
    } else {
        observability::record_rejection();
        tracing::debug!("rejected request: missing or invalid token");
        // Black-hole: indistinguishable from an unknown route, so a scanner
        // never learns the ingest endpoint exists.
        Err(AppError::NotFound)
    }
}

fn token_is_valid(config: &Config, headers: &HeaderMap, uri: &Uri) -> bool {
    if let Some(token) = bearer_token(headers)
        && ct_eq(token, &config.token)
    {
        return true;
    }
    if let Some(token) = query_token(uri)
        && ct_eq(&token, &config.token)
    {
        return true;
    }
    false
}

/// Extract the token from an `Authorization: Bearer <token>` header.
fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .map(str::trim)
}

/// Extract the token from `?access_token=` or `?token=` (Overland supports
/// templated URLs that carry the secret in the query string).
///
/// Decoded with `form_urlencoded` so a percent-encoded or `+`-containing token
/// (e.g. a base64 secret from `openssl rand -base64`) round-trips correctly
/// rather than silently failing to match.
fn query_token(uri: &Uri) -> Option<String> {
    let query = uri.query()?;
    form_urlencoded::parse(query.as_bytes())
        .find(|(key, _)| key == "access_token" || key == "token")
        .map(|(_, value)| value.into_owned())
}

/// Constant-time string comparison. The length check leaks only the length of
/// the secret, which is not sensitive; the byte comparison itself is constant
/// time so equal-length guesses gain no timing signal.
fn ct_eq(candidate: &str, expected: &str) -> bool {
    let a = candidate.as_bytes();
    let b = expected.as_bytes();
    if a.len() != b.len() {
        return false;
    }
    a.ct_eq(b).into()
}
