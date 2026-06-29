//! `locationrelay` — a deliberately tiny, hardened receiver for Overland iOS
//! location beacons. It does exactly one thing: authenticate a POST, validate
//! the GeoJSON batch, and append it to disk. Nothing else.

pub mod auth;
pub mod config;
pub mod error;
pub mod handlers;
pub mod models;
pub mod observability;
pub mod security;
pub mod server;
pub mod storage;

use std::sync::Arc;
use std::time::Duration;

use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::any;
use axum::{Router, middleware};
use tower_governor::governor::GovernorConfigBuilder;
use tower_governor::key_extractor::{PeerIpKeyExtractor, SmartIpKeyExtractor};
use tower_governor::{GovernorError, GovernorLayer};
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

use crate::config::Config;

/// Build the core application router: a single authenticated ingest route plus
/// a catch-all 404, wrapped with auth, body-size limit, request timeout,
/// security headers, panic isolation, and tracing. There is no health/status
/// endpoint. Rate limiting and the concurrency cap are applied in `main` (they
/// need per-connection IP info) so this stays unit-testable.
pub fn build_app(config: Arc<Config>) -> Router {
    let timeout = std::time::Duration::from_secs(config.request_timeout_secs);
    let body_limit = config.max_body_bytes;

    Router::new()
        // Register for *any* method, not just POST. axum's default method router
        // answers a method mismatch with `405 + Allow: POST`, and even routed
        // through our 404 fallback it still attaches the `Allow` header — which
        // fingerprints the ingest route to an unauthenticated method probe and
        // breaks the black-hole property. Handling every method ourselves lets
        // `receive` reject non-POST with the exact same bare 404 as an unknown
        // path, while auth still runs (route_layer) before the body is read.
        .route("/", any(handlers::receive))
        // route_layer => auth runs only for the routes defined above, and
        // crucially before the body is read.
        .route_layer(middleware::from_fn_with_state(
            config.clone(),
            auth::require_auth,
        ))
        .fallback(handlers::not_found)
        .layer(DefaultBodyLimit::max(body_limit))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            timeout,
        ))
        .layer(middleware::from_fn(security::security_headers))
        .layer(CatchPanicLayer::new())
        // Record only the path in the request span — never `uri()` in full,
        // which would carry a `?token=` query secret into the logs if debug
        // logging is ever enabled.
        .layer(TraceLayer::new_for_http().make_span_with(
            |request: &axum::http::Request<axum::body::Body>| {
                tracing::debug_span!(
                    "request",
                    method = %request.method(),
                    path = request.uri().path(),
                )
            },
        ))
        .with_state(config)
}

/// Map every rate-limit rejection to a bare 404 — byte-identical to the
/// catch-all `not_found` and to an auth failure.
///
/// tower_governor's default rejection is a `429 Too Many Requests` with a
/// `Too Many Requests! Wait for Ns` body and a `Retry-After`/`x-ratelimit-*`
/// header. That breaks the black-hole property: it fingerprints the limiter,
/// leaks its configured timing, and confirms to a flood probe that its packets
/// are reaching a live, stateful app. A throttled legit client (Overland) only
/// ever treats `200 + {"result":"ok"}` as delivered, so a 404 — like the
/// original 429 — simply makes it retry the batch later: no behavior change.
fn rate_limit_black_hole(_err: GovernorError) -> axum::response::Response {
    StatusCode::NOT_FOUND.into_response()
}

/// Attach per-client rate limiting to `base`.
///
/// Default (`trust_proxy = false`): key on the **TCP peer IP**
/// (`PeerIpKeyExtractor`). Headers cannot influence the key, so an attacker can
/// neither evade the limit nor inflate the limiter's keyed map by rotating
/// `X-Forwarded-For`. Behind a reverse proxy the peer is the proxy, so all
/// traffic shares one bucket — correct for a single-device sink.
///
/// `trust_proxy = true`: key on the proxy-set forwarded header
/// (`SmartIpKeyExtractor`) for genuine per-client limiting. Only safe when the
/// proxy overwrites/strips any client-supplied forwarded headers.
///
/// Both extractors key on `IpAddr`, so the limiter type is identical and
/// `Router::layer` erases both branches back to a plain `Router`. Every
/// rejection is mapped to a bare 404 via [`rate_limit_black_hole`].
pub fn apply_rate_limit(base: Router, config: &Config) -> Router {
    // The cleanup thread is inlined in each branch so the limiter's concrete
    // type is inferred — the two key extractors produce different generic types
    // that are awkward to name explicitly.
    if config.trust_proxy {
        let conf = GovernorConfigBuilder::default()
            .per_second(config.rate_per_second)
            .burst_size(config.rate_burst)
            .key_extractor(SmartIpKeyExtractor)
            .finish()
            .expect("valid rate-limit configuration");
        let limiter = conf.limiter().clone();
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(Duration::from_secs(60));
                limiter.retain_recent();
            }
        });
        base.layer(GovernorLayer::new(conf).error_handler(rate_limit_black_hole))
    } else {
        let conf = GovernorConfigBuilder::default()
            .per_second(config.rate_per_second)
            .burst_size(config.rate_burst)
            .key_extractor(PeerIpKeyExtractor)
            .finish()
            .expect("valid rate-limit configuration");
        let limiter = conf.limiter().clone();
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(Duration::from_secs(60));
                limiter.retain_recent();
            }
        });
        base.layer(GovernorLayer::new(conf).error_handler(rate_limit_black_hole))
    }
}
