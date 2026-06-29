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

use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::routing::any;
use axum::{Router, middleware};
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
