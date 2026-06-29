//! Route handlers. Exactly one ingest route and a catch-all that 404s
//! everything else — there is no health/status endpoint to probe.

use std::sync::Arc;

use axum::Json;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{Method, StatusCode};
use serde::Serialize;

use crate::config::Config;
use crate::error::AppError;
use crate::models::{self, OverlandPayload};
use crate::storage;

/// The exact response body Overland requires to consider a batch delivered.
#[derive(Serialize)]
pub struct Receipt {
    result: &'static str,
}

/// `POST /` — receive a batch of location beacons and persist them.
///
/// Registered for *any* method so a non-POST never produces an `Allow` header
/// (which would reveal the route). Anything but POST is black-holed with the
/// same bare 404 as an unknown path. The body is taken as raw `Bytes` (already
/// capped by the body-limit layer) so we control parsing and never surface
/// serde's error detail to the client.
pub async fn receive(
    method: Method,
    State(config): State<Arc<Config>>,
    body: Bytes,
) -> Result<Json<Receipt>, AppError> {
    if method != Method::POST {
        return Err(AppError::NotFound);
    }

    let payload: OverlandPayload = serde_json::from_slice(&body).map_err(|_| {
        crate::observability::record_rejection();
        AppError::BadRequest
    })?;

    let features = models::validate(payload)?;

    storage::append(&config, &features).await.map_err(|err| {
        tracing::error!(error = %err, "failed to persist location batch");
        AppError::Internal
    })?;

    Ok(Json(Receipt { result: "ok" }))
}

/// Catch-all for any other method/path.
pub async fn not_found() -> StatusCode {
    StatusCode::NOT_FOUND
}
