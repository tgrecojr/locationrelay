//! Route handlers. Two fixed ingest routes (`/overland`, `/owntracks`) and a
//! catch-all that 404s everything else — there is no health/status endpoint to
//! probe, and no path parameters anywhere.

use std::sync::Arc;

use axum::Json;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{Method, StatusCode};
use serde::Serialize;
use serde_json::Value;

use crate::config::Config;
use crate::error::AppError;
use crate::forwarder::ForwardHandle;
use crate::models::{self, OverlandPayload};
use crate::storage::{self, Stream};

/// The exact response body Overland requires to consider a batch delivered.
#[derive(Serialize)]
pub struct Receipt {
    result: &'static str,
}

/// `POST /overland` — receive an Overland batch of GeoJSON beacons and persist
/// them.
///
/// Registered for *any* method so a non-POST never produces an `Allow` header
/// (which would reveal the route). Anything but POST is black-holed with the
/// same bare 404 as an unknown path. The body is taken as raw `Bytes` (already
/// capped by the body-limit layer) so we control parsing and never surface
/// serde's error detail to the client. The response is the `{"result":"ok"}`
/// receipt Overland expects.
pub async fn receive_overland(
    method: Method,
    State(config): State<Arc<Config>>,
    State(forwarder): State<ForwardHandle>,
    body: Bytes,
) -> Result<Json<Receipt>, AppError> {
    if method != Method::POST {
        return Err(AppError::NotFound);
    }

    let payload: OverlandPayload = serde_json::from_slice(&body).map_err(|_| {
        crate::observability::record_rejection();
        AppError::BadRequest
    })?;

    let features = Arc::new(models::validate(payload)?);

    storage::append(&config, features.as_slice(), Stream::Overland)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to persist location batch");
            AppError::Internal
        })?;

    // Only after the batch is durably on disk do we hand it to the decoupled
    // forwarder (a no-op when forwarding is disabled). This never blocks and a
    // forward failure never changes the response we return to Overland.
    if !features.is_empty() {
        forwarder.enqueue_overland(features);
    }

    Ok(Json(Receipt { result: "ok" }))
}

/// `POST /owntracks` — receive a single OwnTracks message and persist it.
///
/// Same locked-down surface as [`receive_overland`]: non-POST is black-holed,
/// the body is raw `Bytes`, and errors are generic. OwnTracks POSTs one message
/// object at a time and expects a JSON **array** in the 200 response (an empty
/// array means "no friend/command payloads to deliver back"), so that is what we
/// return.
pub async fn receive_owntracks(
    method: Method,
    State(config): State<Arc<Config>>,
    State(forwarder): State<ForwardHandle>,
    body: Bytes,
) -> Result<Json<Vec<Value>>, AppError> {
    if method != Method::POST {
        return Err(AppError::NotFound);
    }

    let message: Value = serde_json::from_slice(&body).map_err(|_| {
        crate::observability::record_rejection();
        AppError::BadRequest
    })?;

    let message = models::validate_owntracks(message)?;

    storage::append(&config, std::slice::from_ref(&message), Stream::Owntracks)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to persist OwnTracks message");
            AppError::Internal
        })?;

    // Durable on disk first, then hand off to the decoupled forwarder.
    forwarder.enqueue_owntracks(Arc::new(message));

    Ok(Json(Vec::new()))
}

/// Catch-all for any other method/path.
pub async fn not_found() -> StatusCode {
    StatusCode::NOT_FOUND
}
