//! Uniform, non-leaky error responses.
//!
//! Bodies are deliberately terse and generic so we never disclose internal
//! state, stack details, or whether a token was "close". Internal failures are
//! logged server-side; everything the client sees is a status code and a short
//! constant string.
//!
//! Auth failures map to `NotFound`, producing a response byte-identical to the
//! catch-all 404 (bare status, empty body, no content-type). An unauthenticated
//! probe therefore cannot tell that an ingest endpoint exists here at all — the
//! whole service looks like a black hole.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

#[derive(Debug)]
pub enum AppError {
    /// Auth failure or unknown route — indistinguishable on the wire.
    NotFound,
    BadRequest,
    PayloadInvalid,
    Internal,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, body) = match self {
            // Return the bare status (no body, no content-type) so this exactly
            // matches the fallback `not_found` handler.
            AppError::NotFound => return StatusCode::NOT_FOUND.into_response(),
            AppError::BadRequest => (StatusCode::BAD_REQUEST, "bad request"),
            AppError::PayloadInvalid => (StatusCode::UNPROCESSABLE_ENTITY, "invalid payload"),
            AppError::Internal => (StatusCode::INTERNAL_SERVER_ERROR, "internal error"),
        };
        (status, body).into_response()
    }
}
