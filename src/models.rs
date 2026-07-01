//! Payload shapes and validation for both supported sources.
//!
//! Overland POSTs a batch envelope of GeoJSON `Point` features; OwnTracks POSTs
//! a single message object. Validation is a tampering / injection control in
//! both cases — only in-range coordinates reach disk. Overland is validated
//! strictly (well-formed GeoJSON `Point`s only); OwnTracks is validated
//! leniently (every `_type` is accepted so nothing is dropped, but any `lat`/
//! `lon` present must be finite and in range). Unknown extra properties are
//! preserved in both, since both schemas evolve.

use serde::Deserialize;
use serde_json::Value;

use crate::error::AppError;
use crate::observability;

/// The top-level body Overland POSTs. `current`/`trip` are accepted but we only
/// persist the `locations` array; unknown top-level keys are ignored.
#[derive(Deserialize)]
pub struct OverlandPayload {
    #[serde(default)]
    pub locations: Vec<Value>,
}

/// Validate the batch and return the list of features to persist.
///
/// Returns `PayloadInvalid` (HTTP 422) if any feature is malformed. An empty
/// batch is valid and simply results in nothing being written.
pub fn validate(payload: OverlandPayload) -> Result<Vec<Value>, AppError> {
    for feature in &payload.locations {
        if let Err(reason) = validate_feature(feature) {
            observability::record_rejection();
            tracing::debug!(reason, "rejected request: invalid feature");
            return Err(AppError::PayloadInvalid);
        }
    }
    Ok(payload.locations)
}

/// Validate a single OwnTracks message.
///
/// OwnTracks HTTP mode POSTs one message object at a time. We accept **every**
/// `_type` (`location`, `transition`, `waypoint`, `lwt`, …) so nothing is
/// dropped on the way to disk or Dawarich, but we still enforce the
/// coordinate-range tampering control whenever `lat`/`lon` are present. The
/// message is returned unchanged for storage and forwarding.
pub fn validate_owntracks(message: Value) -> Result<Value, AppError> {
    if let Err(reason) = validate_owntracks_message(&message) {
        observability::record_rejection();
        tracing::debug!(reason, "rejected request: invalid OwnTracks message");
        return Err(AppError::PayloadInvalid);
    }
    Ok(message)
}

fn validate_owntracks_message(message: &Value) -> Result<(), &'static str> {
    let obj = message.as_object().ok_or("message is not an object")?;
    if let Some(kind) = obj.get("_type")
        && !kind.is_string()
    {
        return Err("_type must be a string");
    }
    validate_optional_coord(obj.get("lat"), -90.0, 90.0, "lat")?;
    validate_optional_coord(obj.get("lon"), -180.0, 180.0, "lon")?;
    Ok(())
}

/// A coordinate is optional (non-`location` messages omit it), but when present
/// it must be a finite number within range — the same injection control the
/// GeoJSON path enforces.
fn validate_optional_coord(
    value: Option<&Value>,
    min: f64,
    max: f64,
    label: &'static str,
) -> Result<(), &'static str> {
    let Some(value) = value else {
        return Ok(());
    };
    let n = value.as_f64().ok_or(match label {
        "lat" => "lat is not a number",
        _ => "lon is not a number",
    })?;
    if !n.is_finite() {
        return Err(match label {
            "lat" => "lat must be finite",
            _ => "lon must be finite",
        });
    }
    if !(min..=max).contains(&n) {
        return Err(match label {
            "lat" => "lat out of range",
            _ => "lon out of range",
        });
    }
    Ok(())
}

fn validate_feature(feature: &Value) -> Result<(), &'static str> {
    let obj = feature.as_object().ok_or("feature is not an object")?;
    let geometry = obj
        .get("geometry")
        .and_then(Value::as_object)
        .ok_or("missing geometry object")?;

    if geometry.get("type").and_then(Value::as_str) != Some("Point") {
        return Err("geometry.type must be Point");
    }

    let coords = geometry
        .get("coordinates")
        .and_then(Value::as_array)
        .ok_or("missing coordinates array")?;
    if coords.len() != 2 {
        return Err("coordinates must have exactly 2 elements");
    }

    let lon = coords[0].as_f64().ok_or("lon is not a number")?;
    let lat = coords[1].as_f64().ok_or("lat is not a number")?;
    if !lon.is_finite() || !lat.is_finite() {
        return Err("coordinates must be finite");
    }
    if !(-180.0..=180.0).contains(&lon) {
        return Err("lon out of range");
    }
    if !(-90.0..=90.0).contains(&lat) {
        return Err("lat out of range");
    }
    Ok(())
}
