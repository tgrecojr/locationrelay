//! Overland payload shape and strict validation.
//!
//! We accept the documented Overland batch envelope and validate every feature
//! before it is written. Validation is a tampering / injection control: only
//! well-formed GeoJSON `Point` features with in-range coordinates reach disk.
//! Unknown extra properties on a feature are preserved (Overland evolves its
//! schema), but the overall structure must be sound.

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
