//! Rejection accounting and time helpers.
//!
//! To stop a brute-forcer from flooding the logs (and the disk) we do NOT emit
//! a log line per rejected request at info level. Instead every rejection bumps
//! an atomic counter and a single background task emits at most one aggregated
//! summary line per minute. Per-event detail is available at `debug` level only.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

static REJECTED: AtomicU64 = AtomicU64::new(0);

/// Record a single rejected request (bad token, bad payload, etc.).
pub fn record_rejection() {
    REJECTED.fetch_add(1, Ordering::Relaxed);
}

/// Spawn a task that emits one throttled summary line per minute, only when
/// there is something to report.
pub fn spawn_rejection_reporter() {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            let count = REJECTED.swap(0, Ordering::Relaxed);
            if count > 0 {
                tracing::warn!(rejected = count, "rejected requests in the last 60s");
            }
        }
    });
}

/// Current UTC instant as an RFC 3339 string (server-stamped receipt time).
/// Formatting is effectively infallible, but on the impossible error we log and
/// fall back to the epoch rather than emit an empty `received_at`.
pub fn now_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|e| {
            tracing::error!(error = %e, "failed to format received_at timestamp");
            "1970-01-01T00:00:00Z".to_string()
        })
}

/// Current UTC date as `YYYY-MM-DD`, used to derive the storage filename
/// server-side. The client never influences this value.
pub fn utc_date() -> String {
    format_date(OffsetDateTime::now_utc().date())
}

/// The `YYYY-MM-DD` date `days` days before today (UTC). Day-files with a name
/// lexicographically less than this are older than the retention window — the
/// `YYYY-MM-DD` form sorts chronologically, so a string compare is sufficient.
pub fn cutoff_date(days: u64) -> String {
    let cutoff = OffsetDateTime::now_utc().date() - time::Duration::days(days as i64);
    format_date(cutoff)
}

fn format_date(date: time::Date) -> String {
    format!(
        "{:04}-{:02}-{:02}",
        date.year(),
        u8::from(date.month()),
        date.day()
    )
}
