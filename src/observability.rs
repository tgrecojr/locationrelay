//! Rejection accounting and time helpers.
//!
//! To stop a brute-forcer from flooding the logs (and the disk) we do NOT emit
//! a log line per rejected request at info level. Instead every rejection bumps
//! an atomic counter and a single background task emits at most one aggregated
//! summary line per minute. Per-event detail is available at `debug` level only.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use time::OffsetDateTime;

static REJECTED: AtomicU64 = AtomicU64::new(0);
static FORWARD_FAILED: AtomicU64 = AtomicU64::new(0);

/// Record a single rejected request (bad token, bad payload, etc.).
pub fn record_rejection() {
    REJECTED.fetch_add(1, Ordering::Relaxed);
}

/// Record a single batch that could not be forwarded to Dawarich (queue
/// overflow, exhausted retries, or a hard rejection). Aggregated like
/// rejections so a Dawarich outage can't flood the logs.
pub fn record_forward_failure() {
    FORWARD_FAILED.fetch_add(1, Ordering::Relaxed);
}

/// Spawn a task that emits one throttled summary line per minute, only when
/// there is something to report.
pub fn spawn_rejection_reporter() {
    spawn_counter_reporter(&REJECTED, "rejected requests in the last 60s");
}

/// Spawn the equivalent throttled reporter for failed Dawarich forwards.
pub fn spawn_forward_failure_reporter() {
    spawn_counter_reporter(
        &FORWARD_FAILED,
        "batches not forwarded to Dawarich in the last 60s (still on disk)",
    );
}

/// Emit at most one aggregated `warn` line per minute for a counter, resetting
/// it each tick, and only when there is something to report.
fn spawn_counter_reporter(counter: &'static AtomicU64, message: &'static str) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            let count = counter.swap(0, Ordering::Relaxed);
            if count > 0 {
                tracing::warn!(count, "{}", message);
            }
        }
    });
}

/// Server-stamped receipt time as `(YYYY-MM-DD, unix_millis)`. Both halves come
/// from a single instant so the date partition and the millisecond filename can
/// never disagree across a midnight boundary. The client never influences either.
pub fn receipt_now() -> (String, u64) {
    let now = OffsetDateTime::now_utc();
    let millis = (now.unix_timestamp_nanos() / 1_000_000) as u64;
    (format_date(now.date()), millis)
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
