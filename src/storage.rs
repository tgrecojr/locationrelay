//! Append-only NDJSON storage.
//!
//! The filename is derived purely server-side from the current UTC date, so the
//! client can never influence the path (no IDOR, no path traversal). Each
//! feature is written as one JSON line, augmented with a server-stamped
//! `received_at`. Files are created mode 0600 inside a 0700 data directory.

use std::sync::Arc;

use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;

use crate::config::Config;
use crate::observability;

/// Serializes all appends so concurrent requests can never interleave their
/// writes within a day-file. One iPhone, one process, one data dir means a
/// single global lock is sufficient — and it keeps the handler/state
/// signatures untouched. The critical section is just the open+write+fsync;
/// JSON serialization happens outside it.
static WRITE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Create the data directory if needed and lock its permissions to 0700.
pub async fn ensure_data_dir(config: &Config) -> anyhow::Result<()> {
    tokio::fs::create_dir_all(&config.data_dir).await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = std::fs::Permissions::from_mode(0o700);
        tokio::fs::set_permissions(&config.data_dir, perms).await?;
    }
    Ok(())
}

/// Append a validated batch of features to today's NDJSON file.
pub async fn append(config: &Arc<Config>, features: &[Value]) -> std::io::Result<()> {
    if features.is_empty() {
        return Ok(());
    }

    let received_at = observability::now_rfc3339();
    let mut buffer = String::new();
    for feature in features {
        let mut record = feature.clone();
        if let Value::Object(map) = &mut record {
            map.insert("received_at".to_string(), json!(received_at));
        }
        buffer.push_str(&serde_json::to_string(&record)?);
        buffer.push('\n');
    }

    let path = config
        .data_dir
        .join(format!("{}.ndjson", observability::utc_date()));
    let mut options = tokio::fs::OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    options.mode(0o600);

    // Hold the lock across open+write+fsync so two batches can never interleave
    // their lines within the same file.
    let _guard = WRITE_LOCK.lock().await;
    let mut file = options.open(&path).await?;
    file.write_all(buffer.as_bytes()).await?;
    if config.fsync {
        file.sync_data().await?;
    }
    Ok(())
}

/// Delete day-files older than the retention window. Returns the number removed.
///
/// Only files whose name is exactly `YYYY-MM-DD.ndjson` are ever considered, so
/// an unexpected file in the data dir is never touched. `retention_days == 0`
/// disables pruning entirely.
pub async fn prune_old_files(config: &Config) -> std::io::Result<usize> {
    if config.retention_days == 0 {
        return Ok(0);
    }
    let cutoff = observability::cutoff_date(config.retention_days);

    let mut removed = 0;
    let mut entries = tokio::fs::read_dir(&config.data_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let Some(stem) = path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.strip_suffix(".ndjson"))
        else {
            continue;
        };
        if !is_day_stem(stem) || stem >= cutoff.as_str() {
            continue;
        }
        match tokio::fs::remove_file(&path).await {
            Ok(()) => removed += 1,
            Err(e) => tracing::warn!(file = stem, error = %e, "failed to prune old data file"),
        }
    }
    Ok(removed)
}

/// True only for a strict `YYYY-MM-DD` stem (10 chars, digits with dashes at
/// positions 4 and 7) — the exact shape our server-side filenames take.
fn is_day_stem(stem: &str) -> bool {
    let b = stem.as_bytes();
    b.len() == 10
        && b[4] == b'-'
        && b[7] == b'-'
        && b.iter().enumerate().all(|(i, c)| {
            if i == 4 || i == 7 {
                *c == b'-'
            } else {
                c.is_ascii_digit()
            }
        })
}
