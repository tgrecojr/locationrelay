//! Raw capture storage.
//!
//! Each accepted POST is written verbatim — byte-for-byte the request body — as a
//! single immutable file, named purely server-side so the client can never
//! influence the path (no IDOR, no traversal):
//!
//! ```text
//! {data_dir}/{stream}/dt={YYYY-MM-DD}/{received_unix_ms}_{shortid}.json
//! ```
//!
//! The file is the exact request body: no field injection, no reserialization,
//! no batch explosion — so it is a faithful record of what the source sent. The
//! server-stamped receipt time lives in the filename (`received_unix_ms`), never
//! inside the payload. Files are mode `0600` inside `0700` directories, written
//! atomically (temp file + rename) so a half-written capture never appears under
//! its final name. A separate downstream process promotes these files to bronze.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use tokio::io::AsyncWriteExt;

use crate::config::Config;
use crate::observability;

/// Which inbound source a capture came from. Fixes the storage subdirectory so
/// the two schemas never share a directory and the path is never client-derived.
#[derive(Clone, Copy)]
pub enum Stream {
    Overland,
    Owntracks,
}

impl Stream {
    /// Fixed, server-controlled subdirectory name for this stream.
    fn dir_name(self) -> &'static str {
        match self {
            Stream::Overland => "overland",
            Stream::Owntracks => "owntracks",
        }
    }
}

/// Create the data directory if needed and lock its permissions to `0700`.
pub async fn ensure_data_dir(config: &Config) -> anyhow::Result<()> {
    ensure_dir_private(&config.data_dir).await?;
    Ok(())
}

/// `create_dir_all(path)` and tighten `path` to `0700`.
async fn ensure_dir_private(path: &Path) -> std::io::Result<()> {
    tokio::fs::create_dir_all(path).await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).await?;
    }
    Ok(())
}

/// Persist one accepted POST body verbatim under `{stream}/dt=<today>/`, and
/// return the path written.
///
/// The payload is the raw request bytes, stored unmodified. The receipt time and
/// stream are encoded in the path (never in the bytes), so no server metadata
/// pollutes the captured body.
pub async fn capture(config: &Config, raw: &[u8], stream: Stream) -> std::io::Result<PathBuf> {
    let (date, received_ms) = observability::receipt_now();

    let stream_dir = config.data_dir.join(stream.dir_name());
    let partition_dir = stream_dir.join(format!("dt={date}"));
    // Both levels are tightened to 0700 (the root already is); the leaf holds the
    // 0600 payload files.
    ensure_dir_private(&stream_dir).await?;
    ensure_dir_private(&partition_dir).await?;

    let shortid = short_id()?;
    let final_path = partition_dir.join(format!("{received_ms}_{shortid}.json"));
    let tmp_path = partition_dir.join(format!(".tmp_{shortid}"));

    // Write to a temp file in the same directory, fsync, then atomically rename so
    // a half-written capture never appears under its final name and the rename
    // stays on one filesystem.
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);

    let write_result = async {
        let mut file = options.open(&tmp_path).await?;
        file.write_all(raw).await?;
        if config.fsync {
            file.sync_data().await?;
        }
        Ok::<(), std::io::Error>(())
    }
    .await;
    if let Err(e) = write_result {
        let _ = tokio::fs::remove_file(&tmp_path).await;
        return Err(e);
    }

    tokio::fs::rename(&tmp_path, &final_path).await?;
    Ok(final_path)
}

/// Six hex chars from three random bytes — a per-file collision guard within the
/// same millisecond, mirroring the bronze writer's short id.
fn short_id() -> std::io::Result<String> {
    let mut buf = [0u8; 3];
    getrandom::fill(&mut buf).map_err(std::io::Error::other)?;
    let mut s = String::with_capacity(6);
    for b in buf {
        let _ = write!(s, "{b:02x}");
    }
    Ok(s)
}

/// Delete capture partition directories older than the retention window. Returns
/// the number of `dt=YYYY-MM-DD` directories removed.
///
/// Only strict `{stream}/dt=YYYY-MM-DD` directories under a known stream subdir
/// are ever considered, so an unexpected file/dir in the tree is never touched.
/// `retention_days == 0` disables pruning entirely.
pub async fn prune_old_files(config: &Config) -> std::io::Result<usize> {
    if config.retention_days == 0 {
        return Ok(0);
    }
    let cutoff = observability::cutoff_date(config.retention_days);
    let mut removed = 0;
    for stream in [Stream::Overland, Stream::Owntracks] {
        let stream_dir = config.data_dir.join(stream.dir_name());
        removed += prune_stream(&stream_dir, &cutoff).await?;
    }
    Ok(removed)
}

/// Prune stale `dt=` partitions under a single stream directory.
async fn prune_stream(stream_dir: &Path, cutoff: &str) -> std::io::Result<usize> {
    let mut entries = match tokio::fs::read_dir(stream_dir).await {
        Ok(entries) => entries,
        // A stream that has never received a POST has no directory yet.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };

    let mut removed = 0;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let Some(date) = path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.strip_prefix("dt="))
        else {
            continue;
        };
        // Only strict `dt=YYYY-MM-DD` partitions older than the cutoff are pruned.
        if !is_day_stem(date) || date >= cutoff {
            continue;
        }
        match tokio::fs::remove_dir_all(&path).await {
            Ok(()) => removed += 1,
            Err(e) => tracing::warn!(dir = date, error = %e, "failed to prune old capture dir"),
        }
    }
    Ok(removed)
}

/// True only for a strict `YYYY-MM-DD` stem (10 chars, digits with dashes at
/// positions 4 and 7) — the exact shape our server-side partitions take.
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
