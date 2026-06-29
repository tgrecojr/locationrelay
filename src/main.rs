//! Binary entry point: load config, wire up rate limiting + concurrency cap,
//! and serve with graceful shutdown.

use std::sync::Arc;
use std::time::Duration;

use locationrelay::config::Config;
use locationrelay::{apply_rate_limit, build_app, forwarder, observability, server, storage};
use tokio::net::TcpListener;
use tower::limit::ConcurrencyLimitLayer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();
    init_tracing();

    let config = Arc::new(Config::from_env()?);
    storage::ensure_data_dir(&config).await?;
    observability::spawn_rejection_reporter();
    observability::spawn_forward_failure_reporter();
    spawn_retention(config.clone());

    // Optional outbound relay to Dawarich. Disabled (a no-op handle) unless a
    // Dawarich URL is configured.
    let forwarder = forwarder::start(&config);

    // Per-client rate limiting (keyed per `apply_rate_limit`). A flood is
    // black-holed as a bare 404 at the outermost layer, before auth even runs.
    let base = build_app(config.clone(), forwarder)
        .layer(ConcurrencyLimitLayer::new(config.max_concurrency));
    let app = apply_rate_limit(base, &config);

    let listener = TcpListener::bind(config.bind_addr).await?;
    tracing::info!(addr = %config.bind_addr, "locationrelay listening");

    let header_timeout = Duration::from_secs(config.header_read_timeout_secs);
    server::serve(listener, app, header_timeout).await
}

/// Periodically prune day-files older than the retention window (default 14
/// days). Runs once at startup and then every 6 hours.
fn spawn_retention(config: Arc<Config>) {
    if config.retention_days == 0 {
        tracing::info!("data retention disabled (LOCATIONRELAY_RETENTION_DAYS=0)");
        return;
    }
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(6 * 3600));
        loop {
            interval.tick().await;
            match storage::prune_old_files(&config).await {
                Ok(n) if n > 0 => {
                    tracing::info!(
                        removed = n,
                        retention_days = config.retention_days,
                        "pruned old data files"
                    );
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "data retention sweep failed"),
            }
        }
    });
}

fn init_tracing() {
    use tracing_subscriber::{EnvFilter, fmt};
    let filter = EnvFilter::try_from_env("LOCATIONRELAY_LOG")
        .unwrap_or_else(|_| EnvFilter::new("locationrelay=info,tower_http=warn"));
    fmt().with_env_filter(filter).init();
}
