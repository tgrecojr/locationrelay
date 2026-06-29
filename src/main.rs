//! Binary entry point: load config, wire up rate limiting + concurrency cap,
//! and serve with graceful shutdown.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use locationrelay::config::Config;
use locationrelay::{build_app, observability, server, storage};
use tokio::net::TcpListener;
use tower::limit::ConcurrencyLimitLayer;
use tower_governor::GovernorLayer;
use tower_governor::governor::GovernorConfigBuilder;
use tower_governor::key_extractor::{PeerIpKeyExtractor, SmartIpKeyExtractor};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();
    init_tracing();

    let config = Arc::new(Config::from_env()?);
    storage::ensure_data_dir(&config).await?;
    observability::spawn_rejection_reporter();
    spawn_retention(config.clone());

    // Per-client rate limiting (keyed per `apply_rate_limit`). A flood is
    // rejected with 429 at the outermost layer, before auth even runs.
    let base = build_app(config.clone()).layer(ConcurrencyLimitLayer::new(config.max_concurrency));
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

/// Attach per-client rate limiting to the router.
///
/// Default (`trust_proxy = false`): key on the **TCP peer IP**
/// (`PeerIpKeyExtractor`). Headers cannot influence the key, so an attacker
/// can neither evade the limit nor inflate the limiter's keyed map by rotating
/// `X-Forwarded-For`. Behind a reverse proxy the peer is the proxy, so all
/// traffic shares one bucket — correct for a single-device sink.
///
/// `trust_proxy = true`: key on the proxy-set forwarded header
/// (`SmartIpKeyExtractor`) for genuine per-client limiting. Only safe when the
/// proxy overwrites/strips any client-supplied forwarded headers.
///
/// Both extractors key on `IpAddr`, so the limiter type is identical and
/// `Router::layer` erases both branches back to a plain `Router`.
fn apply_rate_limit(base: Router, config: &Config) -> Router {
    // The cleanup thread is inlined in each branch so the limiter's concrete
    // type is inferred — the two key extractors produce different generic types
    // that are awkward to name explicitly.
    if config.trust_proxy {
        let conf = GovernorConfigBuilder::default()
            .per_second(config.rate_per_second)
            .burst_size(config.rate_burst)
            .key_extractor(SmartIpKeyExtractor)
            .finish()
            .expect("valid rate-limit configuration");
        let limiter = conf.limiter().clone();
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(Duration::from_secs(60));
                limiter.retain_recent();
            }
        });
        base.layer(GovernorLayer::new(conf))
    } else {
        let conf = GovernorConfigBuilder::default()
            .per_second(config.rate_per_second)
            .burst_size(config.rate_burst)
            .key_extractor(PeerIpKeyExtractor)
            .finish()
            .expect("valid rate-limit configuration");
        let limiter = conf.limiter().clone();
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(Duration::from_secs(60));
                limiter.retain_recent();
            }
        });
        base.layer(GovernorLayer::new(conf))
    }
}

fn init_tracing() {
    use tracing_subscriber::{EnvFilter, fmt};
    let filter = EnvFilter::try_from_env("LOCATIONRELAY_LOG")
        .unwrap_or_else(|_| EnvFilter::new("locationrelay=info,tower_http=warn"));
    fmt().with_env_filter(filter).init();
}
