//! Accept loop built directly on hyper.
//!
//! We don't use `axum::serve` because it doesn't expose an HTTP/1 header-read
//! timeout — the bound that defeats a slow-header (slowloris) connection, which
//! the per-request `TimeoutLayer` cannot cover (that layer only starts after the
//! head is parsed). We also inject the real TCP peer as `ConnectInfo` so the
//! rate limiter keys on a trustworthy source IP, and drive each request through
//! `oneshot` (which runs `poll_ready` first) so the shared concurrency and
//! rate-limit permits are actually acquired before the request runs.

use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::Request;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::server::graceful::GracefulShutdown;
use tokio::net::TcpListener;
use tower::ServiceExt;

/// How long to wait for in-flight connections to drain after a shutdown signal.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Serve `app` on `listener` until a shutdown signal, dropping any connection
/// that takes longer than `header_timeout` to send its request headers.
pub async fn serve(
    listener: TcpListener,
    app: Router,
    header_timeout: Duration,
) -> anyhow::Result<()> {
    let graceful = GracefulShutdown::new();
    let mut shutdown = std::pin::pin!(shutdown_signal());

    loop {
        let (stream, peer) = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok(conn) => conn,
                Err(e) => {
                    tracing::warn!(error = %e, "accept failed");
                    continue;
                }
            },
            _ = &mut shutdown => break,
        };

        let io = TokioIo::new(stream);
        let app = app.clone();
        let service = hyper::service::service_fn(move |req: Request<Incoming>| {
            let app = app.clone();
            async move {
                let (parts, body) = req.into_parts();
                let mut req = Request::from_parts(parts, Body::new(body));
                req.extensions_mut().insert(ConnectInfo(peer));
                app.oneshot(req).await
            }
        });

        let mut builder = http1::Builder::new();
        // A timer must be installed for header_read_timeout to arm (hyper panics
        // otherwise when the timeout fires).
        builder.timer(TokioTimer::new());
        builder.header_read_timeout(header_timeout);
        let conn = graceful.watch(builder.serve_connection(io, service));
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::debug!(error = %e, "connection closed with error");
            }
        });
    }

    // Stop accepting, then drain in-flight connections within a bounded window.
    drop(listener);
    tokio::select! {
        _ = graceful.shutdown() => tracing::info!("all connections drained"),
        _ = tokio::time::sleep(DRAIN_TIMEOUT) => {
            tracing::warn!("graceful shutdown timed out; exiting");
        }
    }
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("shutdown signal received");
}
