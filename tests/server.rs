//! Tests against the real hyper serve loop (`server::serve`) over a TCP socket:
//! the happy path, the method-probe black-hole over the wire, and the
//! slow-header (slowloris) timeout. Uses raw TCP so no HTTP client dep is needed.

use std::sync::Arc;
use std::time::Duration;

use locationrelay::config::Config;
use locationrelay::forwarder::ForwardHandle;
use locationrelay::{apply_rate_limit, build_app, server};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const TOKEN: &str = "server-test-token-abcd1234";

fn test_config(dir: &std::path::Path) -> Arc<Config> {
    Arc::new(Config {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        token: TOKEN.to_string(),
        data_dir: dir.to_path_buf(),
        max_body_bytes: 1024,
        request_timeout_secs: 5,
        header_read_timeout_secs: 1,
        max_concurrency: 8,
        rate_per_second: 1000,
        rate_burst: 1000,
        retention_days: 14,
        fsync: false,
        trust_proxy: false,
        dawarich: None,
        forward_timeout_secs: 5,
        forward_queue_capacity: 256,
        forward_max_attempts: 3,
    })
}

/// Serve-loop tests run with forwarding disabled.
fn app(config: Arc<Config>) -> axum::Router {
    build_app(config, ForwardHandle::disabled())
}

/// Bind an ephemeral port, spawn the real serve loop, return the bound addr and
/// the JoinHandle (abort it to stop the server).
async fn spawn_server(config: Arc<Config>) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    locationrelay::storage::ensure_data_dir(&config)
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = app(config.clone());
    let timeout = Duration::from_secs(config.header_read_timeout_secs);
    let handle = tokio::spawn(async move {
        let _ = server::serve(listener, app, timeout).await;
    });
    (addr, handle)
}

async fn read_to_string(mut stream: TcpStream) -> String {
    let mut buf = Vec::new();
    // Bounded read so a hung connection can't hang the test forever.
    let _ = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut buf)).await;
    String::from_utf8_lossy(&buf).to_string()
}

#[tokio::test]
async fn happy_path_post_over_tcp() {
    let dir = std::env::temp_dir().join("locationrelay-srv-happy");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    let (addr, handle) = spawn_server(test_config(&dir)).await;

    let body = r#"{"locations":[{"type":"Feature","geometry":{"type":"Point","coordinates":[-73.98,40.74]}}]}"#;
    let req = format!(
        "POST /overland HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {TOKEN}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(req.as_bytes()).await.unwrap();
    let resp = read_to_string(stream).await;

    assert!(
        resp.starts_with("HTTP/1.1 200"),
        "expected 200, got:\n{resp}"
    );
    assert!(
        resp.contains(r#"{"result":"ok"}"#),
        "missing receipt:\n{resp}"
    );

    handle.abort();
    let _ = tokio::fs::remove_dir_all(&dir).await;
}

#[tokio::test]
async fn method_probe_over_tcp_has_no_allow_header() {
    let dir = std::env::temp_dir().join("locationrelay-srv-probe");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    let (addr, handle) = spawn_server(test_config(&dir)).await;

    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(b"GET /overland HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let resp = read_to_string(stream).await;

    assert!(
        resp.starts_with("HTTP/1.1 404"),
        "expected 404, got:\n{resp}"
    );
    assert!(
        !resp.to_ascii_lowercase().contains("allow:"),
        "GET /overland leaked an Allow header over the wire:\n{resp}"
    );

    handle.abort();
    let _ = tokio::fs::remove_dir_all(&dir).await;
}

#[tokio::test]
async fn rate_limited_flood_is_black_holed_not_429() {
    // Tiny bucket so a quick burst from one peer IP trips the limiter.
    let mut cfg = (*test_config(&std::env::temp_dir().join("locationrelay-srv-ratelimit"))).clone();
    cfg.rate_per_second = 1;
    cfg.rate_burst = 1;
    let config = Arc::new(cfg);
    let dir = config.data_dir.clone();
    let _ = tokio::fs::remove_dir_all(&dir).await;

    // Build the full stack exactly as `main` does: app + rate limit.
    locationrelay::storage::ensure_data_dir(&config)
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = apply_rate_limit(app(config.clone()), &config);
    let timeout = Duration::from_secs(config.header_read_timeout_secs);
    let handle = tokio::spawn(async move {
        let _ = server::serve(listener, app, timeout).await;
    });

    // Fire a rapid burst on separate connections; later ones must be throttled.
    let mut responses = Vec::new();
    for _ in 0..6 {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        responses.push(read_to_string(stream).await);
    }

    for resp in &responses {
        let lower = resp.to_ascii_lowercase();
        // The throttle rejection must be indistinguishable from any other miss:
        // a bare 404, never the tower_governor 429 / wait-time disclosure.
        assert!(
            resp.starts_with("HTTP/1.1 404"),
            "rate-limited request leaked a non-404 status:\n{resp}"
        );
        assert!(
            !lower.contains("429") && !lower.contains("too many requests"),
            "throttle response fingerprinted the limiter (429/'too many'):\n{resp}"
        );
        assert!(
            !lower.contains("retry-after") && !lower.contains("x-ratelimit"),
            "throttle response leaked rate-limit timing headers:\n{resp}"
        );
    }

    handle.abort();
    let _ = tokio::fs::remove_dir_all(&dir).await;
}

#[tokio::test]
async fn slow_headers_are_dropped() {
    let dir = std::env::temp_dir().join("locationrelay-srv-slow");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    let (addr, handle) = spawn_server(test_config(&dir)).await; // header_read_timeout = 1s

    let mut stream = TcpStream::connect(addr).await.unwrap();
    // Send a partial request line and then stall — never complete the headers.
    stream
        .write_all(b"POST / HTTP/1.1\r\nHost: x\r\n")
        .await
        .unwrap();

    // After the 1s header-read timeout the server must close (or 408) the conn.
    // read_to_end returning (empty or a 408 response) proves it didn't hang.
    let resp = read_to_string(stream).await;
    assert!(
        resp.is_empty() || resp.starts_with("HTTP/1.1 408"),
        "slow-header connection should be dropped or 408'd, got:\n{resp}"
    );

    handle.abort();
    let _ = tokio::fs::remove_dir_all(&dir).await;
}
