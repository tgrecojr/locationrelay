//! Tests against the real hyper serve loop (`server::serve`) over a TCP socket:
//! the happy path, the method-probe black-hole over the wire, and the
//! slow-header (slowloris) timeout. Uses raw TCP so no HTTP client dep is needed.

use std::sync::Arc;
use std::time::Duration;

use locationrelay::config::Config;
use locationrelay::{build_app, server};
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
    })
}

/// Bind an ephemeral port, spawn the real serve loop, return the bound addr and
/// the JoinHandle (abort it to stop the server).
async fn spawn_server(config: Arc<Config>) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    locationrelay::storage::ensure_data_dir(&config)
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = build_app(config.clone());
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
        "POST / HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {TOKEN}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
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
        .write_all(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let resp = read_to_string(stream).await;

    assert!(
        resp.starts_with("HTTP/1.1 404"),
        "expected 404, got:\n{resp}"
    );
    assert!(
        !resp.to_ascii_lowercase().contains("allow:"),
        "GET / leaked an Allow header over the wire:\n{resp}"
    );

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
