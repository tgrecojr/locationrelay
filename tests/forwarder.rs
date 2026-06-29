//! Tests for the outbound Dawarich relay: that batches are POSTed to the
//! Overland endpoint with a Bearer header (never an `api_key` query param), that
//! transient failures are retried and hard rejections are not, and that a slow
//! Dawarich never blocks the inbound handler (decoupling).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, Method, Request, StatusCode, Uri, header};
use axum::routing::any;
use locationrelay::config::{Config, DawarichConfig};
use locationrelay::forwarder::{self, ForwardHandle};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::mpsc::{self, UnboundedReceiver};
use tower::ServiceExt;

const INBOUND_TOKEN: &str = "inbound-token-0123456789";
const DAWARICH_TOKEN: &str = "dawarich-secret-key-9876";

/// What the mock Dawarich captured from one inbound request.
struct Captured {
    method: Method,
    path: String,
    query: Option<String>,
    authorization: Option<String>,
    body: Vec<u8>,
}

struct MockState {
    tx: mpsc::UnboundedSender<Captured>,
    calls: Arc<AtomicUsize>,
    /// Status returned on call N (last entry repeats for further calls).
    statuses: Vec<u16>,
}

async fn capture(
    State(state): State<Arc<MockState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    let n = state.calls.fetch_add(1, Ordering::SeqCst);
    let authorization = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let _ = state.tx.send(Captured {
        method,
        path: uri.path().to_string(),
        query: uri.query().map(str::to_string),
        authorization,
        body: body.to_vec(),
    });
    let idx = n.min(state.statuses.len() - 1);
    StatusCode::from_u16(state.statuses[idx]).unwrap()
}

/// Stand up a mock Dawarich on an ephemeral port. Returns the base URL, a
/// receiver of captured requests, and the live call counter.
async fn spawn_mock(statuses: Vec<u16>) -> (String, UnboundedReceiver<Captured>, Arc<AtomicUsize>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let calls = Arc::new(AtomicUsize::new(0));
    let state = Arc::new(MockState {
        tx,
        calls: calls.clone(),
        statuses,
    });
    let app = Router::new()
        .route("/api/v1/overland/batches", any(capture))
        .with_state(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), rx, calls)
}

fn config_for(base_url: &str, max_attempts: u32) -> Arc<Config> {
    Arc::new(Config {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        token: INBOUND_TOKEN.to_string(),
        data_dir: std::env::temp_dir().join("locationrelay-forwarder-test"),
        max_body_bytes: 1 << 20,
        request_timeout_secs: 15,
        header_read_timeout_secs: 10,
        max_concurrency: 8,
        rate_per_second: 1000,
        rate_burst: 1000,
        retention_days: 0,
        fsync: false,
        trust_proxy: false,
        dawarich: Some(DawarichConfig {
            endpoint: format!("{base_url}/api/v1/overland/batches"),
            token: DAWARICH_TOKEN.to_string(),
        }),
        forward_timeout_secs: 2,
        forward_queue_capacity: 256,
        forward_max_attempts: max_attempts,
    })
}

fn sample_batch() -> Arc<Vec<Value>> {
    Arc::new(vec![json!({
        "type": "Feature",
        "geometry": { "type": "Point", "coordinates": [-73.9857, 40.7484] },
        "properties": { "timestamp": "2026-06-29T12:00:00Z" }
    })])
}

async fn recv(rx: &mut UnboundedReceiver<Captured>) -> Captured {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("mock Dawarich did not receive a request in time")
        .expect("mock channel closed")
}

#[tokio::test]
async fn forwards_with_bearer_header_and_no_query() {
    let (base, mut rx, _calls) = spawn_mock(vec![201]).await;
    let handle = forwarder::start(&config_for(&base, 3));
    handle.enqueue(sample_batch());

    let got = recv(&mut rx).await;
    assert_eq!(got.method, Method::POST);
    assert_eq!(got.path, "/api/v1/overland/batches");
    // The key must travel as a Bearer header...
    assert_eq!(
        got.authorization.as_deref(),
        Some(format!("Bearer {DAWARICH_TOKEN}").as_str())
    );
    // ...and never as a query parameter.
    assert!(
        got.query.is_none(),
        "api key must not appear in the query string"
    );
    assert!(
        !String::from_utf8_lossy(&got.body).contains("api_key"),
        "api key must not appear in the body"
    );

    let body: Value = serde_json::from_slice(&got.body).unwrap();
    assert_eq!(body["locations"][0]["geometry"]["type"], "Point");
}

#[tokio::test]
async fn retries_transient_failure_then_succeeds() {
    // First attempt 503 (transient), second 201.
    let (base, mut rx, calls) = spawn_mock(vec![503, 201]).await;
    let handle = forwarder::start(&config_for(&base, 3));
    handle.enqueue(sample_batch());

    let _first = recv(&mut rx).await;
    let _second = recv(&mut rx).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2, "should have retried once");
}

#[tokio::test]
async fn does_not_retry_on_client_error() {
    // 401 is a hard rejection — a bad key won't fix itself, so do not retry.
    let (base, mut rx, calls) = spawn_mock(vec![401]).await;
    let handle = forwarder::start(&config_for(&base, 3));
    handle.enqueue(sample_batch());

    let _first = recv(&mut rx).await;
    // Wait well past the first backoff window; there must be no second attempt.
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1, "4xx must not be retried");
}

#[tokio::test]
async fn slow_dawarich_does_not_block_the_inbound_handler() {
    // A mock that never answers in time would hang a synchronous forward.
    let (base, _rx, _calls) = spawn_mock(vec![201]).await;
    let mut config = config_for(&base, 3);
    // Point the worker at a black hole so any forward attempt stalls; the handler
    // must still return promptly because forwarding is decoupled.
    Arc::get_mut(&mut config).unwrap().dawarich = Some(DawarichConfig {
        endpoint: "http://10.255.255.1:9/api/v1/overland/batches".to_string(),
        token: DAWARICH_TOKEN.to_string(),
    });
    locationrelay::storage::ensure_data_dir(&config)
        .await
        .unwrap();

    let handle: ForwardHandle = forwarder::start(&config);
    let app = locationrelay::build_app(config.clone(), handle);

    let req = Request::builder()
        .method("POST")
        .uri("/")
        .header(header::AUTHORIZATION, format!("Bearer {INBOUND_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({ "locations": *sample_batch() }).to_string(),
        ))
        .unwrap();

    let response = tokio::time::timeout(Duration::from_secs(2), app.oneshot(req))
        .await
        .expect("inbound handler blocked on a slow Dawarich")
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}
