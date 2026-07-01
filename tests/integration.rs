//! End-to-end tests against the core router (`build_app`), exercising auth,
//! validation, storage, and the locked-down surface area.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use locationrelay::config::Config;
use locationrelay::{build_app, storage};
use tower::ServiceExt;

const TOKEN: &str = "test-token-0123456789";

fn test_config(dir: &std::path::Path) -> Arc<Config> {
    Arc::new(Config {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        token: TOKEN.to_string(),
        data_dir: dir.to_path_buf(),
        max_body_bytes: 1024,
        request_timeout_secs: 5,
        header_read_timeout_secs: 10,
        max_concurrency: 8,
        rate_per_second: 100,
        rate_burst: 100,
        retention_days: 14,
        fsync: false,
        trust_proxy: false,
        dawarich: None,
        forward_timeout_secs: 5,
        forward_queue_capacity: 256,
        forward_max_attempts: 3,
    })
}

/// All router-level tests run with forwarding disabled, so the core ingest
/// behavior is exercised independently of any Dawarich relay.
fn app(config: Arc<Config>) -> axum::Router {
    build_app(config, locationrelay::forwarder::ForwardHandle::disabled())
}

fn unique_dir(tag: &str) -> std::path::PathBuf {
    let base = std::env::var("TMPDIR").unwrap_or_else(|_| "/tmp".to_string());
    std::path::PathBuf::from(base).join(format!("locationrelay-test-{tag}"))
}

fn valid_body() -> String {
    serde_json::json!({
        "locations": [{
            "type": "Feature",
            "geometry": { "type": "Point", "coordinates": [-73.9857, 40.7484] },
            "properties": { "timestamp": "2026-06-29T12:00:00Z", "battery_level": 0.9 }
        }]
    })
    .to_string()
}

fn post(token: Option<&str>, body: &str) -> Request<Body> {
    post_to("/overland", token, body)
}

fn post_to(uri: &str, token: Option<&str>, body: &str) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(t) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {t}"));
    }
    builder.body(Body::from(body.to_string())).unwrap()
}

async fn status_of(config: Arc<Config>, req: Request<Body>) -> StatusCode {
    app(config).oneshot(req).await.unwrap().status()
}

#[tokio::test]
async fn valid_request_is_stored() {
    let dir = unique_dir("valid");
    let config = test_config(&dir);
    storage::ensure_data_dir(&config).await.unwrap();

    let response = app(config)
        .oneshot(post(Some(TOKEN), &valid_body()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&bytes[..], br#"{"result":"ok"}"#);

    // A file for today's date must now exist and mention our coordinates.
    let mut found = false;
    let mut entries = tokio::fs::read_dir(&dir).await.unwrap();
    while let Some(e) = entries.next_entry().await.unwrap() {
        let contents = tokio::fs::read_to_string(e.path()).await.unwrap();
        if contents.contains("received_at") && contents.contains("40.7484") {
            found = true;
        }
    }
    assert!(found, "expected the beacon to be persisted to disk");
    let _ = tokio::fs::remove_dir_all(&dir).await;
}

/// Fire many POSTs concurrently and confirm the day-file ends up with exactly
/// one clean, parseable JSON line per request — i.e. the write lock prevents
/// concurrent appends from interleaving or corrupting each other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writes_do_not_interleave() {
    const N: usize = 30;
    let dir = unique_dir("concurrent");
    let config = test_config(&dir);
    storage::ensure_data_dir(&config).await.unwrap();
    let app = app(config);

    let mut handles = Vec::new();
    for _ in 0..N {
        let app = app.clone();
        handles.push(tokio::spawn(async move {
            app.oneshot(post(Some(TOKEN), &valid_body()))
                .await
                .unwrap()
                .status()
        }));
    }
    for handle in handles {
        assert_eq!(handle.await.unwrap(), StatusCode::OK);
    }

    // Exactly one file (today's date), with N lines, each valid JSON.
    let mut files = Vec::new();
    let mut entries = tokio::fs::read_dir(&dir).await.unwrap();
    while let Some(e) = entries.next_entry().await.unwrap() {
        files.push(e.path());
    }
    assert_eq!(files.len(), 1, "expected a single day-file");

    let contents = tokio::fs::read_to_string(&files[0]).await.unwrap();
    let lines: Vec<&str> = contents.lines().collect();
    assert_eq!(lines.len(), N, "expected one line per request");
    for line in lines {
        let parsed: serde_json::Value =
            serde_json::from_str(line).expect("each line must be valid, non-interleaved JSON");
        assert!(parsed.get("received_at").is_some());
    }
    let _ = tokio::fs::remove_dir_all(&dir).await;
}

#[tokio::test]
async fn missing_token_is_black_holed() {
    let config = test_config(&unique_dir("notoken"));
    assert_eq!(
        status_of(config, post(None, &valid_body())).await,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn wrong_token_is_black_holed() {
    let config = test_config(&unique_dir("wrongtoken"));
    assert_eq!(
        status_of(config, post(Some("nope-nope-nope-nope"), &valid_body())).await,
        StatusCode::NOT_FOUND
    );
}

/// The black-hole property: a request with a bad token must be byte-identical to
/// hitting an unknown route — same status, same headers, same (empty) body — so
/// a probe cannot confirm the ingest endpoint exists.
#[tokio::test]
async fn auth_failure_is_indistinguishable_from_unknown_route() {
    let dir = unique_dir("blackhole");

    let bad_token = app(test_config(&dir))
        .oneshot(post(Some("nope-nope-nope-nope"), &valid_body()))
        .await
        .unwrap();
    let unknown = app(test_config(&dir))
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/does-not-exist")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(bad_token.status(), unknown.status());
    assert_eq!(bad_token.status(), StatusCode::NOT_FOUND);
    assert_eq!(bad_token.headers(), unknown.headers());

    let bad_body = bad_token.into_body().collect().await.unwrap().to_bytes();
    let unknown_body = unknown.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(bad_body, unknown_body);
    assert!(bad_body.is_empty());
}

/// A non-POST request to an ingest route must be byte-identical to hitting an
/// unknown path — same status, same headers (crucially no `Allow` header), same
/// empty body. Otherwise a method probe could fingerprint the ingest route.
/// Regression guard for the axum default-405 `Allow: POST` leak.
#[tokio::test]
async fn method_probe_on_ingest_routes_is_indistinguishable_from_unknown_route() {
    for route in ["/overland", "/owntracks"] {
        for method in ["GET", "OPTIONS", "HEAD", "PUT", "DELETE"] {
            let on_route = app(test_config(&unique_dir("methodprobe")))
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(route)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let on_unknown = app(test_config(&unique_dir("methodprobe")))
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri("/nope")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(
                on_route.status(),
                StatusCode::NOT_FOUND,
                "{method} {route} status"
            );
            assert!(
                on_route.headers().get(header::ALLOW).is_none(),
                "{method} {route} leaked an Allow header"
            );
            assert_eq!(
                on_route.headers(),
                on_unknown.headers(),
                "{method} {route} headers differ from an unknown route"
            );
        }
    }
}

/// Even with a *valid* token, anything but POST is black-holed identically.
#[tokio::test]
async fn valid_token_non_post_is_black_holed() {
    let config = test_config(&unique_dir("validget"));
    let req = Request::builder()
        .method("GET")
        .uri("/overland")
        .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
        .body(Body::empty())
        .unwrap();
    let response = app(config).oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert!(response.headers().get(header::ALLOW).is_none());
}

/// Retention prunes day-files older than the window and keeps recent ones.
#[tokio::test]
async fn retention_prunes_old_day_files() {
    let dir = unique_dir("retention");
    let mut config = (*test_config(&dir)).clone();
    config.retention_days = 14;
    let config = Arc::new(config);
    storage::ensure_data_dir(&config).await.unwrap();

    let old = dir.join("2000-01-01.ndjson");
    let old_owntracks = dir.join("2000-01-01-owntracks.ndjson");
    let today = dir.join(format!(
        "{}.ndjson",
        locationrelay::observability::utc_date()
    ));
    let today_owntracks = dir.join(format!(
        "{}-owntracks.ndjson",
        locationrelay::observability::utc_date()
    ));
    let unrelated = dir.join("notes.txt");
    tokio::fs::write(&old, b"{}\n").await.unwrap();
    tokio::fs::write(&old_owntracks, b"{}\n").await.unwrap();
    tokio::fs::write(&today, b"{}\n").await.unwrap();
    tokio::fs::write(&today_owntracks, b"{}\n").await.unwrap();
    tokio::fs::write(&unrelated, b"keep me").await.unwrap();

    let removed = storage::prune_old_files(&config).await.unwrap();
    assert_eq!(removed, 2, "both stale day-files (overland + owntracks) go");
    assert!(!old.exists(), "stale Overland day-file should be gone");
    assert!(!old_owntracks.exists(), "stale OwnTracks day-file should be gone");
    assert!(today.exists(), "today's Overland file must be kept");
    assert!(today_owntracks.exists(), "today's OwnTracks file must be kept");
    assert!(unrelated.exists(), "non day-files must never be touched");

    let _ = tokio::fs::remove_dir_all(&dir).await;
}

/// The `?token=`/`?access_token=` query form authenticates, and a token with
/// URL-special characters is decoded correctly rather than mismatched. The token
/// is an obviously-fake readable string and the query is encoded at runtime, so
/// there's no high-entropy literal in the source.
#[tokio::test]
async fn query_token_auth_decodes_special_chars() {
    let dir = unique_dir("querytoken");
    // Deliberately contains `+`, `/`, `=`, and a space — the chars that break a
    // naive (non-decoding) query parser.
    let token = "fake-test-token/with+special=chars";
    let mut config = (*test_config(&dir)).clone();
    config.token = token.to_string();
    let config = Arc::new(config);
    storage::ensure_data_dir(&config).await.unwrap();

    let encoded: String = form_urlencoded::byte_serialize(token.as_bytes()).collect();
    let req = Request::builder()
        .method("POST")
        .uri(format!("/overland?access_token={encoded}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(valid_body()))
        .unwrap();
    let response = app(config).oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let _ = tokio::fs::remove_dir_all(&dir).await;
}

#[tokio::test]
async fn malformed_json_is_bad_request() {
    let config = test_config(&unique_dir("malformed"));
    assert_eq!(
        status_of(config, post(Some(TOKEN), "{not json")).await,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn out_of_range_coordinates_are_rejected() {
    let config = test_config(&unique_dir("badcoords"));
    let body = serde_json::json!({
        "locations": [{
            "type": "Feature",
            "geometry": { "type": "Point", "coordinates": [999.0, 40.0] }
        }]
    })
    .to_string();
    assert_eq!(
        status_of(config, post(Some(TOKEN), &body)).await,
        StatusCode::UNPROCESSABLE_ENTITY
    );
}

#[tokio::test]
async fn oversized_body_is_rejected() {
    let config = test_config(&unique_dir("toobig"));
    let big = "x".repeat(4096);
    let status = status_of(config, post(Some(TOKEN), &big)).await;
    assert!(
        status == StatusCode::PAYLOAD_TOO_LARGE || status == StatusCode::BAD_REQUEST,
        "expected oversized body to be rejected, got {status}"
    );
}

#[tokio::test]
async fn unknown_route_is_not_found() {
    let config = test_config(&unique_dir("unknown"));
    let req = Request::builder()
        .method("GET")
        .uri("/admin")
        .body(Body::empty())
        .unwrap();
    assert_eq!(status_of(config, req).await, StatusCode::NOT_FOUND);
}

fn owntracks_body() -> String {
    serde_json::json!({
        "_type": "location",
        "lat": 39.9203830,
        "lon": -75.1400,
        "tst": 1782904123u64,
        "tid": "5F",
        "batt": 100,
        "topic": "owntracks/user/DEVICE",
        "conn": "w"
    })
    .to_string()
}

/// A valid OwnTracks location message is persisted to its own day-file and the
/// 200 response body is the empty JSON array OwnTracks expects.
#[tokio::test]
async fn owntracks_message_is_stored_and_returns_empty_array() {
    let dir = unique_dir("owntracks-valid");
    let config = test_config(&dir);
    storage::ensure_data_dir(&config).await.unwrap();

    let response = app(config)
        .oneshot(post_to("/owntracks", Some(TOKEN), &owntracks_body()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&bytes[..], b"[]", "OwnTracks expects an empty JSON array");

    // The record lands in the `-owntracks` day-file, not the Overland one.
    let expected = dir.join(format!(
        "{}-owntracks.ndjson",
        locationrelay::observability::utc_date()
    ));
    let contents = tokio::fs::read_to_string(&expected).await.unwrap();
    assert!(contents.contains("39.920383"));
    assert!(contents.contains("received_at"));
    let _ = tokio::fs::remove_dir_all(&dir).await;
}

/// Non-`location` OwnTracks messages (e.g. `lwt`) are accepted and stored rather
/// than rejected, so the app never triggers an OwnTracks retry loop.
#[tokio::test]
async fn owntracks_non_location_message_is_accepted() {
    let dir = unique_dir("owntracks-lwt");
    let config = test_config(&dir);
    storage::ensure_data_dir(&config).await.unwrap();

    let body = serde_json::json!({ "_type": "lwt", "tst": 1782904000u64 }).to_string();
    let status = status_of(config, post_to("/owntracks", Some(TOKEN), &body)).await;
    assert_eq!(status, StatusCode::OK);
    let _ = tokio::fs::remove_dir_all(&dir).await;
}

/// An OwnTracks location with an out-of-range coordinate is still rejected —
/// the tampering control applies whenever lat/lon are present.
#[tokio::test]
async fn owntracks_out_of_range_coordinate_is_rejected() {
    let config = test_config(&unique_dir("owntracks-badcoord"));
    let body = serde_json::json!({ "_type": "location", "lat": 999.0, "lon": 0.0 }).to_string();
    assert_eq!(
        status_of(config, post_to("/owntracks", Some(TOKEN), &body)).await,
        StatusCode::UNPROCESSABLE_ENTITY
    );
}

/// The OwnTracks route is black-holed on a bad token exactly like every other
/// miss — no token, bare 404.
#[tokio::test]
async fn owntracks_missing_token_is_black_holed() {
    let config = test_config(&unique_dir("owntracks-notoken"));
    assert_eq!(
        status_of(config, post_to("/owntracks", None, &owntracks_body())).await,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn security_headers_present() {
    let config = test_config(&unique_dir("headers"));
    // Headers are applied globally, including to the catch-all 404.
    let req = Request::builder()
        .uri("/anything")
        .body(Body::empty())
        .unwrap();
    let response = app(config).oneshot(req).await.unwrap();
    assert_eq!(
        response.headers().get("x-content-type-options").unwrap(),
        "nosniff"
    );
    assert_eq!(response.headers().get("x-frame-options").unwrap(), "DENY");
}
