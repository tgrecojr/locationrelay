//! End-to-end tests against the core router (`build_app`), exercising auth,
//! validation, storage, and the locked-down surface area.

use std::path::PathBuf;
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

/// Collect every captured payload file under `{dir}/{stream}/dt=*/`.
async fn capture_files(dir: &std::path::Path, stream: &str) -> Vec<PathBuf> {
    let stream_dir = dir.join(stream);
    let mut out = Vec::new();
    let mut days = match tokio::fs::read_dir(&stream_dir).await {
        Ok(days) => days,
        Err(_) => return out,
    };
    while let Some(day) = days.next_entry().await.unwrap() {
        let mut files = tokio::fs::read_dir(day.path()).await.unwrap();
        while let Some(f) = files.next_entry().await.unwrap() {
            out.push(f.path());
        }
    }
    out
}

#[tokio::test]
async fn valid_request_is_stored_verbatim() {
    let dir = unique_dir("valid");
    let config = test_config(&dir);
    storage::ensure_data_dir(&config).await.unwrap();
    let body = valid_body();

    let response = app(config).oneshot(post(Some(TOKEN), &body)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&bytes[..], br#"{"result":"ok"}"#);

    // Exactly one capture file under overland/dt=<today>/, byte-identical to the
    // request body — no `received_at` injection, no reserialization.
    let files = capture_files(&dir, "overland").await;
    assert_eq!(files.len(), 1, "expected exactly one capture file");
    let stored = tokio::fs::read(&files[0]).await.unwrap();
    assert_eq!(
        stored,
        body.as_bytes(),
        "stored bytes must equal the POST body"
    );
    assert!(
        !String::from_utf8_lossy(&stored).contains("received_at"),
        "the payload must not be mutated with server metadata"
    );
    let _ = tokio::fs::remove_dir_all(&dir).await;
}

/// Fire many POSTs concurrently and confirm each lands as its own intact,
/// byte-exact file — no lock needed, no interleaving, no lost writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writes_each_land_as_own_file() {
    const N: usize = 30;
    let dir = unique_dir("concurrent");
    let config = test_config(&dir);
    storage::ensure_data_dir(&config).await.unwrap();
    let app = app(config);
    let body = valid_body();

    let mut handles = Vec::new();
    for _ in 0..N {
        let app = app.clone();
        let body = body.clone();
        handles.push(tokio::spawn(async move {
            app.oneshot(post(Some(TOKEN), &body))
                .await
                .unwrap()
                .status()
        }));
    }
    for handle in handles {
        assert_eq!(handle.await.unwrap(), StatusCode::OK);
    }

    // N distinct files, each byte-identical to the request body and valid JSON.
    let files = capture_files(&dir, "overland").await;
    assert_eq!(files.len(), N, "expected one capture file per request");
    for path in files {
        let stored = tokio::fs::read(&path).await.unwrap();
        assert_eq!(stored, body.as_bytes(), "each file must be an intact copy");
        serde_json::from_slice::<serde_json::Value>(&stored)
            .expect("each file must be valid, non-interleaved JSON");
    }
    let _ = tokio::fs::remove_dir_all(&dir).await;
}

/// An empty Overland batch is accepted (200) but carries no data, so nothing is
/// written to disk.
#[tokio::test]
async fn empty_overland_batch_is_accepted_but_not_stored() {
    let dir = unique_dir("empty-batch");
    let config = test_config(&dir);
    storage::ensure_data_dir(&config).await.unwrap();

    let body = serde_json::json!({ "locations": [] }).to_string();
    let response = app(config).oneshot(post(Some(TOKEN), &body)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    assert!(
        capture_files(&dir, "overland").await.is_empty(),
        "an empty batch must not be persisted"
    );
    let _ = tokio::fs::remove_dir_all(&dir).await;
}

/// A multi-point batch is stored as ONE file preserving the whole envelope — it
/// is never exploded into per-point records.
#[tokio::test]
async fn multi_point_batch_is_one_file_with_envelope() {
    let dir = unique_dir("multipoint");
    let config = test_config(&dir);
    storage::ensure_data_dir(&config).await.unwrap();

    let feature = serde_json::json!({
        "type": "Feature",
        "geometry": { "type": "Point", "coordinates": [-73.98, 40.74] }
    });
    let body =
        serde_json::json!({ "locations": [feature.clone(), feature.clone(), feature] }).to_string();
    let response = app(config).oneshot(post(Some(TOKEN), &body)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let files = capture_files(&dir, "overland").await;
    assert_eq!(
        files.len(),
        1,
        "a batch must be one file, not one-per-point"
    );
    let stored: serde_json::Value =
        serde_json::from_slice(&tokio::fs::read(&files[0]).await.unwrap()).unwrap();
    assert_eq!(
        stored["locations"].as_array().unwrap().len(),
        3,
        "the batch envelope must be preserved whole"
    );
    let _ = tokio::fs::remove_dir_all(&dir).await;
}

/// The capture filename is server-derived (`{ms}_{6hex}.json`), mode 0600, and no
/// temp remnant is left behind.
#[tokio::test]
async fn capture_filename_and_permissions() {
    let dir = unique_dir("filename");
    let config = test_config(&dir);
    storage::ensure_data_dir(&config).await.unwrap();

    let response = app(config)
        .oneshot(post(Some(TOKEN), &valid_body()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let files = capture_files(&dir, "overland").await;
    assert_eq!(files.len(), 1);
    let name = files[0].file_name().unwrap().to_str().unwrap();
    let (ms, rest) = name.split_once('_').expect("name is {ms}_{id}.json");
    assert!(
        ms.chars().all(|c| c.is_ascii_digit()),
        "ms must be digits: {name}"
    );
    let id = rest.strip_suffix(".json").expect("must end in .json");
    assert_eq!(id.len(), 6, "short id is 6 hex chars: {name}");
    assert!(
        id.chars().all(|c| c.is_ascii_hexdigit()),
        "short id must be hex: {name}"
    );

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = tokio::fs::metadata(&files[0])
            .await
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600, "capture file must be 0600");
    }

    // No leftover temp files in the partition directory.
    let partition = files[0].parent().unwrap();
    let mut entries = tokio::fs::read_dir(partition).await.unwrap();
    while let Some(e) = entries.next_entry().await.unwrap() {
        let n = e.file_name();
        assert!(
            !n.to_string_lossy().starts_with(".tmp"),
            "a temp remnant was left behind: {n:?}"
        );
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

/// Retention prunes `dt=` partition directories older than the window (for both
/// streams) and keeps recent ones; anything not matching the strict shape is left
/// alone.
#[tokio::test]
async fn retention_prunes_old_partition_dirs() {
    let dir = unique_dir("retention");
    let mut config = (*test_config(&dir)).clone();
    config.retention_days = 14;
    let config = Arc::new(config);
    storage::ensure_data_dir(&config).await.unwrap();

    let today = locationrelay::observability::utc_date();
    let old_overland = dir.join("overland/dt=2000-01-01");
    let old_owntracks = dir.join("owntracks/dt=2000-01-01");
    let today_overland = dir.join(format!("overland/dt={today}"));
    let today_owntracks = dir.join(format!("owntracks/dt={today}"));
    // A non-partition directory and a stray file that must never be touched.
    let not_a_partition = dir.join("overland/scratch");
    let unrelated = dir.join("notes.txt");
    for d in [
        &old_overland,
        &old_owntracks,
        &today_overland,
        &today_owntracks,
        &not_a_partition,
    ] {
        tokio::fs::create_dir_all(d).await.unwrap();
        tokio::fs::write(d.join("x.json"), b"{}").await.unwrap();
    }
    tokio::fs::write(&unrelated, b"keep me").await.unwrap();

    let removed = storage::prune_old_files(&config).await.unwrap();
    assert_eq!(
        removed, 2,
        "both stale partitions (overland + owntracks) go"
    );
    assert!(
        !old_overland.exists(),
        "stale Overland partition should be gone"
    );
    assert!(
        !old_owntracks.exists(),
        "stale OwnTracks partition should be gone"
    );
    assert!(
        today_overland.exists(),
        "today's Overland partition must be kept"
    );
    assert!(
        today_owntracks.exists(),
        "today's OwnTracks partition must be kept"
    );
    assert!(
        not_a_partition.exists(),
        "non-`dt=` dirs must never be touched"
    );
    assert!(unrelated.exists(), "unrelated files must never be touched");

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

    // The record lands under `owntracks/dt=<today>/`, not the Overland tree, and
    // is stored verbatim (no `received_at` injection).
    let body = owntracks_body();
    let files = capture_files(&dir, "owntracks").await;
    assert_eq!(files.len(), 1, "one OwnTracks capture file");
    assert!(
        capture_files(&dir, "overland").await.is_empty(),
        "must not land in the Overland tree"
    );
    let stored = tokio::fs::read(&files[0]).await.unwrap();
    assert_eq!(stored, body.as_bytes(), "OwnTracks body stored verbatim");
    assert!(!String::from_utf8_lossy(&stored).contains("received_at"));
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
