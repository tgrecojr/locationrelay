//! Outbound relay to Dawarich.
//!
//! Forwarding is deliberately **decoupled** from the inbound request. The
//! handler persists each batch to NDJSON (the durable source of truth) and then
//! hands an `Arc` of it to a bounded in-memory queue via [`ForwardHandle`]. A
//! single background worker drains that queue and POSTs to Dawarich's Overland
//! endpoint, so a slow or unavailable Dawarich never blocks the iPhone's
//! request. Delivery is at-most-once: if the queue overflows or the process
//! restarts during a Dawarich outage, the affected batches stay on disk for
//! manual replay rather than being retried forever.
//!
//! The Dawarich API key is sent as an `Authorization: Bearer` header, never as
//! a query parameter, and is never logged. The same key authenticates both the
//! Overland and OwnTracks endpoints.

use std::sync::Arc;
use std::time::Duration;

use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use tokio::sync::mpsc::{self, error::TrySendError};

use crate::config::{Config, DawarichConfig};
use crate::observability;

/// A single item queued for forwarding, tagged with its source so the worker
/// can target the right Dawarich endpoint with the right body shape.
pub enum Forward {
    /// An Overland batch of GeoJSON features → `/api/v1/overland/batches`,
    /// wrapped as `{"locations": [...]}`.
    Overland(Arc<Vec<Value>>),
    /// A single OwnTracks message → `/api/v1/owntracks/points`, sent as-is.
    Owntracks(Arc<Value>),
}

/// Cloneable producer side of the forward queue, injected into the router state.
///
/// When forwarding is disabled (no Dawarich URL configured) this holds `None`
/// and the `enqueue_*` methods are no-ops, so the service behaves exactly as it
/// did before this feature existed.
#[derive(Clone)]
pub struct ForwardHandle {
    tx: Option<mpsc::Sender<Forward>>,
}

impl ForwardHandle {
    /// A handle that drops everything — used when forwarding is off and in tests.
    pub fn disabled() -> Self {
        Self { tx: None }
    }

    /// Queue an Overland batch for forwarding.
    pub fn enqueue_overland(&self, batch: Arc<Vec<Value>>) {
        self.enqueue(Forward::Overland(batch));
    }

    /// Queue a single OwnTracks message for forwarding.
    pub fn enqueue_owntracks(&self, message: Arc<Value>) {
        self.enqueue(Forward::Owntracks(message));
    }

    /// Queue an item for forwarding. Never blocks: if the bounded queue is full
    /// the item is dropped (it is already durable on disk) and counted.
    fn enqueue(&self, item: Forward) {
        let Some(tx) = &self.tx else {
            return;
        };
        match tx.try_send(item) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                observability::record_forward_failure();
                tracing::debug!("Dawarich forward queue full; dropped batch (still on disk)");
            }
            Err(TrySendError::Closed(_)) => {
                tracing::debug!("Dawarich forward worker stopped; dropped batch");
            }
        }
    }
}

/// Start the forwarder. Returns a live [`ForwardHandle`] when a Dawarich URL is
/// configured (spawning the background worker), or a disabled handle otherwise.
pub fn start(config: &Arc<Config>) -> ForwardHandle {
    let Some(dawarich) = config.dawarich.clone() else {
        return ForwardHandle::disabled();
    };
    let client = match build_client(config) {
        Ok(client) => client,
        Err(e) => {
            tracing::error!(error = %e, "failed to build Dawarich HTTP client; forwarding disabled");
            return ForwardHandle::disabled();
        }
    };

    let (tx, rx) = mpsc::channel(config.forward_queue_capacity.max(1));
    let max_attempts = config.forward_max_attempts.max(1);
    tracing::info!(
        overland = %dawarich.overland_endpoint,
        owntracks = %dawarich.owntracks_endpoint,
        "Dawarich forwarding enabled"
    );
    tokio::spawn(worker(client, dawarich, rx, max_attempts));
    ForwardHandle { tx: Some(tx) }
}

fn build_client(config: &Config) -> reqwest::Result<Client> {
    let timeout = Duration::from_secs(config.forward_timeout_secs);
    Client::builder()
        .timeout(timeout)
        .connect_timeout(timeout)
        // Never chase a redirect to another host — the endpoint is fixed config.
        .redirect(reqwest::redirect::Policy::none())
        .build()
}

/// Drain the queue forever, forwarding one item at a time (order preserved).
async fn worker(
    client: Client,
    dawarich: DawarichConfig,
    mut rx: mpsc::Receiver<Forward>,
    max_attempts: u32,
) {
    while let Some(item) = rx.recv().await {
        let (endpoint, body) = prepare(&dawarich, &item);
        forward_with_retry(&client, endpoint, &dawarich.token, &body, max_attempts).await;
    }
}

/// Pick the target endpoint and build the request body for one queued item.
/// Overland batches are wrapped in the `{"locations": [...]}` envelope Dawarich
/// expects; OwnTracks messages are forwarded verbatim.
fn prepare<'a>(dawarich: &'a DawarichConfig, item: &Forward) -> (&'a str, Value) {
    match item {
        Forward::Overland(batch) => (
            &dawarich.overland_endpoint,
            json!({ "locations": &**batch }),
        ),
        Forward::Owntracks(message) => (&dawarich.owntracks_endpoint, (**message).clone()),
    }
}

/// Classification of a single POST attempt.
enum Outcome {
    Delivered,
    /// Worth retrying (network error, timeout, 5xx, 429).
    Transient(String),
    /// Will not fix itself (4xx such as a bad key or rejected payload).
    Permanent(String),
}

/// Try to forward one item, retrying transient failures with exponential
/// backoff up to `max_attempts` total tries, then giving up on this item only.
/// reqwest has no built-in retry, so the loop is explicit.
async fn forward_with_retry(
    client: &Client,
    endpoint: &str,
    token: &str,
    body: &Value,
    max_attempts: u32,
) {
    for attempt in 1..=max_attempts {
        match send_once(client, endpoint, token, body).await {
            Outcome::Delivered => return,
            Outcome::Permanent(reason) => {
                tracing::debug!(reason = %reason, "Dawarich rejected batch; not retrying");
                observability::record_forward_failure();
                return;
            }
            Outcome::Transient(reason) if attempt < max_attempts => {
                let backoff = backoff_delay(attempt);
                tracing::debug!(reason = %reason, attempt, ?backoff, "Dawarich forward failed; retrying");
                tokio::time::sleep(backoff).await;
            }
            Outcome::Transient(reason) => {
                tracing::debug!(reason = %reason, attempt, "Dawarich forward gave up after final attempt");
                observability::record_forward_failure();
            }
        }
    }
}

async fn send_once(client: &Client, endpoint: &str, token: &str, body: &Value) -> Outcome {
    let request = client.post(endpoint).bearer_auth(token).json(body);
    match request.send().await {
        Ok(resp) => classify_status(resp.status()),
        // A send-time error is always a transport problem (DNS, connect, TLS,
        // timeout) — transient by nature. The reason is a coarse category so the
        // token (only ever in the Authorization header) can never reach a log.
        Err(e) => {
            let reason = if e.is_timeout() {
                "timeout"
            } else if e.is_connect() {
                "connect"
            } else {
                "transport"
            };
            Outcome::Transient(reason.to_string())
        }
    }
}

fn classify_status(status: StatusCode) -> Outcome {
    if status.is_success() {
        Outcome::Delivered
    } else if status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS {
        Outcome::Transient(format!("status {}", status.as_u16()))
    } else {
        Outcome::Permanent(format!("status {}", status.as_u16()))
    }
}

/// Exponential backoff: 500ms, 1s, 2s, 4s … capped at 30s.
fn backoff_delay(attempt: u32) -> Duration {
    let shift = (attempt - 1).min(20);
    let millis = 500u64.saturating_mul(1u64 << shift).min(30_000);
    Duration::from_millis(millis)
}
