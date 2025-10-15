use anyhow::Context;
use axum::http::header::CONNECTION;
use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    http::{HeaderValue, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{Local, NaiveDate};
use clap::Parser;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use hyli_model::RegisterContractAction;
use hyli_modules::{modules::rest::handle_panic, utils::logger::setup_tracing};
use hyper::body::Incoming;
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::TokioExecutor,
};
use notify::{Event, RecursiveMode, Watcher};
use opentelemetry::{
    InstrumentationScope, KeyValue,
    metrics::{Counter, Gauge},
};
use prometheus::{Encoder, Registry, TextEncoder};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::RwLock;
use std::{collections::HashMap, time::Duration};
use std::{collections::HashSet, sync::Arc};
use tower_governor::key_extractor::{KeyExtractor, SmartIpKeyExtractor};
use tower_http::catch_panic::CatchPanicLayer;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to configuration file
    #[arg(long, default_value = "rate_limiter_proxy.toml")]
    config: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProxyConfig {
    /// Proxy listen address
    pub listen_addr: String,

    /// Target server URL to proxy to
    pub target_url: String,

    /// Redis connection string (optional, uses in-memory store if not provided)
    pub redis_url: Option<String>,

    /// Daily rate limit per identity+contract combination for blob transactions
    pub daily_limit: u32,

    /// Log format
    pub log_format: String,

    /// Blacklisted contract patterns (supports * wildcard)
    pub blacklist_contracts: Vec<String>,

    /// Blacklisted identity patterns (supports * wildcard)
    pub blacklist_identities: Vec<String>,

    /// Allowed contract patterns (supports * wildcard)
    /// If empty, all contracts are allowed (unless blacklisted)
    /// If not empty, only contracts matching these patterns are allowed
    pub allowed_contracts: Vec<String>,
}

impl ProxyConfig {
    fn load(config_file: &str) -> Result<Self, anyhow::Error> {
        let s = config::Config::builder()
            .add_source(config::File::from_str(
                include_str!("../rate_limiter_proxy_conf_defaults.toml"),
                config::FileFormat::Toml,
            ))
            .add_source(config::File::with_name(config_file).required(true))
            .build()?;

        let conf: Self = s.try_deserialize()?;
        Ok(conf)
    }
}

// Simplified versions of the structures we need to parse
#[derive(Debug, Serialize, Deserialize)]
struct BlobTransaction {
    pub identity: String,
    pub blobs: Vec<Blob>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Blob {
    pub contract_name: String,
    pub data: Vec<u8>,
}

/// Simple pattern matching with * wildcard support
/// Returns true if the value matches the pattern
fn matches_pattern(pattern: &str, value: &str) -> bool {
    // If no wildcard, do exact match
    if !pattern.contains('*') {
        return pattern == value;
    }

    // Convert pattern to regex-like matching
    let parts: Vec<&str> = pattern.split('*').collect();

    // Pattern starts with *
    let starts_with_wildcard = pattern.starts_with('*');
    // Pattern ends with *
    let ends_with_wildcard = pattern.ends_with('*');

    match (starts_with_wildcard, ends_with_wildcard, parts.len()) {
        // Pattern is just "*"
        (true, true, 2) if parts[0].is_empty() && parts[1].is_empty() => true,
        // Pattern is "prefix*"
        (false, true, 2) => value.starts_with(parts[0]),
        // Pattern is "*suffix"
        (true, false, 2) => value.ends_with(parts[1]),
        // Pattern is "*middle*"
        (true, true, 3) if parts[0].is_empty() && parts[2].is_empty() => value.contains(parts[1]),
        // Pattern is "prefix*suffix"
        (false, false, 2) => {
            value.starts_with(parts[0])
                && value.ends_with(parts[1])
                && value.len() >= parts[0].len() + parts[1].len()
        }
        // Multiple wildcards - more complex matching
        _ => {
            let mut pos = 0;
            for (i, part) in parts.iter().enumerate() {
                if part.is_empty() {
                    continue;
                }

                // For first part, check if it matches from the beginning
                if i == 0 && !starts_with_wildcard {
                    if !value[pos..].starts_with(part) {
                        return false;
                    }
                    pos += part.len();
                    continue;
                }

                // For last part, check if it matches at the end
                if i == parts.len() - 1 && !ends_with_wildcard {
                    return value[pos..].ends_with(part);
                }

                // For middle parts, find the next occurrence
                if let Some(found_pos) = value[pos..].find(part) {
                    pos += found_pos + part.len();
                } else {
                    return false;
                }
            }
            true
        }
    }
}

/// Blob-specific handler with contract-level rate limiting
async fn blob_proxy_handler(
    State(config): State<AppConfig>,
    req: Request<Body>,
) -> Result<Response<Incoming>, StatusCode> {
    // Extract IP for logging
    let ip = SmartIpKeyExtractor
        .extract(&req)
        .map(|ip| ip.to_string())
        .unwrap_or_else(|_| "unknown".to_string());

    // Extract and parse the request body
    let (parts, body) = req.into_parts();
    let body_bytes = match axum::body::to_bytes(body, 1024 * 1024).await {
        Ok(bytes) => bytes,
        Err(_) => {
            tracing::warn!("Failed to read request body from IP: {}", ip);
            return Err(StatusCode::BAD_REQUEST);
        }
    };

    // Parse JSON to extract contract names and identity
    let (identity, contract_names) = match serde_json::from_slice::<BlobTransaction>(&body_bytes) {
        Ok(blob_tx) => {
            let mut contracts: HashSet<String> = blob_tx
                .blobs
                .iter()
                .map(|blob| blob.contract_name.clone())
                .collect();

            if let Some(pos) = contracts.iter().position(|c| c == "hyli") {
                if let Ok(action) =
                    borsh::from_slice::<RegisterContractAction>(&blob_tx.blobs[pos].data)
                {
                    contracts.insert(action.contract_name.0);
                }
            }
            (blob_tx.identity, contracts.into_iter().collect::<Vec<_>>())
        }
        Err(e) => {
            tracing::warn!(
                "Failed to parse blob transaction JSON from IP {}: {}",
                ip,
                e
            );
            return Err(StatusCode::BAD_REQUEST);
        }
    };

    // Check if identity is blacklisted
    let blacklist_identities = config.config.read().unwrap().blacklist_identities.clone();
    for pattern in &blacklist_identities {
        if matches_pattern(pattern, &identity) {
            config
                .metrics
                .increment_blacklisted_identity(&identity, pattern);
            return Err(StatusCode::FORBIDDEN);
        }
    }

    // Check if any contract is blacklisted
    let blacklist_contracts = config.config.read().unwrap().blacklist_contracts.clone();
    for contract in &contract_names {
        for pattern in &blacklist_contracts {
            if matches_pattern(pattern, contract) {
                config
                    .metrics
                    .increment_blacklisted_contract(contract, pattern);
                return Err(StatusCode::FORBIDDEN);
            }
        }
    }

    // Check if contracts are in the allowed list (if configured)
    let allowed_contracts = config.config.read().unwrap().allowed_contracts.clone();
    if !allowed_contracts.is_empty() {
        for contract in &contract_names {
            let mut is_allowed = false;
            for pattern in &allowed_contracts {
                if matches_pattern(pattern, contract) {
                    is_allowed = true;
                    break;
                }
            }
            if !is_allowed {
                tracing::warn!(
                    "Contract not in allowed list in blob transaction from IP: {}, identity: {}, contract: {}",
                    ip,
                    identity,
                    contract
                );
                return Err(StatusCode::BAD_REQUEST);
            }
        }
    }

    // Rate limiting logic
    let today = Local::now().date_naive();
    let mut limited = false;
    let mut limited_contracts = Vec::new();

    // Get current daily limit from config
    let daily_limit = config.config.read().unwrap().daily_limit;

    // Use DashMap entry API to avoid unnecessary cloning and locking
    match config.rate_limits.entry(identity.clone()) {
        Entry::Occupied(mut occ) => {
            let contract_map = occ.get_mut();
            for contract in &contract_names {
                let entry = contract_map.entry(contract.clone()).or_insert((0, today));
                if entry.1 != today {
                    entry.0 = 0;
                    entry.1 = today;
                }
                if entry.0 >= daily_limit {
                    limited = true;
                    limited_contracts.push(contract.clone());
                } else {
                    entry.0 += 1;
                }
            }
        }
        Entry::Vacant(vac) => {
            let mut contract_map = HashMap::with_capacity(contract_names.len());
            for contract in &contract_names {
                contract_map.insert(contract.clone(), (1, today));
            }
            vac.insert(contract_map);
        }
    }

    // Count all rate-limited contracts for metrics

    let ratelimited = config
        .rate_limits
        .iter()
        .map(|entry| {
            entry
                .value()
                .iter()
                .filter(|(_, (count, date))| count >= &daily_limit && date == &today)
                .count()
        })
        .sum::<usize>();

    config.metrics.set_currently_limited(ratelimited as u64);

    if limited {
        // Update metrics

        for contract in &limited_contracts {
            config.metrics.increment_blob_tx(contract, true);
        }
        tracing::warn!(
            identity = %identity,
            "Rate limit exceeded for identity: {}, contracts: {:?}",
            identity,
            limited_contracts
        );
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }

    // Update metrics for successful blob transactions
    for contract in &contract_names {
        config.metrics.increment_blob_tx(contract, false);
    }

    tracing::info!(
        "Received blob transaction from IP: {}, identity: {}, contracts: {:?}",
        ip,
        identity,
        contract_names
    );

    // Reconstruct the request
    let req = Request::from_parts(parts, Body::from(body_bytes));

    // Forward to the actual handler
    proxy_handler(State(config), req).await
}

/// Regular proxy handler for non-blob requests
async fn proxy_handler(
    State(config): State<AppConfig>,
    mut req: Request<Body>,
) -> Result<Response<Incoming>, StatusCode> {
    // Get current target URL from config
    let target_url = config.config.read().unwrap().target_url.clone();

    // Build the target URL
    let target_uri = format!(
        "{}{}",
        target_url,
        req.uri()
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or("")
    );

    // Parse the target URI
    let uri = target_uri
        .parse::<Uri>()
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    // Update the request URI
    *req.uri_mut() = uri;

    // Use the pooled HTTP client from config
    let client = config.client.clone();

    // Convert axum body to hyper body
    let (mut parts, body) = req.into_parts();
    // Ensure upstream closes the connection after response
    parts
        .headers
        .insert(CONNECTION, HeaderValue::from_static("close"));
    let hyper_req = hyper::Request::from_parts(parts, body);

    tracing::debug!("Forwarding request to target: {}", hyper_req.uri());

    config.metrics.increment_fallback();

    // Forward the request
    match client.request(hyper_req).await {
        Ok(response) => Ok(response),
        Err(e) => {
            tracing::error!("Proxy request failed: {:?}", e);
            Err(StatusCode::BAD_GATEWAY)
        }
    }
}

/// Health check endpoint
async fn health_check() -> &'static str {
    "OK"
}

type RateLimitData = DashMap<String, HashMap<String, (u32, NaiveDate)>>;

#[derive(Clone)]
struct AppConfig {
    config: Arc<RwLock<ProxyConfig>>,
    client: Arc<Client<HttpConnector, Body>>,
    rate_limits: Arc<RateLimitData>,
    metrics: RateLimiterMetrics,
    registry: Registry,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Load initial configuration
    let proxy_config = ProxyConfig::load(&args.config)?;

    setup_tracing(&proxy_config.log_format, "rate-limiter-proxy".to_string())
        .expect("Failed to set up tracing");

    tracing::info!("Starting rate-limiting proxy");
    tracing::info!("Configuration file: {}", args.config);
    tracing::info!("Listen address: {}", proxy_config.listen_addr);
    tracing::info!("Target URL: {}", proxy_config.target_url);

    let registry = Registry::new();
    // Init global metrics meter we expose as an endpoint
    let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_reader(
            opentelemetry_prometheus::exporter()
                .with_registry(registry.clone())
                .build()
                .context("starting prometheus exporter")?,
        )
        .build();

    opentelemetry::global::set_meter_provider(provider.clone());

    // Configure HTTP connector for better load balancing with headless k8s services
    // Set idle timeout to force DNS re-resolution and distribute load across pods
    let mut http_connector = HttpConnector::new();
    // Disable connection pooling to force new connections (and DNS lookups) per request
    http_connector.set_nodelay(true);
    http_connector.set_keepalive(None);

    let client = Arc::new(
        Client::builder(TokioExecutor::new())
            .pool_idle_timeout(Duration::from_secs(0))
            .pool_max_idle_per_host(0)
            .build(http_connector),
    );
    let rate_limits = Arc::new(DashMap::new());
    let shared_config = Arc::new(RwLock::new(proxy_config.clone()));

    let app_config = AppConfig {
        config: shared_config.clone(),
        client,
        rate_limits,
        metrics: RateLimiterMetrics::global("rate_limiter_proxy".to_string()),
        registry,
    };

    // Set up file watcher
    // Important: Don't canonicalize before getting parent directory to support
    // Kubernetes ConfigMaps which use symlinks that are updated atomically
    let config_path = PathBuf::from(&args.config);
    let config_filename = config_path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default();
    let watch_dir = config_path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));

    // Clone the original path for reloading (important for ConfigMap symlinks)
    let config_path_for_reload = config_path;

    let shared_config_clone = shared_config.clone();

    std::thread::spawn(move || {
        let (tx, rx) = std::sync::mpsc::channel::<Result<Event, notify::Error>>();

        let mut watcher = match notify::PollWatcher::new(
            tx,
            notify::Config::default().with_poll_interval(Duration::from_secs(2)),
        ) {
            Ok(w) => w,
            Err(e) => {
                tracing::error!("Failed to create file watcher: {:?}", e);
                return;
            }
        };

        // Watch the parent directory to handle editors that replace files
        if let Err(e) = watcher.watch(&watch_dir, RecursiveMode::NonRecursive) {
            tracing::error!("Failed to watch config directory: {:?}", e);
            return;
        }

        tracing::info!(
            "Watching config file for changes: {}",
            config_path_for_reload.display()
        );

        for res in rx {
            match res {
                Ok(event) => {
                    // Filter events to only process our config file
                    let is_our_file = event.paths.iter().any(|p| {
                        p.file_name()
                            .map(|n| n.to_string_lossy() == config_filename)
                            .unwrap_or(false)
                    });

                    if is_our_file && (event.kind.is_modify() || event.kind.is_create()) {
                        tracing::info!("Config file changed, reloading configuration...");

                        match ProxyConfig::load(config_path_for_reload.to_str().unwrap()) {
                            Ok(new_config) => {
                                let mut config = shared_config_clone.write().unwrap();
                                *config = new_config.clone();
                                tracing::info!("Configuration reloaded successfully");
                                tracing::info!("New target URL: {}", new_config.target_url);
                                tracing::info!("New daily limit: {}", new_config.daily_limit);
                            }
                            Err(e) => {
                                tracing::error!("Failed to reload configuration: {:?}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("File watcher error: {:?}", e);
                }
            }
        }
    });

    // Build the application
    let app = Router::new()
        .route("/health", get(health_check))
        .route("/v1/metrics", get(get_metrics))
        // Blob transaction with custom rate limiting and contract parsing
        .route("/v1/tx/send/blob", post(blob_proxy_handler))
        .fallback(proxy_handler)
        .with_state(app_config)
        .layer(CatchPanicLayer::custom(handle_panic))
        .layer(tower_http::cors::CorsLayer::permissive());

    // Parse listen address
    let listen_addr = proxy_config.listen_addr.clone();
    let listener = tokio::net::TcpListener::bind(&listen_addr).await?;

    tracing::info!("Rate-limiting proxy listening on {}", listen_addr);

    // Start the server
    axum::serve(listener, app).await?;

    Ok(())
}

pub(crate) async fn get_metrics(State(s): State<AppConfig>) -> Result<Response, StatusCode> {
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    encoder
        .encode(&s.registry.gather(), &mut buffer)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let res = String::from_utf8(buffer).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(res.into_response())
}

#[derive(Clone)]
struct RateLimiterMetrics {
    // Define any metrics you want to track here
    fallback_counter: Counter<u64>,
    blob_tx_counter: Counter<u64>,
    currently_limited_gauge: Gauge<u64>,
    blacklisted_identity_counter: Counter<u64>,
    blacklisted_contract_counter: Counter<u64>,
}

impl RateLimiterMetrics {
    pub fn global(node_name: String) -> RateLimiterMetrics {
        let scope = InstrumentationScope::builder(node_name).build();
        let my_meter = opentelemetry::global::meter_with_scope(scope);

        let rate_limiter = "proxy_limit";

        RateLimiterMetrics {
            fallback_counter: my_meter
                .u64_counter(format!("{rate_limiter}_fallback_counter"))
                .build(),
            blob_tx_counter: my_meter
                .u64_counter(format!("{rate_limiter}_blob_tx_counter"))
                .build(),
            currently_limited_gauge: my_meter
                .u64_gauge(format!("{rate_limiter}_currently_limited_counter"))
                .build(),
            blacklisted_identity_counter: my_meter
                .u64_counter(format!("{rate_limiter}_blacklisted_identity_counter"))
                .build(),
            blacklisted_contract_counter: my_meter
                .u64_counter(format!("{rate_limiter}_blacklisted_contract_counter"))
                .build(),
        }
    }

    fn labels(&self, contract: &str, blocked: bool) -> [KeyValue; 2] {
        let blocked = if blocked { "true" } else { "false" };

        [
            KeyValue::new("contract", contract.to_string()),
            KeyValue::new("blocked", blocked),
        ]
    }

    pub fn increment_fallback(&self) {
        self.fallback_counter.add(1, &[]);
    }

    pub fn increment_blob_tx(&self, contract: &str, blocked: bool) {
        self.blob_tx_counter.add(1, &self.labels(contract, blocked));
    }

    pub fn set_currently_limited(&self, value: u64) {
        self.currently_limited_gauge.record(value, &[]);
    }

    pub fn increment_blacklisted_identity(&self, identity: &str, pattern: &str) {
        self.blacklisted_identity_counter.add(
            1,
            &[
                KeyValue::new("identity", identity.to_string()),
                KeyValue::new("pattern", pattern.to_string()),
            ],
        );
    }

    pub fn increment_blacklisted_contract(&self, contract: &str, pattern: &str) {
        self.blacklisted_contract_counter.add(
            1,
            &[
                KeyValue::new("contract", contract.to_string()),
                KeyValue::new("pattern", pattern.to_string()),
            ],
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pattern_matching_exact() {
        assert!(matches_pattern("test", "test"));
        assert!(!matches_pattern("test", "test2"));
        assert!(!matches_pattern("test", "tes"));
    }

    #[test]
    fn test_pattern_matching_prefix() {
        assert!(matches_pattern("test_*", "test_contract"));
        assert!(matches_pattern("test_*", "test_"));
        assert!(!matches_pattern("test_*", "test"));
        assert!(!matches_pattern("test_*", "other_test"));
    }

    #[test]
    fn test_pattern_matching_suffix() {
        assert!(matches_pattern("*_spam", "contract_spam"));
        assert!(matches_pattern("*_spam", "_spam"));
        assert!(!matches_pattern("*_spam", "spam"));
        assert!(!matches_pattern("*_spam", "spam_contract"));
    }

    #[test]
    fn test_pattern_matching_contains() {
        assert!(matches_pattern("*bad*", "bad"));
        assert!(matches_pattern("*bad*", "very_bad_contract"));
        assert!(matches_pattern("*bad*", "badcontract"));
        assert!(matches_pattern("*bad*", "contractbad"));
        assert!(!matches_pattern("*bad*", "good_contract"));
    }

    #[test]
    fn test_pattern_matching_prefix_suffix() {
        assert!(matches_pattern("test_*_spam", "test_contract_spam"));
        assert!(matches_pattern("test_*_spam", "test__spam"));
        assert!(!matches_pattern("test_*_spam", "test_spam"));
        assert!(!matches_pattern("test_*_spam", "other_test_contract_spam"));
    }

    #[test]
    fn test_pattern_matching_wildcard_only() {
        assert!(matches_pattern("*", "anything"));
        assert!(matches_pattern("*", ""));
        assert!(matches_pattern("*", "test_contract_123"));
    }

    #[test]
    fn test_pattern_matching_multiple_wildcards() {
        assert!(matches_pattern(
            "test_*_middle_*_end",
            "test_start_middle_center_end"
        ));
        assert!(matches_pattern("a*b*c", "abc"));
        assert!(matches_pattern("a*b*c", "aXbYc"));
        assert!(matches_pattern("a*b*c", "aXXXbYYYc"));
        assert!(!matches_pattern("a*b*c", "ac"));
        assert!(!matches_pattern("a*b*c", "abc2"));
    }

    #[test]
    fn test_blacklist_identity_patterns() {
        // Test exact match
        assert!(matches_pattern("hyle1badactor", "hyle1badactor"));
        assert!(!matches_pattern("hyle1badactor", "hyle1goodactor"));

        // Test prefix match
        assert!(matches_pattern("hyle1spam*", "hyle1spam123"));
        assert!(matches_pattern("hyle1spam*", "hyle1spammer"));
        assert!(!matches_pattern("hyle1spam*", "hyle1good"));

        // Test suffix match
        assert!(matches_pattern("*abuser", "testabuser"));
        assert!(matches_pattern("*abuser", "hyle1abuser"));
        assert!(!matches_pattern("*abuser", "hyle1good"));
    }

    #[test]
    fn test_blacklist_contract_patterns() {
        // Test exact match
        assert!(matches_pattern("malicious_contract", "malicious_contract"));
        assert!(!matches_pattern("malicious_contract", "good_contract"));

        // Test prefix match
        assert!(matches_pattern("test_*", "test_contract"));
        assert!(matches_pattern("test_*", "test_123"));
        assert!(!matches_pattern("test_*", "prod_contract"));

        // Test contains match
        assert!(matches_pattern("*bad*", "very_bad_contract"));
        assert!(matches_pattern("*bad*", "bad"));
        assert!(!matches_pattern("*bad*", "good_contract"));
    }
}
