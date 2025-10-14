use anyhow::Context;
use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    http::{StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{Local, NaiveDate};
use clap::Parser;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
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
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::RwLock;
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
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,

    /// Target server URL to proxy to
    #[serde(default = "default_target_url")]
    pub target_url: String,

    /// Redis connection string (optional, uses in-memory store if not provided)
    pub redis_url: Option<String>,

    /// Daily rate limit per IP+contract combination for blob transactions
    #[serde(default = "default_daily_limit")]
    pub daily_limit: u32,

    /// Log format
    #[serde(default = "default_log_format")]
    pub log_format: String,
}

fn default_listen_addr() -> String {
    "0.0.0.0:8080".to_string()
}

fn default_target_url() -> String {
    "http://localhost:4321".to_string()
}

fn default_daily_limit() -> u32 {
    500
}

fn default_log_format() -> String {
    "json".to_string()
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
            let contracts: HashSet<String> = blob_tx
                .blobs
                .iter()
                .map(|blob| blob.contract_name.clone())
                .collect();
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

    // Reject if contract names is not 'faucet' or 'wallet'.
    if contract_names.iter().any(|name| {
        name != "secp256k1" && name != "check_secret" && name != "faucet" && name != "wallet"
    }) {
        tracing::warn!(
            "Invalid contract names in blob transaction from IP: {}, identity: {}, contracts: {:?}",
            ip,
            identity,
            contract_names
        );
        return Err(StatusCode::BAD_REQUEST);
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
    let (parts, body) = req.into_parts();
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

    let client = Arc::new(Client::builder(TokioExecutor::new()).build_http());
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

        let mut watcher = match notify::recommended_watcher(tx) {
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
}
