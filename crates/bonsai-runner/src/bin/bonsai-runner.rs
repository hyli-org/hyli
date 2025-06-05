use axum::{extract::Json, http::StatusCode, response::IntoResponse, routing::post, Router};
use risc0_zkvm::Receipt;
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, fs, sync::Arc};
use tracing::{info, Level};

#[derive(Deserialize)]
struct ProveRequest {
    api_key: String,
    elf: Vec<u8>,
    input_data: Vec<u8>,
    contract_name: String,
}

#[derive(Serialize)]
struct ProveResponse {
    receipt: Receipt,
}

#[tokio::main]
async fn main() {
    // Initialisation de tracing
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    // Charger les API keys depuis un fichier JSON
    let api_keys = load_api_keys("api_keys.json").expect("Failed to load API keys");
    let shared_api_keys = Arc::new(api_keys);

    // Création des routes Axum
    let app = Router::new()
        .route("/prove", post(prove_handler))
        .layer(axum::Extension(shared_api_keys));

    let listener = hyle_net::net::bind_tcp_listener(3000).await.unwrap();
    info!("Server listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}

async fn prove_handler(
    api_keys: axum::Extension<Arc<HashSet<String>>>,
    Json(payload): Json<ProveRequest>,
) -> Result<impl IntoResponse, StatusCode> {
    // Vérifier l'API key
    if !api_keys.contains(&payload.api_key) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    // Appeler la fonction `run_bonsai`
    let receipt = hyle_bonsai_runner::run_bonsai(&payload.elf, payload.input_data, &payload.contract_name)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(ProveResponse { receipt }))
}

fn load_api_keys(file_path: &str) -> Result<HashSet<String>, std::io::Error> {
    let data = fs::read_to_string(file_path)?;
    let api_keys: Vec<String> = serde_json::from_str(&data)?;
    Ok(api_keys.into_iter().collect())
}
