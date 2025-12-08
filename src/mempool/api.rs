use anyhow::anyhow;
use axum::{
    extract::{Multipart, State},
    http::StatusCode,
    response::IntoResponse,
    Json, Router,
};
use borsh::{BorshDeserialize, BorshSerialize};
use hyli_contract_sdk::TxHash;
use hyli_model::{
    api::APIRegisterContract, verifiers::validate_program_id, ContractName, ProgramId,
    RegisterContractAction, StructuredBlobData, Verifier,
};
use hyli_modules::{
    bus::{BusMessage, SharedMessageBus},
    modules::SharedBuildApiCtx,
    node_state::contract_registration::validate_contract_registration_metadata,
};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};
use utoipa::OpenApi;
use utoipa_axum::{router::OpenApiRouter, routes};

use crate::{
    bus::{bus_client, metrics::BusMetrics, BusClientSender},
    model::{BlobTransaction, Hashed, ProofTransaction, Transaction, TransactionData},
    rest::AppError,
};

#[derive(Debug, Serialize, Deserialize, Clone, BorshSerialize, BorshDeserialize)]
pub enum RestApiMessage {
    NewTx(Transaction),
}

impl BusMessage for RestApiMessage {}

bus_client! {
struct RestBusClient {
    sender(RestApiMessage),
}
}

pub struct RouterState {
    bus: RestBusClient,
}

#[derive(OpenApi)]
struct MempoolAPI;

pub async fn api(bus: &SharedMessageBus, ctx: &SharedBuildApiCtx) -> Router<()> {
    let state = RouterState {
        bus: RestBusClient::new_from_bus(bus.new_handle()).await,
    };

    let (router, api) = OpenApiRouter::with_openapi(MempoolAPI::openapi())
        .routes(routes!(register_contract))
        .routes(routes!(send_blob_transaction))
        .routes(routes!(send_proof_transaction))
        .routes(routes!(send_proof_transaction_multipart))
        .split_for_parts();

    if let Ok(mut o) = ctx.openapi.lock() {
        *o = o.clone().nest("/v1", api);
    }
    router.with_state(state)
}

async fn handle_send(
    mut state: RouterState,
    payload: TransactionData,
) -> Result<Json<TxHash>, AppError> {
    let tx: Transaction = payload.into();
    let tx_hash = tx.hashed();
    state
        .bus
        .send(RestApiMessage::NewTx(tx))
        .map(|_| tx_hash)
        .map(Json)
        .map_err(|err| AppError(StatusCode::INTERNAL_SERVER_ERROR, anyhow!(err)))
}

#[utoipa::path(
    post,
    path = "/tx/send/blob",
    tag = "Mempool",
    responses(
        (status = OK, description = "Send blob transaction", body = TxHash)
    )
)]
pub async fn send_blob_transaction(
    State(state): State<RouterState>,
    Json(payload): Json<BlobTransaction>,
) -> Result<impl IntoResponse, AppError> {
    info!(
        tx_hash = %payload.hashed().0,
        identity = %payload.identity.0,
        blob_count = payload.blobs.len(),
       contracts = ?payload
        .blobs
        .iter()
        .map(|blob| blob.contract_name.0.clone())
        .collect::<Vec<_>>(),
        "received blob transaction"
    );

    // Filter out incorrect contract-registring transactions
    for blob in payload.blobs.iter() {
        if blob.contract_name.0 != "hyli" {
            continue;
        }
        if let Ok(tx) = StructuredBlobData::<RegisterContractAction>::try_from(blob.data.clone()) {
            let parameters = tx.parameters;
            if let Err(err) = validate_contract_registration_metadata(
                &"hyli".into(),
                &parameters.contract_name,
                &parameters.verifier,
                &parameters.program_id,
                &parameters.state_commitment,
            ) {
                warn!(
                    tx_hash = %payload.hashed().0,
                    contract = %parameters.contract_name.0,
                    verifier = %parameters.verifier.0,
                    error = ?err,
                    "rejecting blob transaction due to invalid contract registration metadata"
                );
                return Err(AppError(StatusCode::BAD_REQUEST, anyhow!(err)));
            }
        }
    }

    // Filter out transactions with incorrect identity
    if let Err(e) = payload.validate_identity() {
        warn!(
            tx_hash = %payload.hashed().0,
            identity = %payload.identity.0,
            error = %e,
            "rejecting blob transaction due to invalid identity"
        );
        return Err(AppError(
            StatusCode::BAD_REQUEST,
            anyhow!("Invalid identity for blob tx: {}", e),
        ));
    }

    // Filter out transactions with too many blobs
    if payload.blobs.len() > 20 {
        warn!(
            tx_hash = %payload.hashed().0,
            blob_count = payload.blobs.len(),
            "rejecting blob transaction due to blob count limit"
        );
        return Err(AppError(
            StatusCode::BAD_REQUEST,
            anyhow!("Too many blobs in transaction"),
        ));
    }
    handle_send(state, TransactionData::Blob(payload))
        .await
        .inspect(|payload_hash| {
            debug!(
                tx_hash = %payload_hash.0,
                "blob transaction accepted and forwarded to bus"
            );
        })
}

#[utoipa::path(
    post,
    path = "/tx/send/proof",
    tag = "Mempool",
    responses(
        (status = OK, description = "Send proof transaction", body = TxHash)
    )
)]
pub async fn send_proof_transaction(
    State(state): State<RouterState>,
    Json(payload): Json<ProofTransaction>,
) -> Result<impl IntoResponse, AppError> {
    info!("Got proof transaction {}", payload.hashed());
    handle_send(state, TransactionData::Proof(payload)).await
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProofTxMetaJson {
    contract_name: ContractName,
    program_id: ProgramId,
    verifier: Verifier,
}

#[utoipa::path(
    post,
    path = "/tx/send/proof/multipart",
    tag = "Mempool",
    responses(
        (status = OK, description = "Send proof transaction via multipart", body = TxHash),
        (status = BAD_REQUEST, description = "Invalid multipart payload"),
    )
)]
/// Expects a multipart form with two parts:
/// - "meta": JSON metadata for the proof transaction (without the proof bytes)
/// - "proof": Raw proof bytes
/// Both parts are required.
/// Implemented because some clients take too long to send compressed data on the regular endpoint.
pub async fn send_proof_transaction_multipart(
    State(state): State<RouterState>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, AppError> {
    use hyli_model::ProofData;

    let mut meta: Option<ProofTxMetaJson> = None;
    let mut proof: Option<ProofData> = None;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| AppError(StatusCode::BAD_REQUEST, anyhow!(e)))?
    {
        match field.name() {
            Some("meta") => {
                let text = field
                    .text()
                    .await
                    .map_err(|e| AppError(StatusCode::BAD_REQUEST, anyhow!(e)))?;
                meta = serde_json::from_str(&text)
                    .map_err(|e| AppError(StatusCode::BAD_REQUEST, anyhow!(e)))
                    .ok();
            }
            Some("proof") => {
                let bytes = field
                    .bytes()
                    .await
                    .map_err(|e| AppError(StatusCode::BAD_REQUEST, anyhow!(e)))?;
                proof = Some(ProofData(bytes.to_vec()));
            }
            Some(name) => {
                return Err(AppError(
                    StatusCode::BAD_REQUEST,
                    anyhow!("unexpected multipart field name: {}", name),
                ));
            }
            None => continue,
        }
    }

    let (Some(meta), Some(proof)) = (meta, proof) else {
        return Err(AppError(
            StatusCode::BAD_REQUEST,
            anyhow!("multipart must include 'meta' JSON and 'proof' bytes parts"),
        ));
    };

    let payload = ProofTransaction {
        contract_name: meta.contract_name,
        program_id: meta.program_id,
        verifier: meta.verifier,
        proof,
    };

    info!("Got proof transaction {} (multipart)", payload.hashed());
    handle_send(state, TransactionData::Proof(payload)).await
}

#[utoipa::path(
    post,
    path = "/contract/register",
    tag = "Mempool",
    responses(
        (status = OK, description = "Register contract", body = TxHash)
    )
)]
pub async fn register_contract(
    State(state): State<RouterState>,
    Json(payload): Json<APIRegisterContract>,
) -> Result<impl IntoResponse, AppError> {
    let owner = "hyli".into();
    validate_contract_registration_metadata(
        &owner,
        &payload.contract_name,
        &payload.verifier,
        &payload.program_id,
        &payload.state_commitment,
    )
    .map_err(|err| AppError(StatusCode::BAD_REQUEST, anyhow!(err)))?;

    validate_program_id(&payload.verifier, &payload.program_id)
        .map_err(|err| AppError(StatusCode::BAD_REQUEST, anyhow!(err)))?;

    let tx = BlobTransaction::from(payload);

    handle_send(state, TransactionData::Blob(tx)).await
}

impl Clone for RouterState {
    fn clone(&self) -> Self {
        use hyli_modules::utils::static_type_map::Pick;
        Self {
            bus: RestBusClient::new(
                Pick::<BusMetrics>::get(&self.bus).clone(),
                Pick::<hyli_modules::bus::BusSender<RestApiMessage>>::get(&self.bus).clone(),
            ),
        }
    }
}
