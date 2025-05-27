use anyhow::anyhow;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json, Router,
};
use client_sdk::contract_indexer::AppError;
use sdk::*;
use tracing::error;
use utoipa::OpenApi;
use utoipa_axum::{router::OpenApiRouter, routes};

use crate::{
    bus::{
        bus_client,
        command_response::{CmdRespClient, Query},
        metrics::BusMetrics,
        SharedMessageBus,
    },
    node_state::module::{QueryBlockHeight, QueryUnsettledTx},
};

use super::module::{NodeStateCtx, QuerySettledHeight};

bus_client! {
struct RestBusClient {
    sender(Query<ContractName, Contract>),
    sender(Query<QuerySettledHeight, BlockHeight>),
    sender(Query<QueryBlockHeight, BlockHeight>),
    sender(Query<QueryUnsettledTx, UnsettledBlobTransaction>),
}
}

pub struct RouterState {
    bus: RestBusClient,
}

#[derive(OpenApi)]
struct NodeStateAPI;

pub async fn api(bus: SharedMessageBus, ctx: &NodeStateCtx) -> Router<()> {
    let state = RouterState {
        bus: RestBusClient::new_from_bus(bus).await,
    };

    let (router, api) = OpenApiRouter::with_openapi(NodeStateAPI::openapi())
        .routes(routes!(get_block_height))
        .routes(routes!(get_contract))
        .routes(routes!(get_contract_settled_height))
        // TODO: figure out if we want to rely on the indexer instead
        .routes(routes!(get_unsettled_tx))
        .split_for_parts();

    if let Ok(mut o) = ctx.api.openapi.lock() {
        *o = o.clone().nest("/v1", api);
    }

    router.with_state(state)
}

#[utoipa::path(
    get,
    path = "/contract/{name}",
    params(
        ("name" = String, Path, description = "Contract name")
    ),
    tag = "Node State",
    responses(
        (status = OK, body = Contract)
    )
)]
pub async fn get_contract(
    Path(name): Path<ContractName>,
    State(mut state): State<RouterState>,
) -> Result<impl IntoResponse, AppError> {
    let name_clone = name.clone();
    match state.bus.request(name).await {
        Ok(contract) => Ok(Json(contract)),
        err => {
            if let Err(e) = err.as_ref() {
                if e.to_string().contains("Contract not found") {
                    return Err(AppError(
                        StatusCode::NOT_FOUND,
                        anyhow!("Contract {} not found", name_clone),
                    ));
                }
            }
            error!("{:?}", err);

            Err(AppError(
                StatusCode::INTERNAL_SERVER_ERROR,
                anyhow!("Error while getting contract {}", name_clone),
            ))
        }
    }
}

#[utoipa::path(
    get,
    path = "/contract/{name}/settled_height",
    params(
        ("name" = String, Path, description = "Contract name")
    ),
    description = "The block height where the contract was settled",
    tag = "Node State",
    responses(
        (status = OK, body = Contract)
    )
)]
pub async fn get_contract_settled_height(
    Path(name): Path<ContractName>,
    State(mut state): State<RouterState>,
) -> Result<impl IntoResponse, AppError> {
    let name_clone = name.clone();
    match state.bus.request(QuerySettledHeight(name)).await {
        Ok(contract) => Ok(Json::<BlockHeight>(contract)),
        err => {
            if let Err(e) = err.as_ref() {
                if e.to_string().contains("Contract not found") {
                    return Err(AppError(
                        StatusCode::NOT_FOUND,
                        anyhow!("Contract {} not found", name_clone),
                    ));
                }
            }
            error!("{:?}", err);

            Err(AppError(
                StatusCode::INTERNAL_SERVER_ERROR,
                anyhow!("Error while getting contract {}", name_clone),
            ))
        }
    }
}

#[utoipa::path(
    get,
    path = "/unsettled_tx/{blob_tx_hash}",
    params(
        ("blob_tx_hash" = String, Path, description = "Blob tx hash"),
    ),
    tag = "Node State",
    responses(
        (status = OK, body = UnsettledBlobTransaction)
    )
)]
pub async fn get_unsettled_tx(
    Path(blob_tx_hash): Path<String>,
    State(mut state): State<RouterState>,
) -> Result<impl IntoResponse, AppError> {
    match state
        .bus
        .request(QueryUnsettledTx(TxHash(blob_tx_hash)))
        .await
    {
        Ok(tx_context) => Ok(Json(tx_context)),
        err => {
            error!("{:?}", err);

            Err(AppError(
                StatusCode::INTERNAL_SERVER_ERROR,
                anyhow!("Error while getting tx context"),
            ))
        }
    }
}

#[utoipa::path(
    get,
    path = "/da/block/height",
    tag = "Node State",
    responses(
        (status = OK, body = BlockHeight)
    )
)]
pub async fn get_block_height(
    State(mut state): State<RouterState>,
) -> Result<impl IntoResponse, AppError> {
    match state.bus.request(QueryBlockHeight {}).await {
        Ok(block_height) => Ok(Json(block_height)),
        err => {
            error!("{:?}", err);

            Err(AppError(
                StatusCode::INTERNAL_SERVER_ERROR,
                anyhow!("Error while getting block height"),
            ))
        }
    }
}

impl Clone for RouterState {
    fn clone(&self) -> Self {
        use crate::utils::static_type_map::Pick;
        Self {
            bus: RestBusClient::new(
                Pick::<BusMetrics>::get(&self.bus).clone(),
                Pick::<tokio::sync::broadcast::Sender<Query<ContractName, Contract>>>::get(
                    &self.bus,
                )
                .clone(),
                Pick::<
                    tokio::sync::broadcast::Sender<
                        Query<QuerySettledHeight, BlockHeight>,
                    >,
                >::get(&self.bus)
                .clone(),
                Pick::<tokio::sync::broadcast::Sender<Query<QueryBlockHeight, BlockHeight>>>::get(
                    &self.bus,
                )
                .clone(),
                Pick::<
                    tokio::sync::broadcast::Sender<
                        Query<QueryUnsettledTx, UnsettledBlobTransaction>,
                    >,
                >::get(&self.bus)
                .clone(),
            ),
        }
    }
}
