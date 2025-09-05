use anyhow::{anyhow, Result};
use client_sdk::contract_indexer::{
    axum::{
        extract::{Path, State},
        http::StatusCode,
        response::IntoResponse,
        Json, Router,
    },
    utoipa::{openapi::OpenApi, ToSchema},
    utoipa_axum::{router::OpenApiRouter, routes},
    AppError, ContractHandler, ContractHandlerStore,
};
use sdk::Identity;
use serde::Serialize;

use client_sdk::contract_indexer::axum;
use client_sdk::contract_indexer::utoipa;

use crate::client::tx_executor_handler::SmtTokenProvableState;

impl ContractHandler for SmtTokenProvableState {
    async fn api(store: ContractHandlerStore<SmtTokenProvableState>) -> (Router<()>, OpenApi) {
        let (router, api) = OpenApiRouter::default()
            .routes(routes!(get_state))
            .routes(routes!(get_balance))
            .routes(routes!(get_allowance))
            .split_for_parts();

        (router.with_state(store), api)
    }
}

#[utoipa::path(
    get,
    path = "/state",
    tag = "Contract",
    responses(
        (status = OK, description = "Get json state of contract")
    )
)]
pub async fn get_state(
    State(state): State<ContractHandlerStore<SmtTokenProvableState>>,
) -> Result<impl IntoResponse, AppError> {
    let store = state.read().await;

    let contract = store.state.as_ref().ok_or(AppError(
        StatusCode::NOT_FOUND,
        anyhow!("Contract '{}' not found", store.contract_name),
    ))?;

    Ok(Json(contract.get_state()))
}

#[derive(Serialize, ToSchema)]
struct BalanceResponse {
    address: String,
    balance: u128,
}
#[utoipa::path(
    get,
    path = "/balance/{account}",
    params(
        ("account" = String, Path, description = "Account")
    ),
    tag = "Contract",
    responses(
        (status = OK, description = "Get balance of account", body = BalanceResponse)
    )
)]
pub async fn get_balance(
    Path(address): Path<Identity>,
    State(state): State<ContractHandlerStore<SmtTokenProvableState>>,
) -> Result<impl IntoResponse, AppError> {
    let store = state.read().await;

    let contract = store.state.as_ref().ok_or(AppError(
        StatusCode::NOT_FOUND,
        anyhow!("Contract '{}' not found", store.contract_name),
    ))?;

    let state = contract.get_state();

    state
        .get(&address)
        .cloned()
        .map(|account| BalanceResponse {
            address: account.address.0,
            balance: account.balance,
        })
        .map(Json)
        .ok_or_else(|| AppError(StatusCode::NOT_FOUND, anyhow!("Account not found")))
}

#[derive(Serialize, ToSchema)]
struct AllowanceResponse {
    owner: String,
    spender: String,
    allowance: u128,
}

#[utoipa::path(
    get,
    path = "/allowance/{account}/{spender}",
    params(
        ("account" = String, Path, description = "Account"),
        ("spender" = String, Path, description = "Spender")
    ),
    tag = "Contract",
    responses(
        (status = OK, description = "Get allowance of account for given spender", body = AllowanceResponse)
    )
)]
pub async fn get_allowance(
    Path((owner, spender)): Path<(Identity, Identity)>,
    State(state): State<ContractHandlerStore<SmtTokenProvableState>>,
) -> Result<impl IntoResponse, AppError> {
    let store = state.read().await;

    let contract = store.state.as_ref().ok_or(AppError(
        StatusCode::NOT_FOUND,
        anyhow!("Contract '{}' not found", store.contract_name),
    ))?;

    let state = contract.get_state();

    state
        .get(&owner)
        .cloned()
        .map(|account| AllowanceResponse {
            owner: account.address.0,
            spender: spender.0.clone(),
            allowance: account.allowances.get(&spender).cloned().unwrap_or(0),
        })
        .map(Json)
        .ok_or_else(|| AppError(StatusCode::NOT_FOUND, anyhow!("Account not found")))
}
