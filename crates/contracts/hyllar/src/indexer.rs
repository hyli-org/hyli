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

use crate::*;
use client_sdk::contract_indexer::axum;
use client_sdk::contract_indexer::utoipa;

impl ContractHandler for Hyllar {
    async fn api(store: ContractHandlerStore<Hyllar>) -> (Router<()>, OpenApi) {
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
pub async fn get_state<S: Serialize + Clone + 'static>(
    State(state): State<ContractHandlerStore<S>>,
) -> Result<impl IntoResponse, AppError> {
    let store = state.read().await;
    store.state.clone().map(Json).ok_or(AppError(
        StatusCode::NOT_FOUND,
        anyhow!("No state found for contract '{}'", store.contract_name),
    ))
}

#[derive(Serialize, ToSchema)]
struct BalanceResponse {
    account: String,
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
    Path(account): Path<Identity>,
    State(state): State<ContractHandlerStore<Hyllar>>,
) -> Result<impl IntoResponse, AppError> {
    let store = state.read().await;
    let state = store.state.clone().ok_or(AppError(
        StatusCode::NOT_FOUND,
        anyhow!("Contract '{}' not found", store.contract_name),
    ))?;

    state
        .balance_of(&account.0)
        .map(|balance| BalanceResponse {
            account: account.0,
            balance,
        })
        .map(Json)
        .map_err(|err| AppError(StatusCode::NOT_FOUND, anyhow!("{err}'")))
}

#[derive(Serialize, ToSchema)]
struct AllowanceResponse {
    account: String,
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
    Path((account, spender)): Path<(Identity, Identity)>,
    State(state): State<ContractHandlerStore<Hyllar>>,
) -> Result<impl IntoResponse, AppError> {
    let store = state.read().await;
    let state = store.state.clone().ok_or(AppError(
        StatusCode::NOT_FOUND,
        anyhow!("Contract '{}' not found", store.contract_name),
    ))?;

    state
        .allowance(&account.0, &spender.0)
        .map(|allowance| AllowanceResponse {
            account: account.0,
            spender: spender.0,
            allowance,
        })
        .map(Json)
        .map_err(|err| AppError(StatusCode::NOT_FOUND, anyhow!("{err}'")))
}
