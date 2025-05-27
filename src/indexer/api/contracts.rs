use super::{IndexerApiState, TxHashDb};
use api::{APIContract, APIContractState};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};

use crate::model::*;
use hyle_modules::log_error;

#[derive(sqlx::FromRow, Debug)]
pub struct ContractDb {
    // Struct for the contracts table
    pub tx_hash: TxHashDb,   // Corresponds to the registration transaction hash
    pub verifier: String,    // Verifier of the contract
    pub program_id: Vec<u8>, // Program ID
    pub state_commitment: Vec<u8>, // state commitment of the contract
    pub contract_name: String, // Contract name
    #[sqlx(try_from = "i64")]
    pub total_tx: u64, // Total number of transactions associated with the contract
    #[sqlx(try_from = "i64")]
    pub unsettled_tx: u64, // Total number of unsettled transactions
    pub earliest_unsettled: Option<i64>, // Block height of the earliest unsettled transaction
}

impl From<ContractDb> for APIContract {
    fn from(val: ContractDb) -> Self {
        APIContract {
            tx_hash: val.tx_hash.0,
            verifier: val.verifier,
            program_id: val.program_id,
            state_commitment: val.state_commitment,
            contract_name: val.contract_name,
            total_tx: val.total_tx,
            unsettled_tx: val.unsettled_tx,
            earliest_unsettled: val.earliest_unsettled.map(|a| BlockHeight(a as u64)),
        }
    }
}

#[derive(sqlx::FromRow, Debug)]
pub struct ContractStateDb {
    // Struct for the contract_state table
    pub contract_name: String,             // Name of the contract
    pub block_hash: ConsensusProposalHash, // Hash of the block where the state is captured
    pub state_commitment: Vec<u8>,         // The contract state stored in JSON format
}

impl From<ContractStateDb> for APIContractState {
    fn from(value: ContractStateDb) -> Self {
        APIContractState {
            contract_name: value.contract_name,
            block_hash: value.block_hash,
            state_commitment: value.state_commitment,
        }
    }
}

#[utoipa::path(
    get,
    tag = "Indexer",
    path = "/contracts",
    responses(
        (status = OK, body = [APIContract])
    )
)]
pub async fn list_contracts(
    State(state): State<IndexerApiState>,
) -> Result<Json<Vec<APIContract>>, StatusCode> {
    let contract = log_error!(
        sqlx::query_as::<_, ContractDb>(
            r#"
        SELECT
          c.*,
          COUNT(DISTINCT (t.tx_hash, t.parent_dp_hash))                             AS total_tx,
          COUNT(DISTINCT (t.tx_hash, t.parent_dp_hash)) 
            FILTER (WHERE t.transaction_status = 'sequenced')   AS unsettled_tx,
          (
            SELECT bl.height
                FROM transactions t2
                JOIN blobs b2
                  ON t2.parent_dp_hash = b2.parent_dp_hash
                 AND t2.tx_hash       = b2.tx_hash
                JOIN blocks bl
                  ON t2.block_hash = bl.hash
                WHERE b2.contract_name     = c.contract_name
                  AND t2.transaction_status = 'sequenced'
                ORDER BY bl.height ASC
                LIMIT 1
          ) AS earliest_unsettled
        FROM contracts AS c
        LEFT JOIN blobs AS b
          ON b.contract_name = c.contract_name
        LEFT JOIN transactions AS t
          ON t.parent_dp_hash = b.parent_dp_hash
         AND t.tx_hash       = b.tx_hash
        GROUP BY c.contract_name
"#
        )
        .fetch_all(&state.db)
        .await
        .map(|db| db.into_iter().map(Into::<APIContract>::into).collect()),
        "Failed to fetch contracts"
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(contract))
}

#[utoipa::path(
    get,
    tag = "Indexer",
    params(
        ("contract_name" = String, Path, description = "Contract name"),
    ),
    path = "/contract/{contract_name}",
    responses(
        (status = OK, body = APIContract)
    )
)]
pub async fn get_contract(
    Path(contract_name): Path<String>,
    State(state): State<IndexerApiState>,
) -> Result<Json<APIContract>, StatusCode> {
    let contract = log_error!(
        sqlx::query_as::<_, ContractDb>(
            r#"
        SELECT
          c.*,
          COUNT(DISTINCT (t.tx_hash, t.parent_dp_hash))                             AS total_tx,
          COUNT(DISTINCT (t.tx_hash, t.parent_dp_hash)) 
            FILTER (WHERE t.transaction_status = 'sequenced')   AS unsettled_tx,
          (
            SELECT bl.height
                FROM transactions t2
                JOIN blobs b2
                  ON t2.parent_dp_hash = b2.parent_dp_hash
                 AND t2.tx_hash       = b2.tx_hash
                JOIN blocks bl
                  ON t2.block_hash = bl.hash
                WHERE b2.contract_name     = c.contract_name
                  AND t2.transaction_status = 'sequenced'
                ORDER BY bl.height ASC
                LIMIT 1
          ) AS earliest_unsettled
        FROM contracts AS c
        LEFT JOIN blobs AS b
          ON b.contract_name = c.contract_name
        LEFT JOIN transactions AS t
          ON t.parent_dp_hash = b.parent_dp_hash
         AND t.tx_hash       = b.tx_hash
        WHERE c.contract_name = $1 
        GROUP BY c.contract_name
        "#
        )
        .bind(contract_name)
        .fetch_optional(&state.db)
        .await
        .map(|db| db.map(Into::<APIContract>::into)),
        "Failed to fetch contract"
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    match contract {
        Some(contract) => Ok(Json(contract)),
        None => Err(StatusCode::NOT_FOUND),
    }
}

#[utoipa::path(
    get,
    tag = "Indexer",
    params(
        ("contract_name" = String, Path, description = "Contract name"),
        ("height" = String, Path, description = "Block height")
    ),
    path = "/state/contract/{contract_name}/block/{height}",
    responses(
        (status = OK, body = APIContractState)
    )
)]
pub async fn get_contract_state_by_height(
    Path((contract_name, height)): Path<(String, i64)>,
    State(state): State<IndexerApiState>,
) -> Result<Json<APIContractState>, StatusCode> {
    let contract = log_error!(
        sqlx::query_as::<_, ContractStateDb>(
            r#"
        SELECT cs.*
        FROM contract_state cs
        JOIN blocks b ON cs.block_hash = b.hash
        WHERE contract_name = $1 AND height = $2"#,
        )
        .bind(contract_name)
        .bind(height)
        .fetch_optional(&state.db)
        .await
        .map(|db| db.map(Into::<APIContractState>::into)),
        "Failed to fetch contract state by height"
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    match contract {
        Some(contract) => Ok(Json(contract)),
        None => Err(StatusCode::NOT_FOUND),
    }
}
