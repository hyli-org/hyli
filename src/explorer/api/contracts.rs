use super::ExplorerApiState;
use api::{APIContract, APIContractHistory, APIContractState, ContractChangeType};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use serde_json::Value;

use crate::model::*;
use hyli_modules::log_error;

#[derive(sqlx::FromRow, Debug)]
pub struct ContractDb {
    // Struct for the contracts table
    pub tx_hash: TxHash,     // Corresponds to the registration transaction hash
    pub verifier: String,    // Verifier of the contract
    pub program_id: Vec<u8>, // Program ID
    pub state_commitment: Vec<u8>, // state commitment of the contract
    pub soft_timeout: Option<i64>,
    pub hard_timeout: Option<i64>,
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
            tx_hash: val.tx_hash,
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
    State(state): State<ExplorerApiState>,
) -> Result<Json<Vec<APIContract>>, StatusCode> {
    let contract = log_error!(
        sqlx::query_as::<_, ContractDb>(
            r#"
SELECT
    c.*,
    COUNT(tx_c.*) AS total_tx,
    COUNT(t.*) FILTER (WHERE t.transaction_status = 'sequenced') AS unsettled_tx,
    min(t.block_height) FILTER (WHERE t.transaction_status = 'sequenced') as earliest_unsettled
FROM contracts AS c
LEFT JOIN txs_contracts as tx_c
    on tx_c.contract_name = c.contract_name
LEFT JOIN transactions AS t
    ON t.parent_dp_hash = tx_c.parent_dp_hash
    AND t.tx_hash       = tx_c.tx_hash
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
    State(state): State<ExplorerApiState>,
) -> Result<Json<APIContract>, StatusCode> {
    let contract = log_error!(
        sqlx::query_as::<_, ContractDb>(
            r#"
        SELECT
          c.*,
          COUNT(t.*)                             			          AS total_tx,
          COUNT(t.*)
            FILTER (WHERE t.transaction_status = 'sequenced')   AS unsettled_tx,
          (
            SELECT min(bl.height)
            FROM blocks bl
            JOIN transactions t2 ON t2.block_hash = bl.hash
            WHERE t2.transaction_status = 'sequenced'
              AND EXISTS (
                SELECT 1
                FROM txs_contracts tx_c2
                WHERE tx_c2.parent_dp_hash = t2.parent_dp_hash
                  AND tx_c2.tx_hash = t2.tx_hash
                  AND tx_c2.contract_name = c.contract_name
              )
          ) AS earliest_unsettled
        FROM contracts AS c
        left join txs_contracts as tx_c
          on tx_c.contract_name = c.contract_name
        LEFT JOIN transactions AS t
          ON t.parent_dp_hash = tx_c.parent_dp_hash
         AND t.tx_hash       = tx_c.tx_hash
        WHERE c.contract_name = $1
        GROUP BY c.contract_name;
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

#[derive(sqlx::FromRow, Debug)]
pub struct ContractHistoryDb {
    pub contract_name: String,
    #[sqlx(try_from = "i64")]
    pub block_height: u64,
    pub tx_index: i32,
    pub change_type: Vec<ContractChangeType>,
    pub verifier: String,
    pub program_id: Vec<u8>,
    pub state_commitment: Vec<u8>,
    pub soft_timeout: Option<i64>,
    pub hard_timeout: Option<i64>,
    pub tx_hash: TxHash,
    pub parent_dp_hash: DataProposalHash,
}

impl From<ContractHistoryDb> for APIContractHistory {
    fn from(val: ContractHistoryDb) -> Self {
        APIContractHistory {
            contract_name: val.contract_name,
            block_height: val.block_height,
            tx_index: val.tx_index,
            change_type: val.change_type,
            verifier: val.verifier,
            program_id: val.program_id,
            state_commitment: val.state_commitment,
            soft_timeout: val.soft_timeout,
            hard_timeout: val.hard_timeout,
            tx_hash: val.tx_hash,
            parent_dp_hash: val.parent_dp_hash,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ContractHistoryQuery {
    pub change_type: Option<String>,
    pub from_height: Option<i64>,
    pub to_height: Option<i64>,
}

#[utoipa::path(
    get,
    tag = "Indexer",
    params(
        ("contract_name" = String, Path, description = "Contract name"),
        ("change_type" = Option<String>, Query, description = "Filter by change type (comma-separated)"),
        ("from_height" = Option<i64>, Query, description = "Start block height"),
        ("to_height" = Option<i64>, Query, description = "End block height"),
    ),
    path = "/contract/{contract_name}/history",
    responses(
        (status = OK, body = [APIContractHistory])
    )
)]
pub async fn get_contract_history(
    Path(contract_name): Path<String>,
    Query(query): Query<ContractHistoryQuery>,
    State(state): State<ExplorerApiState>,
) -> Result<Json<Vec<APIContractHistory>>, StatusCode> {
    let change_types = match query.change_type.as_deref().map(|value| {
        value
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
            .map(|item| {
                serde_json::from_value::<ContractChangeType>(Value::String(item.to_string())).ok()
            })
            .collect::<Vec<_>>()
    }) {
        Some(values) => {
            if values.iter().any(|item| item.is_none()) {
                return Err(StatusCode::BAD_REQUEST);
            }
            let parsed = values.into_iter().flatten().collect::<Vec<_>>();
            if parsed.is_empty() {
                None
            } else {
                Some(parsed)
            }
        }
        None => None,
    };

    let mut sql = String::from(
        r#"
        SELECT
            contract_name,
            block_height,
            tx_index,
            change_type,
            verifier,
            program_id,
            state_commitment,
            soft_timeout,
            hard_timeout,
            tx_hash,
            parent_dp_hash
        FROM contract_history
        WHERE contract_name = $1
        "#,
    );

    let mut param_count = 1;

    if change_types
        .as_ref()
        .map(|types| !types.is_empty())
        .unwrap_or(false)
    {
        param_count += 1;
        sql.push_str(&format!(
            " AND change_type && ${param_count}::contract_change_type[]"
        ));
    }

    if query.from_height.is_some() {
        param_count += 1;
        sql.push_str(&format!(" AND block_height >= ${param_count}"));
    }

    if query.to_height.is_some() {
        param_count += 1;
        sql.push_str(&format!(" AND block_height <= ${param_count}"));
    }

    sql.push_str(" ORDER BY block_height DESC, tx_index DESC");

    let mut query_builder = sqlx::query_as::<_, ContractHistoryDb>(&sql).bind(contract_name);

    if let Some(change_types) = change_types {
        query_builder = query_builder.bind(change_types);
    }

    if let Some(from_height) = query.from_height {
        query_builder = query_builder.bind(from_height);
    }

    if let Some(to_height) = query.to_height {
        query_builder = query_builder.bind(to_height);
    }

    let history = log_error!(
        query_builder.fetch_all(&state.db).await.map(|db| db
            .into_iter()
            .map(Into::<APIContractHistory>::into)
            .collect()),
        "Failed to fetch contract history"
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(history))
}
