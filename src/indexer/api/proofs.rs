use super::{BlockPagination, IndexerApiState, TransactionDb};
use api::APITransaction;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};

use crate::model::*;
use hyle_modules::log_error;

#[utoipa::path(
    get,
    tag = "Indexer",
    path = "/proofs",
    responses(
        (status = OK, body = [APITransaction])
    )
)]
pub async fn get_proofs(
    Query(pagination): Query<BlockPagination>,
    State(state): State<IndexerApiState>,
) -> Result<Json<Vec<APITransaction>>, StatusCode> {
    let transactions = log_error!(
        match pagination.start_block {
            Some(start_block) => sqlx::query_as::<_, TransactionDb>(
                r#"
            SELECT t.*, b.timestamp
            FROM transactions t
            LEFT JOIN blocks b ON t.block_hash = b.hash
            WHERE b.height <= $1 and b.height > $2 AND t.transaction_type = 'proof_transaction'
            ORDER BY b.height DESC, t.index DESC
            LIMIT $3
            "#,
            )
            .bind(start_block)
            .bind(start_block - pagination.nb_results.unwrap_or(10)) // Fine if this goes negative
            .bind(pagination.nb_results.unwrap_or(10)),
            None => sqlx::query_as::<_, TransactionDb>(
                r#"
            SELECT t.*, b.timestamp
            FROM transactions t
            LEFT JOIN blocks b ON t.block_hash = b.hash
            WHERE t.transaction_type = 'proof_transaction'
            ORDER BY b.height DESC, t.index DESC
            LIMIT $1
            "#,
            )
            .bind(pagination.nb_results.unwrap_or(10)),
        }
        .fetch_all(&state.db)
        .await
        .map(|db| db.into_iter().map(Into::<APITransaction>::into).collect()),
        "Failed to fetch proofs"
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(transactions))
}

#[utoipa::path(
    get,
    tag = "Indexer",
    params(
        ("height" = String, Path, description = "Block height")
    ),
    path = "/proofs/block/{height}",
    responses(
        (status = OK, body = [APITransaction])
    )
)]
pub async fn get_proofs_by_height(
    Path(height): Path<i64>,
    State(state): State<IndexerApiState>,
) -> Result<Json<Vec<APITransaction>>, StatusCode> {
    let transactions = log_error!(
        sqlx::query_as::<_, TransactionDb>(
            r#"
        SELECT t.*, b.timestamp
        FROM transactions t
        JOIN blocks b ON t.block_hash = b.hash
        WHERE b.height = $1 AND t.transaction_type = 'proof_transaction'
        ORDER BY t.index DESC
        "#,
        )
        .bind(height)
        .fetch_all(&state.db)
        .await
        .map(|db| db.into_iter().map(Into::<APITransaction>::into).collect()),
        "Failed to fetch proofs by height"
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(transactions))
}

#[utoipa::path(
    get,
    tag = "Indexer",
    params(
        ("tx_hash" = String, Path, description = "Tx hash")
    ),
    path = "/proof/hash/{tx_hash}",
    responses(
        (status = OK, body = APITransaction)
    )
)]
pub async fn get_proof_with_hash(
    Path(tx_hash): Path<String>,
    State(state): State<IndexerApiState>,
) -> Result<Json<APITransaction>, StatusCode> {
    let transaction = log_error!(
        sqlx::query_as::<_, TransactionDb>(
            r#"
SELECT 
    tx_hash,
    version,
    transaction_type,
    transaction_status,
    parent_dp_hash,
    block_hash,
    index,
    b.timestamp,
    lane_id
FROM transactions
LEFT JOIN blocks b 
    ON transactions.block_hash = b.hash
WHERE 
    tx_hash = $1
    AND transaction_type = 'proof_transaction'
ORDER BY block_height DESC, index DESC
LIMIT 1;
"#,
        )
        .bind(tx_hash)
        .fetch_optional(&state.db)
        .await
        .map(|db| db.map(Into::<APITransaction>::into)),
        "Failed to fetch proof by hash"
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    match transaction {
        Some(tx) => Ok(Json(tx)),
        None => Err(StatusCode::NOT_FOUND),
    }
}
