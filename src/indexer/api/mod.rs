use super::IndexerApiState;
use utoipa::OpenApi;

#[derive(Debug, serde::Deserialize)]
pub struct BlockPagination {
    pub start_block: Option<i64>,
    pub nb_results: Option<i64>,
}

#[derive(OpenApi)]
#[openapi(paths(get_blocks, get_data_proposals_by_lane, get_data_proposal_by_hash))]
pub(super) struct IndexerAPI;

mod blobs;
mod blocks;
mod contracts;
mod data_proposals;
mod proofs;
mod stats;
mod transactions;

pub use blobs::*;
pub use blocks::*;
pub use contracts::*;
pub use data_proposals::*;
pub use proofs::*;
pub use stats::*;
pub use transactions::*;
