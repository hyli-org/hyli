//! Various data structures

use hyli_crypto::SharedBlstCrypto;
use hyli_modules::modules::SharedBuildApiCtx;

// Re-export
pub use hyli_model::*;

use crate::utils::conf::SharedConf;
use crate::verifier_workers::ProofVerifierService;
use std::sync::Arc;

pub type LaneOperatorKey = ValidatorPublicKey;

#[derive(Clone)]
pub struct SharedRunContext {
    pub config: SharedConf,
    pub api: SharedBuildApiCtx,
    pub crypto: SharedBlstCrypto,
    pub start_height: Option<BlockHeight>,
    pub start_timestamp: utils::TimestampMs,
    pub proof_verifiers: Arc<ProofVerifierService>,
}
