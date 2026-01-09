//! Various data structures

use hyli_crypto::SharedBlstCrypto;
use hyli_modules::modules::SharedBuildApiCtx;

// Re-export
pub use hyli_model::*;

use crate::utils::conf::SharedConf;

pub type LaneOperatorKey = ValidatorPublicKey;

#[derive(Clone)]
pub struct SharedRunContext {
    pub config: SharedConf,
    pub api: SharedBuildApiCtx,
    pub crypto: SharedBlstCrypto,
    pub start_height: Option<BlockHeight>,
}
