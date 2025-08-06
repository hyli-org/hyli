//! Various data structures

use hyle_crypto::SharedBlstCrypto;
use hyle_modules::modules::SharedBuildApiCtx;

// Re-export
pub use hyle_model::*;

use hyle_modules::utils::conf::SharedConf;
#[derive(Clone)]
pub struct SharedRunContext {
    pub config: SharedConf,
    pub api: SharedBuildApiCtx,
    pub crypto: SharedBlstCrypto,
}
