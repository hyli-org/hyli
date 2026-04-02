pub mod durability;
pub mod file;
pub mod gcs;

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use hyli_modules::node_state::module::load_current_chain_timestamp;

use crate::utils::conf::DataProposalDurabilityConf;

pub use durability::{DataProposalDurability, DurabilityBackend, NullDurabilityBackend};
pub use file::FileDurabilityBackend;
pub use gcs::{DpGcsRuntime, GcsDurabilityBackend};

pub async fn durability_backend_for_conf(
    data_directory: &Path,
    conf: &DataProposalDurabilityConf,
    run_fast_catchup: bool,
) -> Result<Arc<dyn DurabilityBackend>> {
    if conf.gcs_enabled() {
        if run_fast_catchup {
            load_current_chain_timestamp(data_directory)?;
        }
        let backend = GcsDurabilityBackend::new(conf.clone());
        Ok(Arc::new(backend))
    } else if conf.file_enabled() {
        Ok(Arc::new(FileDurabilityBackend::new(
            data_directory.join("durable_data_proposals"),
            conf.gcs_prefix.clone(),
        )))
    } else {
        Ok(Arc::new(NullDurabilityBackend))
    }
}
