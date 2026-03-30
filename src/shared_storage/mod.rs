pub mod durability;
pub mod file;
pub mod gcs;

use std::path::Path;
use std::sync::Arc;

use crate::utils::conf::DataProposalDurabilityConf;

pub use durability::{DataProposalDurability, DurabilityBackend, NullDurabilityBackend};
pub use file::FileDurabilityBackend;
pub use gcs::{DpGcsRuntime, GcsDurabilityBackend};

pub fn durability_backend_for_conf(
    data_directory: &Path,
    conf: &DataProposalDurabilityConf,
) -> Arc<dyn DurabilityBackend> {
    if conf.gcs_enabled() {
        Arc::new(GcsDurabilityBackend::new(data_directory, conf.clone()))
    } else if conf.file_enabled() {
        Arc::new(FileDurabilityBackend::new(
            data_directory.join("durable_data_proposals"),
            conf.gcs_prefix.clone(),
        ))
    } else {
        Arc::new(NullDurabilityBackend)
    }
}
