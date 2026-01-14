use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct GCSConf {
    // GCS uploader options
    pub gcs_bucket: String,
    pub gcs_prefix: String,

    pub save_proofs: bool,
    pub save_blocks: bool,

    pub start_block: u64,

    // New batching configuration
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    #[serde(default = "default_batch_timeout_secs")]
    pub batch_timeout_secs: u64,

    #[serde(default = "default_retry_attempts")]
    pub retry_attempts: u8,

    #[serde(default = "default_storage_backend")]
    pub storage_backend: String,
}

fn default_batch_size() -> usize {
    1000
}

fn default_batch_timeout_secs() -> u64 {
    60
}

fn default_retry_attempts() -> u8 {
    5
}

fn default_storage_backend() -> String {
    "gcs".to_string()
}

impl Default for GCSConf {
    fn default() -> Self {
        Self {
            gcs_bucket: String::new(),
            gcs_prefix: String::new(),
            save_proofs: true,
            save_blocks: true,
            start_block: 0,
            batch_size: default_batch_size(),
            batch_timeout_secs: default_batch_timeout_secs(),
            retry_attempts: default_retry_attempts(),
            storage_backend: default_storage_backend(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct GcsUploaderCtx {
    pub gcs_config: GCSConf,
    pub data_directory: PathBuf,
}
