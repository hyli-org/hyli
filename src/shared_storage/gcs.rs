use std::{
    fs::File,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
};

use anyhow::{anyhow, Context, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use bytes::Bytes;
use google_cloud_storage::client::Storage as GcsStorageClient;
use hyli_model::{BlockHeight, LaneId};
use tokio::sync::OnceCell;

use crate::{
    data_availability::block_storage::Blocks, model::DataProposalHash,
    shared_storage::durability::DurabilityBackend, utils::conf::DataProposalDurabilityConf,
};

#[derive(Clone)]
pub struct DpGcsRuntime {
    pub client: GcsStorageClient,
    pub conf: DataProposalDurabilityConf,
    pub genesis_timestamp_folder: Option<String>,
}

#[derive(Clone)]
pub struct GcsDurabilityBackend {
    source: Arc<GcsRuntimeSource>,
}

enum GcsRuntimeSource {
    Lazy {
        conf: DataProposalDurabilityConf,
        data_directory: PathBuf,
        runtime: OnceCell<DpGcsRuntime>,
    },
    #[cfg(test)]
    Fixed(DpGcsRuntime),
}

impl GcsDurabilityBackend {
    pub fn new(data_directory: &Path, conf: DataProposalDurabilityConf) -> Self {
        Self {
            source: Arc::new(GcsRuntimeSource::Lazy {
                conf,
                data_directory: data_directory.to_path_buf(),
                runtime: OnceCell::new(),
            }),
        }
    }

    #[cfg(test)]
    pub fn with_runtime(runtime: DpGcsRuntime) -> Self {
        Self {
            source: Arc::new(GcsRuntimeSource::Fixed(runtime)),
        }
    }

    async fn runtime(&self) -> Result<DpGcsRuntime> {
        match self.source.as_ref() {
            GcsRuntimeSource::Lazy {
                conf,
                data_directory,
                runtime,
            } => Ok(runtime
                .get_or_try_init(|| async {
                    Ok::<DpGcsRuntime, anyhow::Error>(DpGcsRuntime {
                        client: GcsStorageClient::builder().build().await?,
                        conf: conf.clone(),
                        genesis_timestamp_folder: load_genesis_timestamp_folder(data_directory)?,
                    })
                })
                .await?
                .clone()),
            #[cfg(test)]
            GcsRuntimeSource::Fixed(runtime) => Ok(runtime.clone()),
        }
    }

    async fn upload(
        runtime: DpGcsRuntime,
        lane_id: LaneId,
        dp_hash: DataProposalHash,
        payload: Vec<u8>,
    ) -> Result<()> {
        match runtime
            .client
            .write_object(
                bucket_path(&runtime.conf.gcs_bucket),
                object_name(&runtime, &lane_id, &dp_hash),
                Bytes::from(payload),
            )
            .set_if_generation_match(0_i64)
            .send_buffered()
            .await
        {
            Ok(_) => Ok(()),
            Err(err)
                if err
                    .status()
                    .is_some_and(|status| matches!(status.code as i32, 6 | 9)) =>
            {
                Ok(())
            }
            Err(err) => Err(anyhow!(err)),
        }
    }
}

impl DurabilityBackend for GcsDurabilityBackend {
    fn upload_data_proposal(
        &self,
        lane_id: LaneId,
        dp_hash: DataProposalHash,
        payload: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> {
        let backend = self.clone();
        Box::pin(async move {
            let runtime = backend.runtime().await?;
            Self::upload(runtime, lane_id, dp_hash, payload).await
        })
    }
}

fn bucket_path(bucket: &str) -> String {
    if bucket.starts_with("projects/") {
        bucket.to_string()
    } else {
        format!("projects/_/buckets/{bucket}")
    }
}

fn object_name(runtime: &DpGcsRuntime, lane_id: &LaneId, dp_hash: &DataProposalHash) -> String {
    let prefix = effective_prefix(
        &runtime.conf.gcs_prefix,
        runtime.genesis_timestamp_folder.as_deref(),
    );

    format!("{}/data_proposals/{}/{}.bin", prefix, lane_id, dp_hash)
}

fn effective_prefix(gcs_prefix: &str, genesis_timestamp_folder: Option<&str>) -> String {
    match genesis_timestamp_folder {
        Some(genesis_timestamp_folder) => {
            format!("{gcs_prefix}/{genesis_timestamp_folder}")
        }
        None => gcs_prefix.to_string(),
    }
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
struct GenesisTimestampStore {
    timestamp_folder: String,
}

const GENESIS_TIMESTAMP_FILE: &str = "gcs_genesis_timestamp.bin";

fn load_genesis_timestamp_folder(data_directory: &Path) -> Result<Option<String>> {
    let full_path = data_directory.join(PathBuf::from(GENESIS_TIMESTAMP_FILE));
    let mut handle = match File::open(&full_path) {
        Ok(handle) => handle,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return load_genesis_timestamp_folder_from_blocks(data_directory)
        }
        Err(err) => return Err(err).context("Opening genesis timestamp file"),
    };

    let store: GenesisTimestampStore =
        borsh::from_reader(&mut handle).context("Deserializing genesis timestamp file")?;

    chrono::NaiveDateTime::parse_from_str(&store.timestamp_folder, "%Y-%m-%dT%H-%M-%SZ")
        .context("Parsing genesis timestamp")?;

    Ok(Some(store.timestamp_folder))
}

fn load_genesis_timestamp_folder_from_blocks(data_directory: &Path) -> Result<Option<String>> {
    let blocks = Blocks::new(data_directory)?;
    let Some(genesis_block) = blocks.get_by_height(BlockHeight(0))? else {
        return Ok(None);
    };

    Ok(Some(timestamp_to_folder_name(
        genesis_block.consensus_proposal.timestamp.0,
    )))
}

fn timestamp_to_folder_name(timestamp_ms: u128) -> String {
    let secs = (timestamp_ms / 1000) as i64;
    let datetime =
        chrono::DateTime::<chrono::Utc>::from_timestamp(secs, 0).expect("Invalid timestamp");
    datetime.format("%Y-%m-%dT%H-%M-%SZ").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyli_model::{utils::TimestampMs, SignedBlock};

    #[test]
    fn load_genesis_timestamp_folder_falls_back_to_local_blocks() -> Result<()> {
        let tmpdir = tempfile::tempdir()?;
        {
            let mut blocks = Blocks::new(tmpdir.path())?;
            let mut genesis = SignedBlock::default();
            genesis.consensus_proposal.timestamp = TimestampMs(1_743_336_506_000);
            blocks.put(genesis)?;
        }

        let timestamp_folder = load_genesis_timestamp_folder(tmpdir.path())?;

        assert_eq!(timestamp_folder.as_deref(), Some("2025-03-30T12-08-26Z"));
        Ok(())
    }

    #[test]
    fn effective_prefix_uses_genesis_timestamp_folder_when_available() {
        let lane_id = LaneId::default();
        let dp_hash = DataProposalHash::from_hex("deadbeef").unwrap();
        let prefix = effective_prefix("camelot", Some("2026-03-30T12-08-26Z"));
        let object_name = format!("{}/data_proposals/{}/{}.bin", prefix, lane_id, dp_hash);

        assert_eq!(
            object_name,
            format!("camelot/2026-03-30T12-08-26Z/data_proposals/{lane_id}/{dp_hash}.bin")
        );
    }
}
