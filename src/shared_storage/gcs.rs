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
use hyli_bus::modules::files::NODE_STATE_BIN;
use hyli_model::LaneId;
use hyli_modules::modules::Module;
use hyli_modules::node_state::{module::NodeStateModule, NodeStateStore};
use tokio::sync::OnceCell;

use crate::{
    model::DataProposalHash, shared_storage::durability::DurabilityBackend,
    utils::conf::DataProposalDurabilityConf,
};

#[derive(Clone)]
pub struct DpGcsRuntime {
    pub client: GcsStorageClient,
    pub conf: DataProposalDurabilityConf,
    pub current_chain_timestamp: String,
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

    pub async fn initialize(&self) -> Result<()> {
        let _ = self.runtime().await?;
        Ok(())
    }

    async fn runtime(&self) -> Result<DpGcsRuntime> {
        match self.source.as_ref() {
            GcsRuntimeSource::Lazy {
                conf,
                data_directory,
                runtime,
            } => Ok(runtime
                .get_or_try_init(|| async {
                    let current_chain_timestamp = load_current_chain_timestamp(data_directory)
                        .with_context(|| {
                            format!(
                                "Loading current chain timestamp from {}",
                                data_directory.display()
                            )
                        })?;

                    Ok::<DpGcsRuntime, anyhow::Error>(DpGcsRuntime {
                        client: GcsStorageClient::builder().build().await?,
                        conf: conf.clone(),
                        current_chain_timestamp,
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

pub fn persist_current_chain_timestamp(
    data_directory: &Path,
    current_chain_timestamp: &str,
) -> Result<()> {
    let store = CurrentChainTimestampStore {
        timestamp_folder: current_chain_timestamp.to_string(),
    };
    let path = data_directory.join(CURRENT_CHAIN_TIMESTAMP_FILE);
    let mut file = File::create(&path).context("Creating current chain timestamp file")?;
    borsh::to_writer(&mut file, &store).context("Serializing current chain timestamp file")?;
    Ok(())
}

pub fn persist_current_chain_timestamp_for_block(
    data_directory: &Path,
    timestamp_ms: u128,
) -> Result<()> {
    let store = CurrentChainTimestampStore {
        timestamp_folder: timestamp_to_folder_name(timestamp_ms)?,
    };
    persist_current_chain_timestamp(data_directory, &store.timestamp_folder)
}

fn bucket_path(bucket: &str) -> String {
    if bucket.starts_with("projects/") {
        bucket.to_string()
    } else {
        format!("projects/_/buckets/{bucket}")
    }
}

fn object_name(runtime: &DpGcsRuntime, lane_id: &LaneId, dp_hash: &DataProposalHash) -> String {
    let prefix = effective_prefix(&runtime.conf.gcs_prefix, &runtime.current_chain_timestamp);

    format!("{}/data_proposals/{}/{}.bin", prefix, lane_id, dp_hash)
}

fn effective_prefix(gcs_prefix: &str, current_chain_timestamp: &str) -> String {
    format!("{gcs_prefix}/{current_chain_timestamp}")
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
struct CurrentChainTimestampStore {
    timestamp_folder: String,
}

const CURRENT_CHAIN_TIMESTAMP_FILE: &str = "current_chain_timestamp.bin";

fn load_current_chain_timestamp(data_directory: &Path) -> Result<String> {
    let full_path = data_directory.join(CURRENT_CHAIN_TIMESTAMP_FILE);
    let mut handle = File::open(&full_path).with_context(|| {
        format!(
            "Opening required current chain timestamp file {}",
            full_path.display()
        )
    })?;

    let store: CurrentChainTimestampStore =
        borsh::from_reader(&mut handle).context("Deserializing current chain timestamp file")?;

    chrono::NaiveDateTime::parse_from_str(&store.timestamp_folder, "%Y-%m-%dT%H-%M-%SZ")
        .context("Parsing current chain timestamp")?;

    Ok(store.timestamp_folder)
}

pub fn persist_current_chain_timestamp_from_node_state(data_directory: &Path) -> Result<()> {
    let store =
        NodeStateModule::load_from_disk::<NodeStateStore>(data_directory, NODE_STATE_BIN.as_ref())?
            .context("Missing node_state.bin while loading current chain timestamp")?;
    let current_chain_timestamp = store.current_chain_timestamp.context(
        "Missing current_chain_timestamp in node_state.bin while loading chain metadata",
    )?;
    persist_current_chain_timestamp(data_directory, &current_chain_timestamp)
}

fn timestamp_to_folder_name(timestamp_ms: u128) -> Result<String> {
    let secs = (timestamp_ms / 1000) as i64;
    let datetime = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, 0)
        .ok_or_else(|| anyhow!("Invalid timestamp: {timestamp_ms}"))?;
    Ok(datetime.format("%Y-%m-%dT%H-%M-%SZ").to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyli_bus::modules::write_manifest;
    use hyli_modules::node_state::module::NodeStateModule;

    fn install_rustls_provider_for_tests() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    }

    #[test]
    fn current_chain_timestamp_round_trip() -> Result<()> {
        let tmpdir = tempfile::tempdir()?;
        persist_current_chain_timestamp_for_block(tmpdir.path(), 1_743_336_506_000)?;
        let decoded = load_current_chain_timestamp(tmpdir.path())?;

        assert_eq!(decoded, "2025-03-30T12-08-26Z");
        Ok(())
    }

    #[test]
    fn load_current_chain_timestamp_requires_file() {
        let tmpdir = tempfile::tempdir().unwrap();

        assert!(load_current_chain_timestamp(tmpdir.path()).is_err());
    }

    #[test]
    fn effective_prefix_uses_current_chain_timestamp() {
        let lane_id = LaneId::default();
        let dp_hash = DataProposalHash::from_hex("deadbeef").unwrap();
        let prefix = effective_prefix("camelot", "2026-03-30T12-08-26Z");
        let object_name = format!("{}/data_proposals/{}/{}.bin", prefix, lane_id, dp_hash);

        assert_eq!(
            object_name,
            format!("camelot/2026-03-30T12-08-26Z/data_proposals/{lane_id}/{dp_hash}.bin")
        );
    }

    #[test]
    fn persist_current_chain_timestamp_from_node_state_writes_local_file() -> Result<()> {
        let tmpdir = tempfile::tempdir()?;
        let mut node_state = NodeStateStore::default();
        node_state.current_chain_timestamp = Some("2026-03-30T15-47-06Z".to_string());

        let checksum =
            NodeStateModule::save_on_disk(tmpdir.path(), NODE_STATE_BIN.as_ref(), &node_state)?;
        write_manifest(
            tmpdir.path(),
            &[(tmpdir.path().join(NODE_STATE_BIN), checksum)],
        )?;
        persist_current_chain_timestamp_from_node_state(tmpdir.path())?;

        assert_eq!(
            load_current_chain_timestamp(tmpdir.path())?,
            "2026-03-30T15-47-06Z"
        );
        Ok(())
    }

    #[tokio::test]
    async fn gcs_runtime_loads_timestamp_written_after_backend_construction() -> Result<()> {
        install_rustls_provider_for_tests();
        let tmpdir = tempfile::tempdir()?;
        let backend = GcsDurabilityBackend::new(
            tmpdir.path(),
            DataProposalDurabilityConf {
                gcs_bucket: "test-bucket".to_string(),
                gcs_prefix: "camelot".to_string(),
                save_data_proposals: true,
            },
        );

        persist_current_chain_timestamp(tmpdir.path(), "2026-03-30T15-47-06Z")?;

        let runtime = backend.runtime().await?;

        assert_eq!(runtime.current_chain_timestamp, "2026-03-30T15-47-06Z");
        Ok(())
    }

    #[tokio::test]
    async fn gcs_runtime_initialize_requires_existing_timestamp_file() {
        install_rustls_provider_for_tests();
        let tmpdir = tempfile::tempdir().unwrap();
        let backend = GcsDurabilityBackend::new(
            tmpdir.path(),
            DataProposalDurabilityConf {
                gcs_bucket: "test-bucket".to_string(),
                gcs_prefix: "camelot".to_string(),
                save_data_proposals: true,
            },
        );

        let err = backend.initialize().await.unwrap_err();

        assert!(
            err.to_string().contains("current chain timestamp"),
            "unexpected error: {err:#}"
        );
    }
}
