use std::{future::Future, pin::Pin, sync::Arc};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use google_cloud_storage::client::Storage as GcsStorageClient;
use hyli_model::LaneId;
use tokio::sync::OnceCell;

use crate::{
    model::DataProposalHash, shared_storage::durability::DurabilityBackend,
    utils::conf::DataProposalDurabilityConf,
};

#[derive(Clone)]
pub struct DpGcsRuntime {
    pub client: GcsStorageClient,
    pub conf: DataProposalDurabilityConf,
}

#[derive(Clone)]
pub struct GcsDurabilityBackend {
    source: Arc<GcsRuntimeSource>,
}

enum GcsRuntimeSource {
    Lazy {
        conf: DataProposalDurabilityConf,
        runtime: OnceCell<DpGcsRuntime>,
    },
    #[cfg(test)]
    Fixed(DpGcsRuntime),
}

impl GcsDurabilityBackend {
    pub fn new(conf: DataProposalDurabilityConf) -> Self {
        Self {
            source: Arc::new(GcsRuntimeSource::Lazy {
                conf,
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
            GcsRuntimeSource::Lazy { conf, runtime } => Ok(runtime
                .get_or_try_init(|| async {
                    Ok::<DpGcsRuntime, anyhow::Error>(DpGcsRuntime {
                        client: GcsStorageClient::builder().build().await?,
                        conf: conf.clone(),
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
        current_chain_timestamp: Option<String>,
        lane_id: LaneId,
        dp_hash: DataProposalHash,
        payload: Vec<u8>,
    ) -> Result<()> {
        let current_chain_timestamp = current_chain_timestamp.ok_or_else(|| {
            anyhow!("Current chain timestamp is not available for GCS durability")
        })?;
        match runtime
            .client
            .write_object(
                bucket_path(&runtime.conf.gcs_bucket),
                object_name(
                    &runtime.conf.gcs_prefix,
                    &current_chain_timestamp,
                    &lane_id,
                    &dp_hash,
                ),
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
        current_chain_timestamp: Option<String>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> {
        let backend = self.clone();
        Box::pin(async move {
            let runtime = backend.runtime().await?;
            Self::upload(runtime, current_chain_timestamp, lane_id, dp_hash, payload).await
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

fn object_name(
    gcs_prefix: &str,
    current_chain_timestamp: &str,
    lane_id: &LaneId,
    dp_hash: &DataProposalHash,
) -> String {
    let prefix = effective_prefix(gcs_prefix, current_chain_timestamp);
    format!("{}/data_proposals/{}/{}.bin", prefix, lane_id, dp_hash)
}

fn effective_prefix(gcs_prefix: &str, current_chain_timestamp: &str) -> String {
    format!("{gcs_prefix}/{current_chain_timestamp}")
}

pub fn timestamp_to_folder_name(timestamp_ms: u128) -> Result<String> {
    let secs = (timestamp_ms / 1000) as i64;
    let datetime = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, 0)
        .ok_or_else(|| anyhow!("Invalid timestamp: {timestamp_ms}"))?;
    Ok(datetime.format("%Y-%m-%dT%H-%M-%SZ").to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyli_bus::modules::write_manifest;
    use hyli_modules::node_state::module::{
        load_current_chain_timestamp, persist_current_chain_timestamp,
    };
    use hyli_modules::node_state::NodeStateStore;

    fn install_rustls_provider_for_tests() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    }

    #[test]
    fn current_chain_timestamp_round_trip() -> Result<()> {
        let tmpdir = tempfile::tempdir()?;
        let mut store = NodeStateStore::default();
        store.current_chain_timestamp = Some("2025-03-30T12-08-26Z".to_string());
        let persisted = persist_current_chain_timestamp(tmpdir.path(), &store)?
            .expect("timestamp file should be written");
        write_manifest(tmpdir.path(), &[persisted])?;
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
    fn persist_current_chain_timestamp_writes_local_file() -> Result<()> {
        let tmpdir = tempfile::tempdir()?;
        let mut node_state = NodeStateStore::default();
        node_state.current_chain_timestamp = Some("2026-03-30T15-47-06Z".to_string());
        let persisted = persist_current_chain_timestamp(tmpdir.path(), &node_state)?
            .expect("timestamp file should be written");
        write_manifest(tmpdir.path(), &[persisted])?;

        assert_eq!(
            load_current_chain_timestamp(tmpdir.path())?,
            "2026-03-30T15-47-06Z"
        );
        Ok(())
    }

    #[tokio::test]
    async fn gcs_runtime_builds_after_backend_construction() -> Result<()> {
        install_rustls_provider_for_tests();
        let backend = GcsDurabilityBackend::new(DataProposalDurabilityConf {
            gcs_bucket: "test-bucket".to_string(),
            gcs_prefix: "camelot".to_string(),
            save_data_proposals: true,
        });

        let runtime = backend.runtime().await?;

        assert_eq!(runtime.conf.gcs_prefix, "camelot");
        Ok(())
    }
}
