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
        lane_id: LaneId,
        dp_hash: DataProposalHash,
        payload: Vec<u8>,
    ) -> Result<()> {
        match runtime
            .client
            .write_object(
                bucket_path(&runtime.conf.gcs_bucket),
                object_name(&runtime.conf, &lane_id, &dp_hash),
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

fn object_name(
    conf: &DataProposalDurabilityConf,
    lane_id: &LaneId,
    dp_hash: &DataProposalHash,
) -> String {
    format!(
        "{}/data_proposals/{}/{}.bin",
        conf.gcs_prefix, lane_id, dp_hash
    )
}
