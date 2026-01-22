use std::path::PathBuf;

use anyhow::Result;
use bytes::Bytes;
use google_cloud_storage::client::Storage;
use sdk::{DataEvent, DataProposalHash, TxHash};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::bus::{BusMessage, SharedMessageBus};
use crate::modules::Module;
use crate::{log_error, module_bus_client, module_handle_messages};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GCSRequest {
    ProofUpload {
        proof: Vec<u8>,
        tx_hash: TxHash,
        parent_data_proposal_hash: DataProposalHash,
    },
}

impl BusMessage for GCSRequest {}

module_bus_client! {
    #[derive(Debug)]
    struct GcsUploaderBusClient {
        receiver(DataEvent),
        receiver(GCSRequest),
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct GCSConf {
    // GCS uploader options
    pub gcs_bucket: String,
    pub gcs_prefix: String,

    pub save_proofs: bool,
    pub save_blocks: bool,

    pub start_block: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GcsUploaderCtx {
    pub gcs_config: GCSConf,
    pub data_directory: PathBuf,
}

pub struct GcsUploader {
    ctx: GcsUploaderCtx,
    bus: GcsUploaderBusClient,
    gcs_client: Storage,
}

impl Module for GcsUploader {
    type Context = GcsUploaderCtx;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let bus = GcsUploaderBusClient::new_from_bus(bus.new_handle()).await;
        let gcs_client = Storage::builder().build().await?;
        Ok(GcsUploader {
            ctx,
            bus,
            gcs_client,
        })
    }

    async fn run(&mut self) -> Result<()> {
        self.start().await?;
        Ok(())
    }
}

impl GcsUploader {
    pub async fn start(&mut self) -> Result<()> {
        module_handle_messages! {
            on_self self,
            listen<DataEvent> event => {
                // FIXME: this implies that every SignedBlock will be in GCSUploader's Bus channel
                if !self.ctx.gcs_config.save_blocks {
                    debug!("Skipping block upload as save_blocks is disabled");
                    return Ok(());
                }
                self.handle_data_availability_event(event).await?;
            },
            listen<GCSRequest> request => {
                if !self.ctx.gcs_config.save_proofs {
                    debug!("Skipping GCS request as save_proofs is disabled");
                    return Ok(());
                }
                debug!("Received GCS request: {:?}", request);
                match request {
                    GCSRequest::ProofUpload { proof, tx_hash, parent_data_proposal_hash } => {
                        debug!("Processing proof upload of size: {}", proof.len());
                        _ = log_error!(
                            self.upload_proof(proof, tx_hash, parent_data_proposal_hash).await,
                            "Uploading proof to GCS"
                        );
                    }
                }
            }
        };
        Ok(())
    }

    async fn handle_data_availability_event(&mut self, event: DataEvent) -> Result<()> {
        let DataEvent::OrderedSignedBlock(block) = event;
        let block_height = block.height().0;
        let prefix = &self.ctx.gcs_config.gcs_prefix;
        let object_name = format!("{prefix}/block_{block_height}.bin");
        let data = borsh::to_vec(&block)?;
        // Log, but ignore errors - could be that we already dumped this, or some other thing - we'll do our best to store everything.
        match self
            .gcs_client
            .write_object(
                self.gcs_bucket_path(),
                object_name.clone(),
                Bytes::from(data),
            )
            .set_if_generation_match(0_i64)
            .send_unbuffered()
            .await
        {
            Ok(_) => {
                tracing::info!(
                    "Successfully uploaded block {} to GCS bucket {}",
                    block_height,
                    self.ctx.gcs_config.gcs_bucket
                );
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to upload block {} to GCS bucket {}: {}",
                    block_height,
                    self.ctx.gcs_config.gcs_bucket,
                    e
                );
            }
        }
        Ok(())
    }

    pub async fn upload_proof(
        &self,
        proof: Vec<u8>,
        tx_hash: TxHash,
        parent_data_proposal_hash: DataProposalHash,
    ) -> Result<()> {
        // Upload to GCS if client is configured
        let prefix = &self.ctx.gcs_config.gcs_prefix;
        let object_name = format!(
            "{}/proofs/{}/{}.bin",
            prefix, parent_data_proposal_hash, tx_hash
        );

        match self
            .gcs_client
            .write_object(
                self.gcs_bucket_path(),
                object_name.clone(),
                Bytes::from(proof),
            )
            .set_if_generation_match(0_i64)
            .send_buffered()
            .await
        {
            Ok(_) => {
                tracing::info!(
                    "Successfully uploaded proof {} to GCS bucket {}",
                    tx_hash,
                    self.ctx.gcs_config.gcs_bucket
                );
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to upload proof {} to GCS bucket {}: {}",
                    tx_hash,
                    self.ctx.gcs_config.gcs_bucket,
                    e
                );
            }
        }
        Ok(())
    }

    fn gcs_bucket_path(&self) -> String {
        let bucket = &self.ctx.gcs_config.gcs_bucket;
        if bucket.starts_with("projects/") {
            bucket.clone()
        } else {
            format!("projects/_/buckets/{bucket}")
        }
    }
}
