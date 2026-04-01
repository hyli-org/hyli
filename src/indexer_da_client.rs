use std::{
    collections::BTreeMap,
    path::PathBuf,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context, Result};
use google_cloud_storage::client::Storage as GcsStorageClient;
use hyli_bus::modules::ModulePersistOutput;
use hyli_model::{
    BlockHeight, DataAvailabilityEvent, DataAvailabilityRequest, DataProposal, Hashed, LaneId,
    SignedBlock, StoredSignedBlock,
};
use hyli_modules::{
    bus::{BusClientSender, SharedMessageBus},
    module_bus_client, module_handle_messages,
    modules::Module,
    node_state::{
        module::{load_current_chain_timestamp, persist_current_chain_timestamp, NodeStateModule},
        NodeStateStore,
    },
    utils::da_codec::DataAvailabilityClient,
};
use tracing::{debug, info, warn};

use crate::{
    model::{DataEvent, MempoolStatusEvent},
    shared_storage::gcs::timestamp_to_folder_name,
    utils::conf::DataProposalDurabilityConf,
};

pub struct StoredDaIndexerClientCtx {
    pub data_directory: PathBuf,
    pub da_read_from: String,
    pub da_fallback_addresses: Vec<String>,
    pub timeout_client_secs: u64,
    pub gcs_conf: DataProposalDurabilityConf,
}

module_bus_client! {
#[derive(Debug)]
struct StoredDaIndexerClientBus {
    sender(DataEvent),
    sender(MempoolStatusEvent),
}
}

pub struct StoredDaIndexerClient {
    bus: StoredDaIndexerClientBus,
    config: StoredDaIndexerClientCtx,
    da_client: Option<DataAvailabilityClient>,
    gcs_client: GcsStorageClient,
    current_block: BlockHeight,
    current_chain_timestamp: Option<String>,
    block_buffer: BTreeMap<BlockHeight, StoredSignedBlock>,
    deadline: Instant,
}

impl Module for StoredDaIndexerClient {
    type Context = StoredDaIndexerClientCtx;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let start_block_in_file = match NodeStateModule::load_from_disk::<BlockHeight>(
            &ctx.data_directory,
            "da_start_height.bin".as_ref(),
        )? {
            Some(height) => height,
            None => {
                warn!("Starting StoredDaIndexerClient from default block height.");
                BlockHeight(0)
            }
        };

        let bus = StoredDaIndexerClientBus::new_from_bus(bus.new_handle()).await;
        let gcs_client = GcsStorageClient::builder().build().await?;
        let current_chain_timestamp = load_current_chain_timestamp(&ctx.data_directory).ok();

        Ok(Self {
            bus,
            config: ctx,
            da_client: None,
            gcs_client,
            current_block: start_block_in_file,
            current_chain_timestamp,
            block_buffer: BTreeMap::new(),
            deadline: Instant::now(),
        })
    }

    async fn run(&mut self) -> Result<()> {
        self.start().await
    }

    async fn persist(&mut self) -> Result<ModulePersistOutput> {
        let file = PathBuf::from("da_start_height.bin");
        let checksum =
            NodeStateModule::save_on_disk(&self.config.data_directory, &file, &self.current_block)?;

        let mut persisted = vec![(self.config.data_directory.join(file), checksum)];
        if let Some(timestamp_folder) = self.current_chain_timestamp.clone() {
            let mut store = NodeStateStore::default();
            store.current_chain_timestamp = Some(timestamp_folder);
            if let Some(timestamp_file) =
                persist_current_chain_timestamp(&self.config.data_directory, &store)?
            {
                persisted.push(timestamp_file);
            }
        }
        Ok(persisted)
    }
}

impl StoredDaIndexerClient {
    async fn start(&mut self) -> Result<()> {
        self.connect().await?;

        info!(
            "Starting DA client for stored signed blocks at block {}",
            self.current_block
        );

        module_handle_messages! {
            on_self self,
            recv = self.recv_next() => {
                match recv? {
                    Some(DataAvailabilityEvent::StoredSignedBlock(block)) => {
                        self.handle_stored_signed_block(block).await?;
                    }
                    Some(DataAvailabilityEvent::MempoolStatusEvent(status)) => {
                        self.bus.send_waiting_if_full(status).await?;
                    }
                    Some(DataAvailabilityEvent::BlockNotFound(height)) => {
                        return Err(anyhow!("Stored block {height} was not found by DA server"));
                    }
                    Some(DataAvailabilityEvent::SignedBlock(_)) => {
                        debug!("Ignoring signed block on stored block client");
                    }
                    None => {
                        warn!("Stored DA stream closed. Reconnecting after sleeping 1s...");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        self.connect().await?;
                    }
                }
            }
        };
        Ok(())
    }

    async fn connect(&mut self) -> Result<()> {
        let mut addresses = vec![self.config.da_read_from.clone()];
        addresses.extend(self.config.da_fallback_addresses.clone());

        for address in addresses {
            match DataAvailabilityClient::connect_with_opts(
                "stored_da_indexer",
                Some(1024 * 1024 * 1024),
                address.clone(),
            )
            .await
            {
                Ok(mut client) => {
                    client
                        .send(DataAvailabilityRequest::StreamStoredFromHeight(
                            self.current_block,
                        ))
                        .await?;
                    self.deadline =
                        Instant::now() + Duration::from_secs(self.config.timeout_client_secs);
                    self.da_client = Some(client);
                    return Ok(());
                }
                Err(err) => {
                    warn!("Failed to connect to DA server {}: {}", address, err);
                }
            }
        }

        Err(anyhow!("Failed to connect to any DA server"))
    }

    async fn recv_next(&mut self) -> Result<Option<DataAvailabilityEvent>> {
        let Some(client) = self.da_client.as_mut() else {
            return Ok(None);
        };

        match tokio::time::timeout(
            self.deadline.saturating_duration_since(Instant::now()),
            client.recv(),
        )
        .await
        {
            Ok(event) => {
                self.deadline =
                    Instant::now() + Duration::from_secs(self.config.timeout_client_secs);
                Ok(event)
            }
            Err(_) => {
                warn!("Stored DA client timed out. Reconnecting.");
                self.connect().await?;
                Ok(None)
            }
        }
    }

    async fn handle_stored_signed_block(&mut self, block: StoredSignedBlock) -> Result<()> {
        let block_height = block.height();

        match block_height.cmp(&self.current_block) {
            std::cmp::Ordering::Less => {
                warn!("Ignoring past stored block {}", block_height);
            }
            std::cmp::Ordering::Equal => {
                self.emit_reconstructed_block(block).await?;
                self.current_block = block_height + 1;
                self.process_buffered_blocks().await?;
            }
            std::cmp::Ordering::Greater => {
                for missing_height in self.current_block.0..block_height.0 {
                    self.request_specific_block(BlockHeight(missing_height))
                        .await?;
                }
                self.block_buffer.insert(block_height, block);
            }
        }

        Ok(())
    }

    async fn process_buffered_blocks(&mut self) -> Result<()> {
        while let Some(block) = self.block_buffer.remove(&self.current_block) {
            let height = block.height();
            self.emit_reconstructed_block(block).await?;
            self.current_block = height + 1;
        }
        Ok(())
    }

    async fn request_specific_block(&mut self, height: BlockHeight) -> Result<()> {
        let Some(client) = self.da_client.as_mut() else {
            return Err(anyhow!("DA client not initialized"));
        };
        client
            .send(DataAvailabilityRequest::StoredBlockRequest(height))
            .await?;
        Ok(())
    }

    async fn emit_reconstructed_block(&mut self, block: StoredSignedBlock) -> Result<()> {
        let signed_block = self.reconstruct_signed_block(block).await?;
        self.bus
            .send_waiting_if_full(DataEvent::OrderedSignedBlock(signed_block))
            .await
    }

    async fn reconstruct_signed_block(&mut self, block: StoredSignedBlock) -> Result<SignedBlock> {
        let current_chain_timestamp = self
            .resolve_current_chain_timestamp(&block)
            .await?
            .context("Current chain timestamp is required to fetch data proposals from GCS")?;

        let mut data_proposals = Vec::with_capacity(block.data_proposals.len());
        for (lane_id, hashes) in &block.data_proposals {
            let mut proposals = Vec::with_capacity(hashes.len());
            for dp_hash in hashes {
                proposals.push(
                    self.fetch_data_proposal(&current_chain_timestamp, lane_id, dp_hash)
                        .await?,
                );
            }
            data_proposals.push((lane_id.clone(), proposals));
        }

        Ok(SignedBlock {
            data_proposals,
            consensus_proposal: block.consensus_proposal,
            certificate: block.certificate,
        })
    }

    async fn resolve_current_chain_timestamp(
        &mut self,
        block: &StoredSignedBlock,
    ) -> Result<Option<String>> {
        if let Some(current_chain_timestamp) = &self.current_chain_timestamp {
            return Ok(Some(current_chain_timestamp.clone()));
        }

        if block.height() != BlockHeight(0) {
            return Ok(None);
        }

        let current_chain_timestamp =
            timestamp_to_folder_name(block.consensus_proposal.timestamp.0)?;
        self.current_chain_timestamp = Some(current_chain_timestamp.clone());

        let mut store = NodeStateStore::default();
        store.current_chain_timestamp = Some(current_chain_timestamp.clone());
        let _ = persist_current_chain_timestamp(&self.config.data_directory, &store)?;

        Ok(Some(current_chain_timestamp))
    }

    async fn fetch_data_proposal(
        &self,
        current_chain_timestamp: &str,
        lane_id: &LaneId,
        dp_hash: &hyli_model::DataProposalHash,
    ) -> Result<DataProposal> {
        let mut reader = self
            .gcs_client
            .read_object(
                bucket_path(&self.config.gcs_conf.gcs_bucket),
                data_proposal_object_name(
                    &self.config.gcs_conf.gcs_prefix,
                    current_chain_timestamp,
                    lane_id,
                    dp_hash,
                ),
            )
            .send()
            .await?;
        let mut bytes = Vec::new();
        while let Some(chunk) = reader.next().await.transpose()? {
            bytes.extend_from_slice(&chunk);
        }
        let proposal = borsh::from_slice::<DataProposal>(&bytes)
            .context("Deserializing data proposal from GCS")?;
        if proposal.hashed() != *dp_hash {
            return Err(anyhow!(
                "Fetched data proposal hash mismatch for lane {}",
                lane_id
            ));
        }
        Ok(proposal)
    }
}

fn bucket_path(bucket: &str) -> String {
    if bucket.starts_with("projects/") {
        bucket.to_string()
    } else {
        format!("projects/_/buckets/{bucket}")
    }
}

fn data_proposal_object_name(
    gcs_prefix: &str,
    current_chain_timestamp: &str,
    lane_id: &LaneId,
    dp_hash: &hyli_model::DataProposalHash,
) -> String {
    format!(
        "{}/{}/data_proposals/{}/{}.bin",
        gcs_prefix, current_chain_timestamp, lane_id, dp_hash
    )
}
