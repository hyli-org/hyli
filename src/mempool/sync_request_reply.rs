use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use futures::StreamExt;
use hyle_crypto::SharedBlstCrypto;
use hyle_model::{utils::TimestampMs, DataProposalHash, LaneId, ValidatorPublicKey};
use hyle_modules::{log_error, log_warn};
use hyle_net::clock::TimestampMsClock;
use tokio::pin;
use tracing::{debug, info, warn};

use crate::p2p::network::{HeaderSigner, OutboundMessage};

use super::{
    metrics::MempoolMetrics,
    storage::{LaneEntryMetadata, Storage},
    storage_fjall::LanesStorage,
    MempoolNetMessage,
};

pub struct SyncRequest {
    pub from: Option<DataProposalHash>,
    pub to: DataProposalHash,
    pub validator: ValidatorPublicKey,
}

/// Submodule of Mempool dedicated to SyncRequest/SyncReply handling
pub struct MempoolSync {
    // TODO: Remove after putting lane id in sync request/sync reply
    lane_id: LaneId,
    /// Storage handle
    lanes: LanesStorage,
    /// Crypto handle
    crypto: SharedBlstCrypto,
    /// Metrics handle
    metrics: MempoolMetrics,
    /// Keeping track of last time we sent a reply to the validator and the data proposal hash
    by_pubkey_by_dp_hash: HashMap<ValidatorPublicKey, HashMap<DataProposalHash, TimestampMs>>,
    /// Map containing per data proposal, which validators are interested in a sync reply
    todo: HashMap<DataProposalHash, (LaneEntryMetadata, HashSet<ValidatorPublicKey>)>,
    /// Network message channel
    net_sender: tokio::sync::broadcast::Sender<OutboundMessage>,
    /// Chan where Mempool puts received Sync Requests to handle
    sync_request_receiver: tokio::sync::mpsc::Receiver<SyncRequest>,
}

impl MempoolSync {
    pub fn create(
        lane_id: LaneId,
        lanes: LanesStorage,
        crypto: SharedBlstCrypto,
        metrics: MempoolMetrics,
        net_sender: tokio::sync::broadcast::Sender<OutboundMessage>,
        sync_request_receiver: tokio::sync::mpsc::Receiver<SyncRequest>,
    ) -> MempoolSync {
        MempoolSync {
            lane_id,
            lanes,
            crypto,
            metrics,
            net_sender,
            sync_request_receiver,
            by_pubkey_by_dp_hash: Default::default(),
            todo: Default::default(),
        }
    }
    pub async fn start(&mut self) -> anyhow::Result<()> {
        info!("Starting MempoolSync");

        let mut batched_replies_interval = tokio::time::interval(Duration::from_millis(200));
        loop {
            tokio::select! {
                Some(sync_request) = self.sync_request_receiver.recv() => {
                    _ = log_error!(
                        self.unfold_sync_request_interval(sync_request).await,
                        "Unfolding SyncRequest interval"
                    );
                }
                _ = batched_replies_interval.tick() => {
                    self.send_replies().await;
                }
            }
        }
    }

    /// Reply can be emitted because
    /// - it has never been emitted before
    /// - it was emitted a long time ago
    fn should_throttle(
        &self,
        validator: &ValidatorPublicKey,
        data_proposal_hash: &DataProposalHash,
    ) -> bool {
        let now = TimestampMsClock::now();

        let Some(data_proposal_record) = self
            .by_pubkey_by_dp_hash
            .get(validator)
            .and_then(|validator_records| validator_records.get(data_proposal_hash))
        else {
            return false;
        };

        if now - data_proposal_record.clone() > Duration::from_secs(10) {
            return false;
        }

        true
    }

    /// Fetches metadata from storage for the given interval, and populate the todo hashmap with it. Called everytime we get a new SyncRequest
    async fn unfold_sync_request_interval(
        &mut self,
        SyncRequest {
            from,
            to,
            validator,
        }: SyncRequest,
    ) -> anyhow::Result<()> {
        if from.as_ref() == Some(&to) {
            return Ok(());
        }

        pin! {
            let stream = self.lanes.get_entries_metadata_between_hashes(&self.lane_id, from, Some(to));
        };

        while let Some(entry) = stream.next().await {
            if let Ok((metadata, dp_hash)) =
                log_warn!(entry, "Getting entry metada to prepare sync replies")
            {
                self.todo
                    .entry(dp_hash)
                    .or_insert((metadata, Default::default()))
                    .1
                    .insert(validator.clone());
            }
        }
        Ok(())
    }

    /// Try to send replies based on what is stored in the todo hashmap. Every time a reply is sent, it stored a timestamp to throttle upcoming SyncRequests, and remove it from the todo hashmap
    async fn send_replies(&mut self) {
        if self.todo.is_empty() {
            return;
        }

        let mut todo = HashMap::new();

        std::mem::swap(&mut self.todo, &mut todo);

        for (dp_hash, (metadata, validators)) in todo.into_iter() {
            for validator in validators.into_iter() {
                if self.should_throttle(&validator, &dp_hash) {
                    debug!(
                        "Throttling reply for DP Hash: {} to: {}",
                        &dp_hash, &validator
                    );
                    self.metrics
                        .mempool_sync_throttled(&self.lane_id, &validator);
                } else {
                    self.metrics
                        .mempool_sync_processed(&self.lane_id, &validator);

                    // Update last dissemination time
                    let now = TimestampMsClock::now();
                    self.by_pubkey_by_dp_hash
                        .entry(validator.clone())
                        .or_default()
                        .insert(dp_hash.clone(), now);

                    if let Ok(Some(data_proposal)) = log_error!(
                        self.lanes.get_dp_by_hash(&self.lane_id, &dp_hash),
                        "Getting data proposal for to prepare a SyncReply"
                    ) {
                        let signed_reply =
                            self.crypto
                                .sign_msg_with_header(MempoolNetMessage::SyncReply(
                                    metadata.clone(),
                                    data_proposal,
                                ));

                        if let Ok(signed_reply) = signed_reply {
                            if log_error!(
                                self.net_sender
                                    .send(OutboundMessage::send(validator.clone(), signed_reply)),
                                "Sending MempoolNetMessage::SyncReply msg on the bus"
                            )
                            .is_ok()
                            {
                                debug!("Sent reply for DP Hash: {} to: {}", &dp_hash, &validator);
                                // In case of success, we don't put back this reply in the todo map
                                continue;
                            }
                        }
                    }

                    warn!(
                        "Could not send reply for DP Hash: {} to: {}, retrying later.",
                        &dp_hash, &validator
                    );
                    self.metrics.mempool_sync_failure(&self.lane_id, &validator);

                    self.todo
                        .entry(dp_hash.clone())
                        .or_insert((metadata.clone(), Default::default()))
                        .1
                        .insert(validator);
                }
            }
        }
    }
}
