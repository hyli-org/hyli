use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use futures::StreamExt;
use hyli_crypto::SharedBlstCrypto;
use hyli_model::{utils::TimestampMs, DataProposalHash, LaneId, ValidatorPublicKey};
use hyli_modules::{
    bus::{BusEnvelope, BusSender},
    log_error, log_warn,
};
use hyli_net::clock::TimestampMsClock;
use tokio::pin;
use tracing::{debug, info, warn};

use crate::{
    mempool::storage::MetadataOrMissingHash,
    p2p::network::{HeaderSigner, OutboundMessage},
};

const REPLY_BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const REPLY_BACKOFF_MAX: Duration = Duration::from_secs(8);
const REPLY_DISPATCH_INTERVAL: Duration = Duration::from_millis(400);

use super::{
    metrics::MempoolMetrics,
    storage::{LaneEntryMetadata, Storage},
    storage_fjall::LanesStorage,
    MempoolNetMessage,
};

#[derive(Clone)]
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
    by_pubkey_by_dp_hash: HashMap<ValidatorPublicKey, HashMap<DataProposalHash, ThrottleState>>,
    /// Map containing per data proposal, which validators are interested in a sync reply
    todo: HashMap<DataProposalHash, (LaneEntryMetadata, HashSet<ValidatorPublicKey>)>,
    /// Network message channel
    net_sender: BusSender<OutboundMessage>,
    /// Chan where Mempool puts received Sync Requests to handle
    sync_request_receiver: tokio::sync::mpsc::Receiver<SyncRequest>,
}

#[derive(Clone)]
struct ThrottleState {
    last_sent: TimestampMs,
    backoff: Duration,
}

impl MempoolSync {
    pub fn create(
        lane_id: LaneId,
        lanes: LanesStorage,
        crypto: SharedBlstCrypto,
        metrics: MempoolMetrics,
        net_sender: BusSender<OutboundMessage>,
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

        let mut batched_replies_interval = tokio::time::interval(REPLY_DISPATCH_INTERVAL);
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
        let Some(data_proposal_record) = self
            .by_pubkey_by_dp_hash
            .get(validator)
            .and_then(|validator_records| validator_records.get(data_proposal_hash))
        else {
            return false;
        };

        should_throttle_since(data_proposal_record, TimestampMsClock::now())
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
            debug!("No need to unfold an empty interval for SyncRequest: from: {:?}, to: {}, validator: {}", from, to, validator);
            return Ok(());
        }

        pin! {
            let stream = self.lanes.get_entries_metadata_between_hashes(&self.lane_id, from.clone(), Some(to.clone()));
        };

        while let Some(entry) = stream.next().await {
            if let Ok(MetadataOrMissingHash::Metadata(metadata, dp_hash)) =
                log_warn!(entry, "Getting entry metada to prepare sync replies")
            {
                self.todo
                    .entry(dp_hash)
                    .or_insert((metadata, Default::default()))
                    .1
                    .insert(validator.clone());
            } else {
                warn!("Could not get entry metadata to prepare sync replies for SyncRequest: from: {:?}, to: {}, validator: {}", from, to, validator);
            }

            // If from is None, we are just looking for the 'to' entry
            // So one loop iteration is enough
            if from.is_none() {
                break;
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
                                self.net_sender.send(BusEnvelope::from_message(
                                    OutboundMessage::send(validator.clone(), signed_reply)
                                )),
                                "Sending MempoolNetMessage::SyncReply msg on the bus"
                            )
                            .is_ok()
                            {
                                debug!("Sent reply for DP Hash: {} to: {}", &dp_hash, &validator);
                                self.metrics
                                    .mempool_sync_processed(&self.lane_id, &validator);
                                // Update last dissemination time only after a successful send so we
                                // do not throttle retries that failed to go out.
                                self.record_success(&validator, &dp_hash);
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
                    self.record_failure(&validator, &dp_hash);

                    self.todo
                        .entry(dp_hash.clone())
                        .or_insert((metadata.clone(), Default::default()))
                        .1
                        .insert(validator);
                }
            }
        }
    }

    fn record_success(&mut self, validator: &ValidatorPublicKey, dp_hash: &DataProposalHash) {
        let now = TimestampMsClock::now();
        self.by_pubkey_by_dp_hash
            .entry(validator.clone())
            .or_default()
            .insert(
                dp_hash.clone(),
                ThrottleState {
                    last_sent: now,
                    backoff: REPLY_BACKOFF_INITIAL,
                },
            );
    }

    fn record_failure(&mut self, validator: &ValidatorPublicKey, dp_hash: &DataProposalHash) {
        let now = TimestampMsClock::now();
        let prev_backoff = self
            .by_pubkey_by_dp_hash
            .get(validator)
            .and_then(|m| m.get(dp_hash))
            .map(|s| s.backoff);
        let backoff = next_backoff(prev_backoff);
        self.by_pubkey_by_dp_hash
            .entry(validator.clone())
            .or_default()
            .insert(
                dp_hash.clone(),
                ThrottleState {
                    last_sent: now,
                    backoff,
                },
            );
    }
}

fn should_throttle_since(state: &ThrottleState, now: TimestampMs) -> bool {
    now - state.last_sent.clone() < state.backoff
}

fn next_backoff(prev: Option<Duration>) -> Duration {
    match prev {
        None => REPLY_BACKOFF_INITIAL,
        Some(current) => {
            let doubled_ms = current.as_millis().saturating_mul(2);
            let capped_ms = doubled_ms.min(REPLY_BACKOFF_MAX.as_millis());
            Duration::from_millis(capped_ms as u64)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        mempool::storage::Storage,
        model::{DataProposal, Transaction},
    };
    use anyhow::Result;
    use hyli_crypto::BlstCrypto;
    use std::{collections::BTreeMap, sync::Arc};
    use tokio::time::{timeout, Duration};

    struct SyncTestHarness {
        mempool_sync: MempoolSync,
        validator: ValidatorPublicKey,
        dp_hash: DataProposalHash,
        data_proposal: DataProposal,
        receiver: tokio::sync::broadcast::Receiver<OutboundMessage>,
    }

    fn setup_sync_harness() -> Result<SyncTestHarness> {
        let crypto = BlstCrypto::new("mempool-sync")?;
        let validator = BlstCrypto::new("requester")?.validator_pubkey().clone();
        let lane_id = LaneId(crypto.validator_pubkey().clone());

        let mut lanes = LanesStorage::new(tempfile::tempdir()?.path(), BTreeMap::default())?;

        let data_proposal = DataProposal::new(None, vec![Transaction::default()]);
        let (dp_hash, _) = lanes.store_data_proposal(&crypto, &lane_id, data_proposal.clone())?;

        let metrics = MempoolMetrics::global("mempool-sync-test".to_string());
        let (net_sender, receiver) = tokio::sync::broadcast::channel(8);
        let (_sync_request_sender, sync_request_receiver) = tokio::sync::mpsc::channel(8);

        let mempool_sync = MempoolSync::create(
            lane_id,
            lanes,
            Arc::new(crypto),
            metrics,
            net_sender,
            sync_request_receiver,
        );

        Ok(SyncTestHarness {
            mempool_sync,
            validator,
            dp_hash,
            data_proposal,
            receiver,
        })
    }

    fn assert_sync_reply(
        outbound: OutboundMessage,
        expected_validator: &ValidatorPublicKey,
        expected_dp: &DataProposal,
    ) {
        match outbound {
            OutboundMessage::SendMessage { validator_id, msg } => {
                assert_eq!(&validator_id, expected_validator);
                match msg {
                    crate::p2p::network::NetMessage::MempoolMessage(msg) => match msg.msg {
                        MempoolNetMessage::SyncReply(_, dp) => {
                            assert_eq!(&dp, expected_dp);
                        }
                        other => panic!("Expected SyncReply message, got {other:?}"),
                    },
                    other => panic!("Expected mempool message, got {other:?}"),
                }
            }
            other => panic!("Expected direct send, got {other:?}"),
        }
    }

    #[test_log::test(tokio::test)]
    async fn throttle_helper_respects_window() {
        let now = TimestampMs(10_000);
        let state_initial = ThrottleState {
            last_sent: now.clone() - (REPLY_BACKOFF_INITIAL - Duration::from_millis(1)),
            backoff: REPLY_BACKOFF_INITIAL,
        };
        assert!(
            should_throttle_since(&state_initial, now.clone()),
            "should throttle when last send is within initial window"
        );

        let state_outside = ThrottleState {
            last_sent: now.clone() - (REPLY_BACKOFF_INITIAL + Duration::from_millis(1)),
            backoff: REPLY_BACKOFF_INITIAL,
        };
        assert!(
            !should_throttle_since(&state_outside, now.clone()),
            "should not throttle when outside initial window"
        );

        let state_custom_backoff = ThrottleState {
            last_sent: now.clone() - Duration::from_secs(3),
            backoff: Duration::from_secs(4),
        };
        assert!(
            should_throttle_since(&state_custom_backoff, now.clone()),
            "should throttle when within custom backoff"
        );

        let state_custom_backoff_ok = ThrottleState {
            last_sent: now.clone() - Duration::from_secs(5),
            backoff: Duration::from_secs(4),
        };
        assert!(
            !should_throttle_since(&state_custom_backoff_ok, now),
            "should not throttle when past custom backoff"
        );
    }

    #[test_log::test(tokio::test)]
    async fn exponential_backoff_doubles_and_caps() -> Result<()> {
        let mut harness = setup_sync_harness()?;
        let v = harness.validator.clone();
        let dp = harness.dp_hash.clone();

        harness.mempool_sync.record_failure(&v, &dp);
        let state = harness
            .mempool_sync
            .by_pubkey_by_dp_hash
            .get(&v)
            .and_then(|m| m.get(&dp))
            .cloned()
            .expect("state should exist");
        assert_eq!(state.backoff, REPLY_BACKOFF_INITIAL);

        harness.mempool_sync.record_failure(&v, &dp);
        let state = harness
            .mempool_sync
            .by_pubkey_by_dp_hash
            .get(&v)
            .and_then(|m| m.get(&dp))
            .cloned()
            .expect("state should exist");
        assert_eq!(state.backoff, Duration::from_secs(2));

        harness.mempool_sync.record_failure(&v, &dp);
        let state = harness
            .mempool_sync
            .by_pubkey_by_dp_hash
            .get(&v)
            .and_then(|m| m.get(&dp))
            .cloned()
            .expect("state should exist");
        assert_eq!(state.backoff, Duration::from_secs(4));

        harness.mempool_sync.record_failure(&v, &dp);
        let state = harness
            .mempool_sync
            .by_pubkey_by_dp_hash
            .get(&v)
            .and_then(|m| m.get(&dp))
            .cloned()
            .expect("state should exist");
        assert_eq!(state.backoff, REPLY_BACKOFF_MAX);

        harness.mempool_sync.record_success(&v, &dp);
        let state = harness
            .mempool_sync
            .by_pubkey_by_dp_hash
            .get(&v)
            .and_then(|m| m.get(&dp))
            .cloned()
            .expect("state should exist");
        assert_eq!(state.backoff, REPLY_BACKOFF_INITIAL);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn throttles_repeated_requests_for_same_dp() -> Result<()> {
        let mut harness = setup_sync_harness()?;
        let mut receiver = harness.receiver.resubscribe();
        let request = SyncRequest {
            from: None,
            to: harness.dp_hash.clone(),
            validator: harness.validator.clone(),
        };

        harness
            .mempool_sync
            .unfold_sync_request_interval(request.clone())
            .await?;
        harness.mempool_sync.send_replies().await;

        let first = receiver.recv().await?;
        assert_sync_reply(first, &harness.validator, &harness.data_proposal);

        harness
            .mempool_sync
            .unfold_sync_request_interval(request)
            .await?;
        harness.mempool_sync.send_replies().await;

        assert!(timeout(Duration::from_millis(200), receiver.recv())
            .await
            .is_err());

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn sends_again_after_throttle_window_expires() -> Result<()> {
        let mut harness = setup_sync_harness()?;
        let mut receiver = harness.receiver.resubscribe();
        let request = SyncRequest {
            from: None,
            to: harness.dp_hash.clone(),
            validator: harness.validator.clone(),
        };

        harness
            .mempool_sync
            .unfold_sync_request_interval(request.clone())
            .await?;
        harness.mempool_sync.send_replies().await;
        let _ = receiver.recv().await?;

        harness
            .mempool_sync
            .unfold_sync_request_interval(request.clone())
            .await?;
        harness.mempool_sync.send_replies().await;
        assert!(timeout(Duration::from_millis(200), receiver.recv())
            .await
            .is_err());

        let past = TimestampMsClock::now() - REPLY_BACKOFF_MAX - Duration::from_millis(1);
        harness
            .mempool_sync
            .by_pubkey_by_dp_hash
            .entry(harness.validator.clone())
            .or_default()
            .insert(
                harness.dp_hash.clone(),
                ThrottleState {
                    last_sent: past,
                    backoff: REPLY_BACKOFF_INITIAL,
                },
            );

        harness
            .mempool_sync
            .unfold_sync_request_interval(request)
            .await?;
        harness.mempool_sync.send_replies().await;

        let second = receiver.recv().await?;
        assert_sync_reply(second, &harness.validator, &harness.data_proposal);

        Ok(())
    }
}
