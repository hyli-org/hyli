use hyle_modules::{log_error, module_handle_messages};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

use crate::{
    consensus::ConsensusEvent, mempool::storage::Storage, model::*,
    node_state::module::NodeStateEvent, p2p::network::MsgWithHeader, utils::conf::P2pMode,
};

use client_sdk::tcp_client::TcpServerMessage;
use hyle_model::{DataProposalHash, LaneBytesSize, LaneId, ProgramId, Verifier};
use hyle_modules::{bus::SharedMessageBus, modules::Module};
use tracing::warn;

use super::{api::RestApiMessage, MempoolNetMessage, QueryNewCut};

use crate::model::SharedRunContext;

use super::{
    api, mempool_bus_client::MempoolBusClient, metrics::MempoolMetrics,
    storage_fjall::LanesStorage, Mempool, MempoolStore,
};

use anyhow::Result;

impl Module for Mempool {
    type Context = SharedRunContext;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let metrics = MempoolMetrics::global(ctx.config.id.clone());
        let api = api::api(&bus, &ctx.api).await;
        if let Ok(mut guard) = ctx.api.router.lock() {
            if let Some(router) = guard.take() {
                guard.replace(router.nest("/v1/", api));
            }
        }
        let bus = MempoolBusClient::new_from_bus(bus.new_handle()).await;

        let attributes = Self::load_from_disk::<MempoolStore>(
            ctx.config.data_directory.join("mempool.bin").as_path(),
        )
        .unwrap_or_default();

        let mut lanes_tip =
            Self::load_from_disk::<BTreeMap<LaneId, (DataProposalHash, LaneBytesSize)>>(
                ctx.config
                    .data_directory
                    .join("mempool_lanes_tip.bin")
                    .as_path(),
            )
            .unwrap_or_default();

        // Testnet bug fix - we had an issue that led to a specific DP being double-counted. This accounts for that in nodes that behaved correctly.
        // This is a workaround for the issue, and should be removed in the future.
        // The issue is that the staking was updated to a higher value, and we can't go back down to the correct, lower value.
        // So as a hack, we'll just update the metadata with the higher value.
        // The nodes won't actually redisseminate this DP, and any new one will use correct values.
        let bugged_dp_hash = DataProposalHash(
            "3fe68d0d7d08581dec2e89291fb34ce77cd591edb47d11ec0f19f7d5b5dd508e".to_string(),
        );
        #[allow(clippy::unwrap_used, reason = "hardcoded bugfix")]
        let bugged_lane_id = LaneId(ValidatorPublicKey(hex::decode("afac1e7cf451ee4659a2b12822acfb54a8aaabb9acd0db917974838ffa7c8da9eb6a856df16a336c772247dc06f2f86e").unwrap()));
        let bugged_tip = lanes_tip.get_mut(&bugged_lane_id);
        tracing::warn!("Bugged tip: {:?}", bugged_tip);
        if let Some((dp, size)) = bugged_tip {
            if dp == &bugged_dp_hash {
                if size.0 == 69400606 {
                    tracing::warn!("Fixing bugged tip in Mempool");
                    *size = LaneBytesSize(69627580);
                } else {
                    tracing::warn!("Bugged tip already fixed in Mempool");
                }
            }
        }

        let mut lanes = LanesStorage::new(&ctx.config.data_directory, lanes_tip)?;

        let bugged_metadata = lanes.get_metadata_by_hash(&bugged_lane_id, &bugged_dp_hash);
        let bugged_dp = lanes.get_dp_by_hash(&bugged_lane_id, &bugged_dp_hash);

        if let (Ok(Some(mut metadata)), Ok(Some(dp))) = (bugged_metadata, bugged_dp) {
            tracing::warn!(
                "Bugged DP metadata: {:?}, DP: {:?} (size: {}, expected 226974",
                metadata.cumul_size,
                dp.hashed(),
                dp.estimate_size()
            );
            metadata.cumul_size = LaneBytesSize(69627580);
            // Don't change signatures - any present should be for the incorrect size,
            // but that shouldn't matter.
            lanes.put_no_verification(bugged_lane_id.clone(), (metadata, dp))?;
        } else {
            tracing::warn!("Bugged DP not found in lanes storage");
        }

        // Register the Hyli contract to be able to handle registrations.
        #[allow(clippy::expect_used, reason = "not held across await")]
        attributes
            .known_contracts
            .write()
            .expect("logic issue")
            .0
            .entry("hyle".into())
            .or_insert_with(|| (Verifier("hyle".to_owned()), ProgramId(vec![])));

        Ok(Mempool {
            bus,
            file: Some(ctx.config.data_directory.clone()),
            conf: ctx.config.clone(),
            crypto: Arc::clone(&ctx.crypto),
            metrics,
            lanes,
            inner: attributes,
        })
    }

    async fn run(&mut self) -> Result<()> {
        let tick_interval = std::cmp::min(
            self.conf.consensus.slot_duration / 2,
            Duration::from_millis(500),
        );
        let mut new_dp_timer = tokio::time::interval(tick_interval);
        // We always disseminate new data proposals, so we can run the re-dissemination timer
        // infrequently, as it will only be useful if we had a network issue that lead
        // to a PoDA not being created.
        let mut disseminate_timer = tokio::time::interval(Duration::from_secs(15));
        new_dp_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        disseminate_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let sync_request_sender = self.start_mempool_sync();

        // TODO: Recompute optimistic node_state for contract registrations.
        module_handle_messages! {
            on_self self,
            delay_shutdown_until {
                // TODO: serialize these somehow?
                self.processing_dps.is_empty() && self.processing_txs.is_empty() && self.own_data_proposal_in_preparation.is_empty()
            },
            listen<MsgWithHeader<MempoolNetMessage>> cmd => {
                let _ = log_error!(self.handle_net_message(cmd, &sync_request_sender).await, "Handling MempoolNetMessage in Mempool");
            }
            listen<RestApiMessage> cmd => {
                let _ = log_error!(self.handle_api_message(cmd), "Handling API Message in Mempool");
            }
            listen<TcpServerMessage> cmd => {
                let _ = log_error!(self.handle_tcp_server_message(cmd), "Handling TCP Server message in Mempool");
            }
            listen<ConsensusEvent> cmd => {
                let _ = log_error!(self.handle_consensus_event(cmd).await, "Handling ConsensusEvent in Mempool");
            }
            listen<NodeStateEvent> cmd => {
                let NodeStateEvent::NewBlock(block) = cmd;
                // In this p2p mode we don't receive consensus events so we must update manually.
                if self.conf.p2p.mode == P2pMode::LaneManager {
                    if let Err(e) = self.staking.process_block(block.as_ref()) {
                        tracing::error!("Error processing block in mempool: {:?}", e);
                    }
                }
                for (_, contract, _) in block.registered_contracts.into_values() {
                    self.handle_contract_registration(contract);
                }
                for (contract_name, program_id) in block.updated_program_ids.into_iter() {
                    self.handle_contract_update(contract_name, program_id);
                }

            }
            command_response<QueryNewCut, Cut> staking => {
                self.handle_querynewcut(staking)
            }
            Some(event) = self.inner.processing_dps.join_next() => {
                if let Ok(event) = log_error!(event, "Processing DPs from JoinSet") {
                    if let Ok(event) = log_error!(event, "Error in running task") {
                        let _ = log_error!(self.handle_internal_event(event),
                            "Handling InternalMempoolEvent in Mempool");
                    }
                }
            }
            // own_lane.rs code below
            Some(Ok(tx)) = self.inner.processing_txs.join_next() => {
                match tx {
                    Ok(tx) => {
                        let _ = log_error!(self.on_new_tx(tx), "Handling tx in Mempool");
                    }
                    Err(e) => {
                        warn!("Error processing tx: {:?}", e);
                    }
                }
            }
            Some(own_dp) = self.inner.own_data_proposal_in_preparation.join_next() => {
                // Fatal here, if we loose the dp in the join next error, it's lost
                if let Ok((own_dp_hash, own_dp)) = log_error!(own_dp, "Getting result for data proposal preparation from joinset"){
                    _ = log_error!(self.resume_new_data_proposal(own_dp, own_dp_hash).await, "Resuming own data proposal creation");
                }
            }
            _ = new_dp_timer.tick() => {
                _  = log_error!(self.prepare_new_data_proposal(), "Try preparing a new data proposal on tick");
            }
            _ = disseminate_timer.tick() => {
                if let Ok(true) = log_error!(
                    self
                    .redisseminate_oldest_data_proposal()
                    .await,
                    "Disseminate data proposals on tick"
                ) {
                    disseminate_timer.reset();
                }
            }
        };

        Ok(())
    }

    async fn persist(&mut self) -> Result<()> {
        if let Some(file) = &self.file {
            _ = log_error!(
                Self::save_on_disk(file.join("mempool.bin").as_path(), &self.inner),
                "Persisting Mempool storage"
            );
            _ = log_error!(
                Self::save_on_disk(
                    file.join("mempool_lanes_tip.bin").as_path(),
                    &self.lanes.lanes_tip
                ),
                "Persisting Mempool lanes tip"
            );
        }

        Ok(())
    }
}
