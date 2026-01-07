use hyli_modules::{log_error, module_handle_messages};
use std::{sync::Arc, time::Duration};

use crate::{
    consensus::ConsensusEvent, model::*, p2p::network::MsgWithHeader, utils::conf::P2pMode,
};

use client_sdk::tcp_client::TcpServerMessage;
use hyli_modules::{bus::SharedMessageBus, modules::Module};
use tracing::warn;

use super::{api::RestApiMessage, MempoolNetMessage, QueryNewCut};

use crate::model::SharedRunContext;

use super::{
    api, mempool_bus_client::MempoolBusClient, metrics::MempoolMetrics, shared_lanes_storage,
    Mempool, MempoolStore,
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

        Ok(Mempool {
            bus,
            file: Some(ctx.config.data_directory.clone()),
            conf: ctx.config.clone(),
            crypto: Arc::clone(&ctx.crypto),
            metrics,
            lanes: shared_lanes_storage(&ctx.config.data_directory)?,
            inner: attributes,
        })
    }

    async fn run(&mut self) -> Result<()> {
        let tick_interval = std::cmp::min(
            self.conf.consensus.slot_duration / 2,
            Duration::from_millis(500),
        );
        let mut new_dp_timer = tokio::time::interval(tick_interval);
        new_dp_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

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
                    if let Err(e) = self.staking.process_block(&block.staking_data) {
                        tracing::error!("Error processing block in mempool: {:?}", e);
                    }
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
                if let Ok((_own_dp_hash, own_dp)) = log_error!(own_dp, "Getting result for data proposal preparation from joinset"){
                    _ = log_error!(self.resume_new_data_proposal(own_dp).await, "Resuming own data proposal creation");
                }
            }
            _ = new_dp_timer.tick() => {
                _  = log_error!(self.prepare_new_data_proposal(), "Try preparing a new data proposal on tick");
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
                    &self.lanes.lane_tips_snapshot()
                ),
                "Persisting Mempool lanes tip"
            );
        }

        Ok(())
    }
}
