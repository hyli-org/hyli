use anyhow::Result;
use hyli_modules::{
    bus::SharedMessageBus,
    log_error,
    modules::{files::CONSENSUS_BIN, Module},
};

use super::{
    api, consensus_bus_client::ConsensusBusClient, metrics::ConsensusMetrics, Consensus,
    ConsensusStore,
};
use crate::model::SharedRunContext;

impl Module for Consensus {
    type Context = SharedRunContext;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let file = ctx.config.data_directory.join(CONSENSUS_BIN);
        let mut store: ConsensusStore = Self::load_from_disk_or_default(file.as_path());
        // Cap in-memory sync cache on startup to avoid loading oversized state.
        store
            .bft_round_state
            .follower
            .sync_prepares
            .trim_to_limit(ctx.config.consensus.sync_prepares_max_in_memory);
        let metrics = ConsensusMetrics::global(ctx.config.id.clone());

        let api = api::api(&bus, &ctx).await;
        if let Ok(mut guard) = ctx.api.router.lock() {
            if let Some(router) = guard.take() {
                guard.replace(router.nest("/v1/consensus", api));
            }
        }
        let bus = ConsensusBusClient::new_from_bus(bus.new_handle()).await;

        Ok(Consensus {
            metrics,
            bus,
            file: Some(file),
            store,
            config: ctx.config.clone(),
            crypto: ctx.crypto.clone(),
        })
    }

    async fn run(&mut self) -> Result<()> {
        self.wait_genesis().await
    }

    async fn persist(&mut self) -> Result<()> {
        if let Some(file) = &self.file {
            // Persist only a small sync cache to reduce serialization cost.
            let serialize_limit = self
                .config
                .consensus
                .sync_prepares_max_serialized
                .min(self.config.consensus.sync_prepares_max_in_memory);
            let removed = self
                .store
                .bft_round_state
                .follower
                .sync_prepares
                .drain_oldest_excess(serialize_limit);
            _ = log_error!(
                Self::save_on_disk(file.as_path(), &self.store),
                "Persisting consensus state"
            );
            self.store
                .bft_round_state
                .follower
                .sync_prepares
                .restore_oldest(removed);
        }

        Ok(())
    }
}
