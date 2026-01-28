use anyhow::Result;
use hyli_bus::modules::ModulePersistOutput;
use hyli_modules::{
    bus::SharedMessageBus,
    modules::{files::CONSENSUS_BIN, Module},
};
use std::path::PathBuf;
use tracing::warn;

use super::{
    api, consensus_bus_client::ConsensusBusClient, metrics::ConsensusMetrics, Consensus,
    ConsensusStore,
};
use crate::model::SharedRunContext;

impl Module for Consensus {
    type Context = SharedRunContext;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let file = PathBuf::from(CONSENSUS_BIN);
        let mut store: ConsensusStore =
            match Self::load_from_disk(&ctx.config.data_directory, &file)? {
                Some(s) => s,
                None => {
                    warn!("Starting consensus from default.");
                    ConsensusStore::default()
                }
            };
        // Cap in-memory prepare cache on startup to avoid loading oversized state.
        store
            .bft_round_state
            .follower
            .buffered_prepares
            .set_max_size(Some(ctx.config.consensus.buffered_prepares_max_in_memory));
        let metrics = ConsensusMetrics::global();

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

    async fn persist(&mut self) -> Result<ModulePersistOutput> {
        if let Some(file) = &self.file {
            let serialize_limit = self
                .config
                .consensus
                .buffered_prepares_max_serialized
                .min(self.config.consensus.buffered_prepares_max_in_memory);
            self.store
                .bft_round_state
                .follower
                .buffered_prepares
                .set_max_size(Some(serialize_limit));
            let checksum = Self::save_on_disk(&self.config.data_directory, file, &self.store)?;
            return Ok(vec![(self.config.data_directory.join(file), checksum)]);
        }

        Ok(vec![])
    }
}
