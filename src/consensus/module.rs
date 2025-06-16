use anyhow::Result;
use hyle_modules::{bus::SharedMessageBus, modules::Module};

use crate::model::SharedRunContext;

use super::{
    api, consensus_bus_client::ConsensusBusClient, metrics::ConsensusMetrics, Consensus,
    ConsensusStore,
};

impl Module for Consensus {
    type Context = SharedRunContext;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let file = ctx.config.data_directory.clone().join("consensus.bin");
        let mut store: ConsensusStore = Self::load_from_disk_or_default(file.as_path());
        // TEMP HACK
        store.bft_round_state.timeout.requests.clear();
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

    fn run(&mut self) -> impl futures::Future<Output = Result<()>> + Send {
        self.wait_genesis()
    }
}
