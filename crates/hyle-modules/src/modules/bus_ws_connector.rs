use anyhow::Result;
use sdk::{
    api::{APIBlock, APITransaction, TransactionTypeDb},
    NodeStateEvent, TransactionData,
};
use serde::Serialize;

use crate::{
    bus::{BusClientSender, SharedMessageBus},
    modules::websocket::WsTopicMessage,
};
use crate::{module_bus_client, module_handle_messages, modules::Module};

#[derive(Debug, Clone, Serialize)]
pub enum WebsocketOutEvent {
    NodeStateEvent(NodeStateEvent),
    NewBlock(APIBlock),
    NewTx(APITransaction),
}

module_bus_client! {
#[derive(Debug)]
pub struct NodeWebsocketConnectorBusClient {
    sender(WsTopicMessage<WebsocketOutEvent>),
    receiver(NodeStateEvent),
}
}

pub struct NodeWebsocketConnector {
    bus: NodeWebsocketConnectorBusClient,
    events: Vec<String>,
}

pub struct NodeWebsocketConnectorCtx {
    pub events: Vec<String>,
}

impl Module for NodeWebsocketConnector {
    type Context = NodeWebsocketConnectorCtx;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        Ok(Self {
            bus: NodeWebsocketConnectorBusClient::new_from_bus(bus.new_handle()).await,
            events: ctx.events,
        })
    }

    async fn run(&mut self) -> Result<()> {
        module_handle_messages! {
            on_bus self.bus,
            listen<NodeStateEvent> msg => {
                self.handle_node_state_event(msg)?;
            },
        };
        Ok(())
    }
}

impl NodeWebsocketConnector {
    fn handle_node_state_event(&mut self, event: NodeStateEvent) -> Result<()> {
        self.handle("node_state", &event, Self::handle_node_state);
        self.handle("new_block", &event, Self::handle_new_block);
        self.handle("new_tx", &event, Self::handle_new_tx);
        Ok(())
    }

    fn handle(
        &mut self,
        topic: &str,
        event: &NodeStateEvent,
        handler: fn(NodeStateEvent) -> Vec<WebsocketOutEvent>,
    ) {
        if self.events.contains(&topic.to_string()) {
            for e in handler(event.clone()) {
                let _ = self.bus.send(WsTopicMessage::new(topic, e));
            }
        }
    }

    fn handle_node_state(event: NodeStateEvent) -> Vec<WebsocketOutEvent> {
        vec![WebsocketOutEvent::NodeStateEvent(event)]
    }

    fn handle_new_block(event: NodeStateEvent) -> Vec<WebsocketOutEvent> {
        let NodeStateEvent::NewBlock(block) = event;

        let api_block = APIBlock {
            hash: block.hash.clone(),
            parent_hash: block.parent_hash,
            height: block.block_height.0,
            timestamp: block.block_timestamp.0 as i64,
            total_txs: block.txs.len() as u64,
        };

        vec![WebsocketOutEvent::NewBlock(api_block)]
    }

    fn handle_new_tx(event: NodeStateEvent) -> Vec<WebsocketOutEvent> {
        let NodeStateEvent::NewBlock(block) = event;
        let mut txs = Vec::new();

        for (idx, (id, tx)) in block.txs.iter().enumerate() {
            let metadata = tx.metadata(id.0.clone());
            let transaction_type = match tx.transaction_data {
                TransactionData::Blob(_) => TransactionTypeDb::BlobTransaction,
                TransactionData::Proof(_) => TransactionTypeDb::ProofTransaction,
                TransactionData::VerifiedProof(_) => TransactionTypeDb::ProofTransaction,
            };
            let transaction_status = sdk::api::TransactionStatusDb::Sequenced;
            let lane_id = block.lane_ids.get(&metadata.id.1).cloned();
            let api_tx = APITransaction {
                tx_hash: metadata.id.1,
                parent_dp_hash: metadata.id.0,
                version: metadata.version,
                transaction_type,
                transaction_status,
                block_hash: Some(block.hash.clone()),
                index: Some(idx as u32),
                timestamp: Some(block.block_timestamp.clone()),
                lane_id,
            };
            txs.push(WebsocketOutEvent::NewTx(api_tx));
        }
        txs
    }
}
