pub mod handlers;
pub mod types;

use anyhow::{Context, Result};
use axum::{extract::State, routing::post, Json, Router};
use hyli_modules::{
    bus::SharedMessageBus, module_bus_client, module_handle_messages, modules::Module,
};
use sdk::ContractName;
use std::net::SocketAddr;
use tokio_util::sync::CancellationToken;
use tracing::info;

use handlers::HandlerCtx;
use types::{JsonRpcRequest, JsonRpcResponse};

module_bus_client! {
    struct RpcProxyBusClient {}
}

pub struct HylaneRpcProxyCtx {
    pub rpc_port: u16,
    pub node_url: String,
    pub hyli_chain_id: u64,
    pub bridge_cn: ContractName,
    pub hyperlane_cn: ContractName,
    pub token_cn: ContractName,
    pub relayer_identity: sdk::Identity,
}

pub struct HylaneRpcProxyModule {
    port: u16,
    handler_ctx: HandlerCtx,
    bus: RpcProxyBusClient,
}

impl Module for HylaneRpcProxyModule {
    type Context = HylaneRpcProxyCtx;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let handler_ctx = HandlerCtx::new(
            ctx.node_url,
            ctx.hyli_chain_id,
            ctx.bridge_cn,
            ctx.hyperlane_cn,
            ctx.token_cn,
            ctx.relayer_identity,
        )
        .context("Building RPC proxy handler context")?;

        Ok(HylaneRpcProxyModule {
            port: ctx.rpc_port,
            handler_ctx,
            bus: RpcProxyBusClient::new_from_bus(bus.new_handle()).await,
        })
    }

    async fn run(&mut self) -> Result<()> {
        self.serve().await
    }
}

impl HylaneRpcProxyModule {
    async fn serve(&mut self) -> Result<()> {
        info!(
            "📡  Starting Hyperlane JSON-RPC proxy on port {}",
            self.port
        );

        let ctx = self.handler_ctx.clone();
        let app = Router::new().route("/", post(rpc_handler)).with_state(ctx);

        let addr = SocketAddr::from(([0, 0, 0, 0], self.port));
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .with_context(|| format!("Binding JSON-RPC proxy on port {}", self.port))?;

        let cancel = CancellationToken::new();
        let server = tokio::spawn({
            let token = cancel.clone();
            async move {
                let _ = axum::serve(listener, app)
                    .with_graceful_shutdown(async move { token.cancelled().await })
                    .await;
            }
        });

        module_handle_messages! {
            on_self self,
            delay_shutdown_until {
                cancel.cancel();
                server.is_finished()
            },
        };

        Ok(())
    }
}

// ── Axum handler ──────────────────────────────────────────────────────────────

async fn rpc_handler(
    State(ctx): State<HandlerCtx>,
    Json(req): Json<JsonRpcRequest>,
) -> Json<JsonRpcResponse> {
    let id = req.id.clone();
    let params = &req.params;

    let resp = match req.method.as_str() {
        "eth_blockNumber" => handlers::eth_block_number(&ctx, id).await,
        "eth_chainId" => handlers::eth_chain_id(&ctx, id).await,
        "net_version" => handlers::net_version(&ctx, id).await,
        "eth_getBlockByNumber" => handlers::eth_get_block_by_number(&ctx, id, params).await,
        "eth_getLogs" => handlers::eth_get_logs(&ctx, id, params).await,
        "eth_call" => handlers::eth_call(&ctx, id, params).await,
        "eth_sendTransaction" => handlers::eth_send_raw_transaction(&ctx, id, params).await,
        "eth_sendRawTransaction" => handlers::eth_send_raw_transaction(&ctx, id, params).await,
        "eth_getTransactionReceipt" => {
            handlers::eth_get_transaction_receipt(&ctx, id, params).await
        }
        "eth_estimateGas" => JsonRpcResponse::ok(id, serde_json::json!("0x186a0")),
        "eth_getTransactionCount" => JsonRpcResponse::ok(id, serde_json::json!("0x0")),
        "eth_gasPrice" => JsonRpcResponse::ok(id, serde_json::json!("0x1")),
        "eth_getBalance" => JsonRpcResponse::ok(
            id,
            serde_json::json!("0xde0b6b3a7640000"), // 1 ETH
        ),
        other => JsonRpcResponse::method_not_found(id, other),
    };

    Json(resp)
}
