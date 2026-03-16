use alloy_consensus::TxEnvelope;
use alloy_eips::eip2718::Decodable2718;
use alloy_primitives::keccak256;
use alloy_sol_types::SolCall;
use anyhow::Result;
use client_sdk::rest_client::{IndexerApiHttpClient, NodeApiClient, NodeApiHttpClient};
use sdk::{Blob, BlobData, BlobTransaction, ContractName, Identity};
use serde_json::{json, Value};
use std::sync::Arc;

use crate::{
    client::TxExecutorHandler, processCall, server::rpc_proxy::types::JsonRpcResponse,
    HyperlaneBridgeAction, HyperlaneBridgeState, BRIDGE_CONTRACT_NAME,
};
use smt_token::client::tx_executor_handler::SmtTokenProvableState;

#[derive(Clone)]
pub struct HandlerCtx {
    pub node_url: String,
    pub hyli_chain_id: u64,
    pub bridge_cn: ContractName,
    pub hyperlane_cn: ContractName,
    pub token_cn: ContractName,
    pub relayer_identity: Identity,
    pub node_client: Arc<NodeApiHttpClient>,
    pub indexer_client: Arc<IndexerApiHttpClient>,
}

impl HandlerCtx {
    pub fn new(
        node_url: String,
        hyli_chain_id: u64,
        bridge_cn: ContractName,
        hyperlane_cn: ContractName,
        token_cn: ContractName,
        relayer_identity: Identity,
    ) -> Result<Self> {
        let node_client = Arc::new(NodeApiHttpClient::new(node_url.clone())?);
        let indexer_client = Arc::new(IndexerApiHttpClient::new(node_url.clone())?);
        Ok(Self {
            node_url,
            hyli_chain_id,
            bridge_cn,
            hyperlane_cn,
            token_cn,
            relayer_identity,
            node_client,
            indexer_client,
        })
    }
}

// ── Dispatch event topic0 ─────────────────────────────────────────────────────
fn dispatch_topic0() -> [u8; 32] {
    *keccak256("Dispatch(address,uint32,bytes32,bytes)")
}

// ── Block helpers ─────────────────────────────────────────────────────────────

pub async fn eth_block_number(ctx: &HandlerCtx, id: Value) -> JsonRpcResponse {
    match ctx.indexer_client.get_last_block().await {
        Ok(block) => JsonRpcResponse::ok(id, json!(format!("0x{:x}", block.height))),
        Err(e) => JsonRpcResponse::internal_error(id, format!("Failed to get last block: {e}")),
    }
}

pub async fn eth_chain_id(ctx: &HandlerCtx, id: Value) -> JsonRpcResponse {
    JsonRpcResponse::ok(id, json!(format!("0x{:x}", ctx.hyli_chain_id)))
}

pub async fn net_version(ctx: &HandlerCtx, id: Value) -> JsonRpcResponse {
    JsonRpcResponse::ok(id, json!(ctx.hyli_chain_id.to_string()))
}

pub async fn eth_get_block_by_number(
    ctx: &HandlerCtx,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    let block_tag = params.get(0).and_then(|v| v.as_str()).unwrap_or("latest");

    let block = if block_tag == "latest" {
        ctx.indexer_client.get_last_block().await
    } else {
        let height_str = block_tag.trim_start_matches("0x");
        match u64::from_str_radix(height_str, 16) {
            Ok(h) => {
                ctx.indexer_client
                    .get_block_by_height(&sdk::BlockHeight(h))
                    .await
            }
            Err(_) => return JsonRpcResponse::internal_error(id, "Invalid block number"),
        }
    };

    match block {
        Ok(b) => {
            let number = format!("0x{:x}", b.height);
            let timestamp = format!("0x{:x}", b.timestamp);
            let hash = format!("0x{}", b.hash);
            JsonRpcResponse::ok(
                id,
                json!({
                    "number": number,
                    "hash": hash,
                    "parentHash": format!("0x{}", b.parent_hash),
                    "timestamp": timestamp,
                    "transactions": [],
                    "gasLimit": "0x1c9c380",
                    "gasUsed": "0x0",
                    "baseFeePerGas": "0x1",
                    "extraData": "0x",
                    "logsBloom": "0x".to_string() + &"0".repeat(512),
                    "miner": "0x0000000000000000000000000000000000000000",
                    "difficulty": "0x0",
                    "totalDifficulty": "0x0",
                    "nonce": "0x0000000000000000",
                    "sha3Uncles": "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
                    "uncles": [],
                    "size": "0x1",
                    "stateRoot": "0x0000000000000000000000000000000000000000000000000000000000000000",
                    "receiptsRoot": "0x0000000000000000000000000000000000000000000000000000000000000000",
                    "transactionsRoot": "0x0000000000000000000000000000000000000000000000000000000000000000",
                }),
            )
        }
        Err(e) => JsonRpcResponse::internal_error(id, format!("Failed to get block: {e}")),
    }
}

// ── eth_getLogs ───────────────────────────────────────────────────────────────

pub async fn eth_get_logs(ctx: &HandlerCtx, id: Value, params: &Value) -> JsonRpcResponse {
    let filter = match params.get(0) {
        Some(f) => f.clone(),
        None => json!({}),
    };

    let from_block = parse_block_number(filter.get("fromBlock").and_then(|v| v.as_str()), 0);
    let to_block_tag = filter
        .get("toBlock")
        .and_then(|v| v.as_str())
        .unwrap_or("latest");

    // Fetch latest block height for "latest" resolution
    let latest_height = match ctx.indexer_client.get_last_block().await {
        Ok(b) => b.height,
        Err(e) => {
            return JsonRpcResponse::internal_error(id, format!("Failed to get latest block: {e}"))
        }
    };

    let to_block = parse_block_number(Some(to_block_tag), latest_height);

    // Fetch all blob transactions for the bridge contract
    let txs = match ctx
        .indexer_client
        .get_blob_transactions_by_contract(&ctx.bridge_cn)
        .await
    {
        Ok(t) => t,
        Err(e) => {
            return JsonRpcResponse::internal_error(
                id,
                format!("Failed to query bridge transactions: {e}"),
            )
        }
    };

    let mut logs = Vec::new();
    let topic0 = dispatch_topic0();

    for (log_index, tx) in txs.iter().enumerate() {
        let block_height = tx.block_height.0;
        if block_height < from_block || block_height > to_block {
            continue;
        }

        // Check if this tx contains a TransferRemote action blob
        let transfer_remote_blob = tx
            .blobs
            .iter()
            .find(|b| b.contract_name == ctx.bridge_cn.0 && is_transfer_remote_blob(&b.data));

        let Some(bridge_blob) = transfer_remote_blob else {
            continue;
        };

        // Parse TransferRemote parameters from the bridge blob
        let Some((eth_recipient, amount, destination)) = parse_transfer_remote(&bridge_blob.data)
        else {
            continue;
        };

        // Build Hyperlane message bytes
        let origin = (ctx.hyli_chain_id & 0xFFFF_FFFF) as u32;
        let nonce = (log_index & 0xFFFF_FFFF) as u32;

        // Sender: bridge contract name as 32-byte left-padded string
        let mut sender_bytes = [0u8; 32];
        let bridge_name = BRIDGE_CONTRACT_NAME.as_bytes();
        let copy_len = bridge_name.len().min(32);
        sender_bytes[32 - copy_len..].copy_from_slice(&bridge_name[..copy_len]);

        let message_bytes = build_hyperlane_message(
            3, // version
            nonce,
            origin,
            sender_bytes,
            destination,
            eth_recipient,
            amount,
        );

        // Build log topics
        let mut sender_topic = [0u8; 32];
        sender_topic[12..].copy_from_slice(&sender_bytes[20..32]); // address (last 20 bytes)

        let mut dest_topic = [0u8; 32];
        dest_topic[28..32].copy_from_slice(&destination.to_be_bytes());

        let msg_id = keccak256(&message_bytes);

        let log = json!({
            "address": "0x0000000000000000000000000000000000000001",
            "topics": [
                format!("0x{}", hex::encode(topic0)),
                format!("0x{}", hex::encode(sender_topic)),
                format!("0x{}", hex::encode(dest_topic)),
                format!("0x{}", hex::encode(eth_recipient)),
            ],
            "data": format!("0x{}", hex::encode(&message_bytes)),
            "blockNumber": format!("0x{:x}", block_height),
            "transactionHash": format!("0x{}", tx.tx_hash),
            "transactionIndex": format!("0x{:x}", tx.index),
            "blockHash": format!("0x{}", tx.block_hash),
            "logIndex": format!("0x{:x}", log_index),
            "removed": false,
            "messageId": format!("0x{}", hex::encode(*msg_id)),
        });

        logs.push(log);
    }

    JsonRpcResponse::ok(id, json!(logs))
}

/// Parse block number from a hex string tag like "0x1a3" or "latest"/"earliest".
fn parse_block_number(tag: Option<&str>, latest: u64) -> u64 {
    match tag {
        None | Some("latest") | Some("pending") => latest,
        Some("earliest") => 0,
        Some(s) => {
            let s = s.trim_start_matches("0x");
            u64::from_str_radix(s, 16).unwrap_or(0)
        }
    }
}

/// Check if a blob's data encodes a TransferRemote action.
fn is_transfer_remote_blob(data: &[u8]) -> bool {
    if let Ok(structured) =
        sdk::StructuredBlobData::<HyperlaneBridgeAction>::try_from(sdk::BlobData(data.to_vec()))
    {
        matches!(
            structured.parameters,
            HyperlaneBridgeAction::TransferRemote { .. }
        )
    } else {
        false
    }
}

/// Extract (eth_recipient, amount, destination_domain) from a bridge blob.
fn parse_transfer_remote(data: &[u8]) -> Option<([u8; 32], u128, u32)> {
    let structured =
        sdk::StructuredBlobData::<HyperlaneBridgeAction>::try_from(sdk::BlobData(data.to_vec()))
            .ok()?;
    if let HyperlaneBridgeAction::TransferRemote {
        eth_recipient,
        amount,
    } = structured.parameters
    {
        // Destination domain is not stored in the bridge blob; we derive it from the reth blob
        // if available. For the log synthesis we use 0 as a placeholder destination.
        Some((eth_recipient, amount, 0))
    } else {
        None
    }
}

/// Build a Hyperlane message byte string.
fn build_hyperlane_message(
    version: u8,
    nonce: u32,
    origin: u32,
    sender: [u8; 32],
    destination: u32,
    recipient: [u8; 32],
    amount: u128,
) -> Vec<u8> {
    let mut msg = Vec::with_capacity(77 + 64);
    msg.push(version);
    msg.extend_from_slice(&nonce.to_be_bytes());
    msg.extend_from_slice(&origin.to_be_bytes());
    msg.extend_from_slice(&sender);
    msg.extend_from_slice(&destination.to_be_bytes());
    msg.extend_from_slice(&recipient);
    // Body: token recipient (32 bytes) + amount (32 bytes, big-endian)
    msg.extend_from_slice(&recipient); // token recipient = same as message recipient
    let mut amount_bytes = [0u8; 32];
    amount_bytes[16..].copy_from_slice(&amount.to_be_bytes());
    msg.extend_from_slice(&amount_bytes);
    msg
}

// ── eth_call ──────────────────────────────────────────────────────────────────

pub async fn eth_call(_ctx: &HandlerCtx, id: Value, params: &Value) -> JsonRpcResponse {
    let data = params
        .get(0)
        .and_then(|obj| obj.get("data"))
        .and_then(|v| v.as_str())
        .unwrap_or("0x");

    let data_bytes = hex::decode(data.trim_start_matches("0x")).unwrap_or_default();

    if data_bytes.len() < 4 {
        return JsonRpcResponse::ok(id, json!("0x"));
    }

    let selector = &data_bytes[..4];

    // latestCheckpoint() -> (bytes32, uint32) stub
    let latest_checkpoint_sel = &keccak256("latestCheckpoint()")[..4];
    if selector == latest_checkpoint_sel {
        // Return (bytes32(0), uint32(0)) ABI-encoded (64 bytes)
        return JsonRpcResponse::ok(id, json!(format!("0x{}", "0".repeat(128))));
    }

    // threshold() -> uint8
    let threshold_sel = &keccak256("threshold()")[..4];
    if selector == threshold_sel {
        return JsonRpcResponse::ok(
            id,
            json!("0x0000000000000000000000000000000000000000000000000000000000000001"),
        );
    }

    // Default: return empty
    JsonRpcResponse::ok(id, json!("0x"))
}

// ── eth_sendRawTransaction / eth_sendTransaction ──────────────────────────────

pub async fn eth_send_raw_transaction(
    ctx: &HandlerCtx,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    // params[0] is either a hex-encoded EIP-2718 transaction or an object with "data"
    let raw_hex = if let Some(s) = params.get(0).and_then(|v| v.as_str()) {
        s.trim_start_matches("0x").to_string()
    } else if let Some(data) = params
        .get(0)
        .and_then(|obj| obj.get("data"))
        .and_then(|v| v.as_str())
    {
        data.trim_start_matches("0x").to_string()
    } else {
        return JsonRpcResponse::err(id, -32602, "Missing transaction data");
    };

    let raw_bytes = match hex::decode(&raw_hex) {
        Ok(b) => b,
        Err(e) => return JsonRpcResponse::err(id, -32602, format!("Invalid hex: {e}")),
    };

    // Decode EIP-2718 transaction
    let tx = match TxEnvelope::decode_2718(&mut raw_bytes.as_slice()) {
        Ok(t) => t,
        Err(e) => {
            return JsonRpcResponse::err(
                id,
                -32602,
                format!("Failed to decode EIP-2718 transaction: {e}"),
            )
        }
    };

    // Extract (recipient, amount) from the process() calldata
    let input = extract_tx_input(&tx);
    if input.len() < 4 || !input.starts_with(&processCall::SELECTOR) {
        return JsonRpcResponse::err(
            id,
            -32602,
            "Transaction does not call Mailbox.process(bytes,bytes)",
        );
    }

    let decoded = match processCall::abi_decode_raw(&input[4..]) {
        Ok(d) => d,
        Err(e) => {
            return JsonRpcResponse::err(id, -32602, format!("Failed to ABI-decode process: {e}"))
        }
    };

    let message: &[u8] = decoded.message.as_ref();
    const HEADER_LEN: usize = 77;
    if message.len() < HEADER_LEN + 64 {
        return JsonRpcResponse::err(
            id,
            -32602,
            "Hyperlane message too short to contain TokenMessage",
        );
    }
    let body = &message[HEADER_LEN..];
    let mut recipient_bytes = [0u8; 32];
    recipient_bytes.copy_from_slice(&body[..32]);
    let mut amount_bytes = [0u8; 16];
    amount_bytes.copy_from_slice(&body[48..64]);
    let amount = u128::from_be_bytes(amount_bytes);

    // Convert recipient bytes to Hyli Identity
    let end = recipient_bytes
        .iter()
        .rposition(|&b| b != 0)
        .map(|i| i + 1)
        .unwrap_or(0);
    let recipient = Identity(String::from_utf8_lossy(&recipient_bytes[..end]).into_owned());

    // Build the Hyli blob transaction
    let bridge_state = HyperlaneBridgeState {
        hyperlane_contract: ctx.hyperlane_cn.clone(),
        token_contract: ctx.token_cn.clone(),
    };
    let handler = TxExecutorHandler {
        state: bridge_state,
        smt_executor: SmtTokenProvableState::default(),
    };

    let mut builder =
        client_sdk::transaction_builder::ProvableBlobTx::new(ctx.relayer_identity.clone());

    // Blob 0: hyperlane reth blob (raw EIP-2718 bytes)
    builder.blobs.push(Blob {
        contract_name: ctx.hyperlane_cn.clone(),
        data: BlobData(raw_bytes.clone()),
    });

    // Blobs 1+2: bridge ProcessMessage + smt-token Transfer
    if let Err(e) = handler.process_message(&mut builder, recipient, amount) {
        return JsonRpcResponse::internal_error(id, format!("Failed to build process blobs: {e}"));
    }

    let blob_tx = BlobTransaction::new(builder.identity, builder.blobs);
    let tx_hash = match ctx.node_client.send_tx_blob(blob_tx).await {
        Ok(h) => h,
        Err(e) => return JsonRpcResponse::internal_error(id, format!("Failed to send tx: {e}")),
    };

    // Return as a 32-byte Ethereum-style tx hash
    let hash_str = tx_hash.to_string();
    let padded = if hash_str.len() < 64 {
        format!("{:0>64}", hash_str)
    } else {
        hash_str[..64].to_string()
    };
    JsonRpcResponse::ok(id, json!(format!("0x{}", padded)))
}

// ── eth_getTransactionReceipt ─────────────────────────────────────────────────

pub async fn eth_get_transaction_receipt(
    ctx: &HandlerCtx,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    let hash_hex = match params.get(0).and_then(|v| v.as_str()) {
        Some(h) => h.trim_start_matches("0x"),
        None => return JsonRpcResponse::ok(id, Value::Null),
    };

    // Attempt to find the transaction by hash in the indexer
    let tx_hash = match sdk::TxHash::from_hex(hash_hex) {
        Ok(h) => h,
        Err(_) => return JsonRpcResponse::ok(id, Value::Null),
    };

    match ctx.indexer_client.get_transaction_with_hash(&tx_hash).await {
        Ok(tx) => {
            let status = match tx.transaction_status {
                sdk::api::TransactionStatusDb::Success => "0x1",
                sdk::api::TransactionStatusDb::Failure => "0x0",
                _ => {
                    // Still pending
                    return JsonRpcResponse::ok(id, Value::Null);
                }
            };

            let block_number = tx
                .block_height
                .map(|h| format!("0x{:x}", h.0))
                .unwrap_or_default();
            let block_hash = tx
                .block_hash
                .as_ref()
                .map(|h| format!("0x{h}"))
                .unwrap_or_else(|| "0x".to_string());

            JsonRpcResponse::ok(
                id,
                json!({
                    "transactionHash": format!("0x{hash_hex}"),
                    "transactionIndex": "0x0",
                    "blockHash": block_hash,
                    "blockNumber": block_number,
                    "from": "0x0000000000000000000000000000000000000000",
                    "to": "0x0000000000000000000000000000000000000001",
                    "cumulativeGasUsed": "0x0",
                    "gasUsed": "0x0",
                    "logs": [],
                    "logsBloom": "0x".to_string() + &"0".repeat(512),
                    "status": status,
                    "type": "0x2",
                }),
            )
        }
        Err(_) => JsonRpcResponse::ok(id, Value::Null),
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn extract_tx_input(tx: &TxEnvelope) -> &[u8] {
    match tx {
        TxEnvelope::Legacy(t) => t.tx().input.as_ref(),
        TxEnvelope::Eip2930(t) => t.tx().input.as_ref(),
        TxEnvelope::Eip1559(t) => t.tx().input.as_ref(),
        TxEnvelope::Eip7702(t) => t.tx().input.as_ref(),
        _ => &[],
    }
}
