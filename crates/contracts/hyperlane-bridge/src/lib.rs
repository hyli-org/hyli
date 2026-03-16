use borsh::{BorshDeserialize, BorshSerialize};
use sha2::{Digest, Sha256};

use alloy_consensus::TxEnvelope;
use alloy_eips::eip2718::Decodable2718;
use alloy_sol_types::{sol, SolCall};

use sdk::{
    utils::parse_calldata, Blob, BlobData, BlobIndex, Calldata, ContractAction, ContractName,
    Identity, IndexedBlobs, RunResult, StructuredBlob, StructuredBlobData, ZkContract,
};
use smt_token::SmtTokenAction;

#[cfg(feature = "client")]
pub mod client;

#[cfg(feature = "server")]
pub mod server;

/// Bridge state — holds the names of the two companion contracts.
#[derive(Debug, Clone, BorshDeserialize, BorshSerialize)]
pub struct HyperlaneBridgeState {
    pub hyperlane_contract: ContractName,
    pub token_contract: ContractName,
}

impl Default for HyperlaneBridgeState {
    fn default() -> Self {
        HyperlaneBridgeState {
            hyperlane_contract: ContractName("hyperlane".to_string()),
            token_contract: ContractName("oranj".to_string()),
        }
    }
}

/// Actions that the bridge contract can execute.
#[derive(Debug, Clone, BorshDeserialize, BorshSerialize, PartialEq)]
pub enum HyperlaneBridgeAction {
    /// Hyli → Ethereum: lock tokens in the bridge reserve and emit a Hyperlane message.
    TransferRemote {
        eth_recipient: [u8; 32],
        amount: u128,
    },
    /// Ethereum → Hyli: verify an incoming Hyperlane message and release tokens.
    ProcessMessage,
}

impl ContractAction for HyperlaneBridgeAction {
    fn as_blob(
        &self,
        contract_name: ContractName,
        caller: Option<BlobIndex>,
        callees: Option<Vec<BlobIndex>>,
    ) -> Blob {
        Blob {
            contract_name,
            data: BlobData::from(StructuredBlobData {
                caller,
                callees,
                parameters: self.clone(),
            }),
        }
    }
}

// Automatic full-state rollback on error.
impl sdk::FullStateRevert for HyperlaneBridgeState {}

// ---------------------------------------------------------------------------
// Solidity ABI selectors for Hyperlane contracts
// ---------------------------------------------------------------------------

sol! {
    /// WarpRoute.transferRemote(destDomain, recipient, amount)
    function transferRemote(uint32 destination, bytes32 recipient, uint256 amount) external payable;

    /// Mailbox.process(metadata, message)
    function process(bytes metadata, bytes message) external payable;
}

/// Name the bridge contract is expected to be registered under.
pub const BRIDGE_CONTRACT_NAME: &str = "hyperlane-bridge";

// ---------------------------------------------------------------------------
// ZkContract implementation
// ---------------------------------------------------------------------------

impl ZkContract for HyperlaneBridgeState {
    fn execute(&mut self, calldata: &Calldata) -> RunResult {
        let (action, mut execution_ctx) = parse_calldata::<HyperlaneBridgeAction>(calldata)?;

        let output = match action {
            HyperlaneBridgeAction::TransferRemote {
                eth_recipient: _,
                amount,
            } => {
                // 1. Verify a smt-token Transfer TO this bridge for `amount`.
                check_smt_transfer_to_bridge(&calldata.blobs, &self.token_contract, amount)?;
                // 2. Verify the reth blob encodes a transferRemote for the same `amount`.
                check_reth_transfer_remote(&calldata.blobs, &self.hyperlane_contract, amount)?;
                "TransferRemote executed".to_string()
            }

            HyperlaneBridgeAction::ProcessMessage => {
                // 1. Decode the incoming Hyperlane message from the reth blob.
                let (recipient_bytes, amount) =
                    decode_token_message_from_reth_blob(&calldata.blobs, &self.hyperlane_contract)?;
                let recipient = bytes32_to_identity(&recipient_bytes);
                // 2. Assert the callee smt-token blob transfers exactly that amount FROM the bridge.
                execution_ctx.is_in_callee_blobs(
                    &self.token_contract,
                    SmtTokenAction::Transfer {
                        sender: Identity::from(BRIDGE_CONTRACT_NAME),
                        recipient,
                        amount,
                    },
                )?;
                "ProcessMessage executed".to_string()
            }
        };

        Ok((output.into_bytes(), execution_ctx, vec![]))
    }

    /// State commitment = SHA-256(hyperlane_contract || token_contract).
    fn commit(&self) -> sdk::StateCommitment {
        let mut hasher = Sha256::new();
        hasher.update(self.hyperlane_contract.0.as_bytes());
        hasher.update(self.token_contract.0.as_bytes());
        sdk::StateCommitment(hasher.finalize().to_vec())
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Convert a 32-byte slice to a Hyli `Identity` by stripping trailing null bytes.
fn bytes32_to_identity(bytes: &[u8; 32]) -> Identity {
    let end = bytes
        .iter()
        .rposition(|&b| b != 0)
        .map(|i| i + 1)
        .unwrap_or(0);
    Identity(String::from_utf8_lossy(&bytes[..end]).into_owned())
}

/// Extract the calldata (`input`) field from any supported EIP-2718 transaction type.
fn extract_tx_input(tx: &TxEnvelope) -> &[u8] {
    match tx {
        TxEnvelope::Legacy(t) => t.tx().input.as_ref(),
        TxEnvelope::Eip2930(t) => t.tx().input.as_ref(),
        TxEnvelope::Eip1559(t) => t.tx().input.as_ref(),
        TxEnvelope::Eip7702(t) => t.tx().input.as_ref(),
        _ => &[],
    }
}

/// Find the hyperlane reth blob, decode the EIP-2718 transaction, and return
/// `(recipient_bytes32, amount)` extracted from either a `transferRemote` or
/// a `Mailbox.process` call.
pub fn decode_token_message_from_reth_blob(
    blobs: &IndexedBlobs,
    hyperlane_cn: &ContractName,
) -> Result<([u8; 32], u128), String> {
    for (_, blob) in blobs {
        if &blob.contract_name != hyperlane_cn {
            continue;
        }

        // Strip optional StructuredBlobData wrapper to obtain raw EIP-2718 bytes.
        let raw_bytes: Vec<u8> =
            if let Ok(structured) = StructuredBlobData::<Vec<u8>>::try_from(blob.data.clone()) {
                structured.parameters
            } else {
                blob.data.0.clone()
            };

        // Decode the EIP-2718 encoded Ethereum transaction.
        let tx = TxEnvelope::decode_2718(&mut raw_bytes.as_slice())
            .map_err(|e| format!("Failed to decode EIP-2718 tx: {e}"))?;

        let input = extract_tx_input(&tx);
        if input.len() < 4 {
            return Err("Transaction input too short to contain a function selector".to_string());
        }

        // ---- WarpRoute.transferRemote(uint32, bytes32, uint256) ----
        if input.starts_with(&transferRemoteCall::SELECTOR) {
            let decoded = transferRemoteCall::abi_decode_raw(&input[4..])
                .map_err(|e| format!("Failed to ABI-decode transferRemote: {e}"))?;
            let amount: u128 = u128::try_from(decoded.amount)
                .map_err(|_| "transferRemote amount exceeds u128".to_string())?;
            let recipient: [u8; 32] = decoded.recipient.into();
            return Ok((recipient, amount));
        }

        // ---- Mailbox.process(bytes, bytes) ----
        if input.starts_with(&processCall::SELECTOR) {
            let decoded = processCall::abi_decode_raw(&input[4..])
                .map_err(|e| format!("Failed to ABI-decode process call: {e}"))?;

            // Hyperlane Message layout (binary, not ABI-encoded):
            //   version (1) + nonce (4) + origin (4) + sender (32)
            //   + destination (4) + recipient (32) = 77 bytes header
            //   + body: token_recipient (32) + amount (32) + optional metadata
            let message: &[u8] = decoded.message.as_ref();
            const HEADER_LEN: usize = 77;
            if message.len() < HEADER_LEN + 64 {
                return Err(
                    "Hyperlane message body too short to contain a TokenMessage".to_string()
                );
            }
            let body = &message[HEADER_LEN..];
            let mut recipient = [0u8; 32];
            recipient.copy_from_slice(&body[..32]);

            // uint256 amount is big-endian; we only keep the lower 128 bits.
            let mut amount_bytes = [0u8; 16];
            amount_bytes.copy_from_slice(&body[48..64]);
            let amount = u128::from_be_bytes(amount_bytes);

            return Ok((recipient, amount));
        }

        return Err(format!(
            "Unknown Hyperlane function selector in reth blob: {:02x?}",
            &input[..4]
        ));
    }

    Err(format!(
        "No hyperlane blob found for contract '{}'",
        hyperlane_cn.0
    ))
}

/// Scan blobs for a smt-token `Transfer { recipient: "hyperlane-bridge", amount }`.
fn check_smt_transfer_to_bridge(
    blobs: &IndexedBlobs,
    token_cn: &ContractName,
    expected_amount: u128,
) -> Result<(), String> {
    for (_, blob) in blobs {
        if &blob.contract_name != token_cn {
            continue;
        }
        let Ok(structured) = StructuredBlob::<SmtTokenAction>::try_from(blob.clone()) else {
            continue;
        };
        match structured.data.parameters {
            SmtTokenAction::Transfer {
                recipient, amount, ..
            } => {
                if recipient.0 != BRIDGE_CONTRACT_NAME {
                    continue;
                }
                if amount != expected_amount {
                    return Err(format!(
                        "smt-token Transfer amount {amount} does not match expected {expected_amount}"
                    ));
                }
                return Ok(());
            }
            _ => continue,
        }
    }
    Err(format!(
        "No smt-token Transfer to '{BRIDGE_CONTRACT_NAME}' found for amount {expected_amount}"
    ))
}

/// Verify that the reth blob encodes a `transferRemote` call with `expected_amount`.
fn check_reth_transfer_remote(
    blobs: &IndexedBlobs,
    hyperlane_cn: &ContractName,
    expected_amount: u128,
) -> Result<(), String> {
    let (_, amount) = decode_token_message_from_reth_blob(blobs, hyperlane_cn)?;
    if amount != expected_amount {
        return Err(format!(
            "reth transferRemote amount {amount} does not match expected {expected_amount}"
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope};
    use alloy_eips::eip2718::Encodable2718;
    use alloy_primitives::{Address, Bytes, ChainId, FixedBytes, TxKind, U256};
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;
    use sdk::{BlobIndex, Calldata, ContractName, IndexedBlobs, TxHash};

    // -----------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------

    fn make_state() -> HyperlaneBridgeState {
        HyperlaneBridgeState::default()
    }

    fn sign_and_encode(input: Vec<u8>) -> Vec<u8> {
        let signer = PrivateKeySigner::random();
        let tx = TxEip1559 {
            chain_id: ChainId::from(1u64),
            nonce: 0,
            max_fee_per_gas: 1u128,
            max_priority_fee_per_gas: 1u128,
            gas_limit: 100_000,
            to: TxKind::Call(Address::ZERO),
            value: U256::ZERO,
            input: Bytes::from(input),
            access_list: Default::default(),
        };
        let signature = signer.sign_hash_sync(&tx.signature_hash()).unwrap();
        let envelope: TxEnvelope = tx.into_signed(signature).into();
        let mut encoded = vec![];
        envelope.encode_2718(&mut encoded);
        encoded
    }

    fn make_transfer_remote_reth_blob(
        hyperlane_cn: ContractName,
        recipient: [u8; 32],
        amount: u128,
    ) -> Blob {
        let calldata = transferRemoteCall {
            destination: 1,
            recipient: FixedBytes::from(recipient),
            amount: U256::from(amount),
        }
        .abi_encode();
        let encoded = sign_and_encode(calldata);
        Blob {
            contract_name: hyperlane_cn,
            data: BlobData(encoded),
        }
    }

    fn make_process_reth_blob(
        hyperlane_cn: ContractName,
        hyli_recipient: &str,
        amount: u128,
    ) -> Blob {
        // Build the Hyperlane TokenMessage body (abi.encodePacked style).
        let mut body = vec![0u8; 77]; // header placeholder
        body[0] = 3; // version

        // Token recipient (bytes32): UTF-8 string, zero-padded.
        let mut token_recipient = [0u8; 32];
        let id_bytes = hyli_recipient.as_bytes();
        let copy_len = id_bytes.len().min(32);
        token_recipient[..copy_len].copy_from_slice(&id_bytes[..copy_len]);
        body.extend_from_slice(&token_recipient);

        // Amount (uint256, 32 bytes, big-endian — lower 16 bytes hold u128).
        let mut amount_bytes = [0u8; 32];
        amount_bytes[16..].copy_from_slice(&amount.to_be_bytes());
        body.extend_from_slice(&amount_bytes);

        let calldata = processCall {
            metadata: Bytes::new(),
            message: Bytes::from(body),
        }
        .abi_encode();
        let encoded = sign_and_encode(calldata);
        Blob {
            contract_name: hyperlane_cn,
            data: BlobData(encoded),
        }
    }

    fn make_smt_transfer_blob(
        token_cn: ContractName,
        sender: &str,
        recipient: &str,
        amount: u128,
        caller: Option<BlobIndex>,
    ) -> Blob {
        SmtTokenAction::Transfer {
            sender: Identity::from(sender),
            recipient: Identity::from(recipient),
            amount,
        }
        .as_blob(token_cn, caller, None)
    }

    fn make_bridge_blob(
        action: HyperlaneBridgeAction,
        caller: Option<BlobIndex>,
        callees: Option<Vec<BlobIndex>>,
    ) -> Blob {
        action.as_blob(
            ContractName("hyperlane-bridge".to_string()),
            caller,
            callees,
        )
    }

    fn make_calldata(index: BlobIndex, blobs: Vec<(BlobIndex, Blob)>) -> Calldata {
        Calldata {
            tx_hash: TxHash::default(),
            identity: Identity::from("alice@hydentity"),
            blobs: IndexedBlobs(blobs),
            tx_blob_count: 4,
            index,
            tx_ctx: None,
            private_input: vec![],
        }
    }

    // -----------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------

    /// TransferRemote: valid blobs → execution succeeds.
    #[test]
    fn test_transfer_remote_valid() {
        let state = make_state();
        let amount = 1_000u128;
        let eth_recipient = [0u8; 32];

        let bridge_blob = make_bridge_blob(
            HyperlaneBridgeAction::TransferRemote {
                eth_recipient,
                amount,
            },
            None,
            None,
        );
        let smt_blob = make_smt_transfer_blob(
            state.token_contract.clone(),
            "alice@hydentity",
            BRIDGE_CONTRACT_NAME,
            amount,
            None,
        );
        let reth_blob =
            make_transfer_remote_reth_blob(state.hyperlane_contract.clone(), eth_recipient, amount);

        let blobs = vec![
            (BlobIndex(0), bridge_blob),
            (BlobIndex(1), smt_blob),
            (BlobIndex(2), reth_blob),
        ];
        let calldata = make_calldata(BlobIndex(0), blobs);
        let mut s = state.clone();
        let result = s.execute(&calldata);
        assert!(result.is_ok(), "expected Ok but got: {:?}", result.err());
    }

    /// TransferRemote: smt-token amount mismatch → execution fails.
    #[test]
    fn test_transfer_remote_amount_mismatch() {
        let state = make_state();
        let amount = 1_000u128;
        let wrong_amount = 999u128;
        let eth_recipient = [0u8; 32];

        let bridge_blob = make_bridge_blob(
            HyperlaneBridgeAction::TransferRemote {
                eth_recipient,
                amount,
            },
            None,
            None,
        );
        // smt-token blob carries the WRONG amount
        let smt_blob = make_smt_transfer_blob(
            state.token_contract.clone(),
            "alice@hydentity",
            BRIDGE_CONTRACT_NAME,
            wrong_amount,
            None,
        );
        let reth_blob =
            make_transfer_remote_reth_blob(state.hyperlane_contract.clone(), eth_recipient, amount);

        let blobs = vec![
            (BlobIndex(0), bridge_blob),
            (BlobIndex(1), smt_blob),
            (BlobIndex(2), reth_blob),
        ];
        let calldata = make_calldata(BlobIndex(0), blobs);
        let mut s = state.clone();
        let result = s.execute(&calldata);
        assert!(result.is_err(), "expected Err for amount mismatch");
    }

    /// ProcessMessage: valid blobs → execution succeeds.
    #[test]
    fn test_process_message_valid() {
        let state = make_state();
        let hyli_recipient = "alice@hydentity";
        let amount = 500u128;

        // Blob 0: hyperlane reth blob (process)
        let reth_blob =
            make_process_reth_blob(state.hyperlane_contract.clone(), hyli_recipient, amount);
        // Blob 1: hyperlane-bridge (ProcessMessage), callee = blob 2
        let bridge_blob = make_bridge_blob(
            HyperlaneBridgeAction::ProcessMessage,
            None,
            Some(vec![BlobIndex(2)]),
        );
        // Blob 2: smt-token Transfer (FROM bridge TO alice), caller = blob 1
        let smt_blob = make_smt_transfer_blob(
            state.token_contract.clone(),
            BRIDGE_CONTRACT_NAME,
            hyli_recipient,
            amount,
            Some(BlobIndex(1)),
        );

        let blobs = vec![
            (BlobIndex(0), reth_blob),
            (BlobIndex(1), bridge_blob),
            (BlobIndex(2), smt_blob),
        ];
        let calldata = make_calldata(BlobIndex(1), blobs);
        let mut s = state.clone();
        let result = s.execute(&calldata);
        assert!(result.is_ok(), "expected Ok but got: {:?}", result.err());
    }

    /// ProcessMessage: smt amount exceeds message amount → execution fails.
    #[test]
    fn test_process_message_drain_attempt() {
        let state = make_state();
        let hyli_recipient = "alice@hydentity";
        let message_amount = 500u128;
        let drain_amount = 9_999u128; // attacker tries to drain more

        let reth_blob = make_process_reth_blob(
            state.hyperlane_contract.clone(),
            hyli_recipient,
            message_amount,
        );
        let bridge_blob = make_bridge_blob(
            HyperlaneBridgeAction::ProcessMessage,
            None,
            Some(vec![BlobIndex(2)]),
        );
        // smt-token blob claims a larger amount than the message authorises
        let smt_blob = make_smt_transfer_blob(
            state.token_contract.clone(),
            BRIDGE_CONTRACT_NAME,
            hyli_recipient,
            drain_amount,
            Some(BlobIndex(1)),
        );

        let blobs = vec![
            (BlobIndex(0), reth_blob),
            (BlobIndex(1), bridge_blob),
            (BlobIndex(2), smt_blob),
        ];
        let calldata = make_calldata(BlobIndex(1), blobs);
        let mut s = state.clone();
        let result = s.execute(&calldata);
        assert!(result.is_err(), "expected Err for drain attempt");
    }
}
