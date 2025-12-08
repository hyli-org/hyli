use std::sync::Arc;

use alloy_consensus::Header;
use alloy_eips::eip2718::Encodable2718;
use alloy_genesis::{ChainConfig, Genesis};
use alloy_primitives::{keccak256, B256};
use alloy_rlp::decode_exact;
use anyhow::{anyhow, bail, Context, Error};
use borsh::de::BorshDeserialize;
use hyli_model::{
    Blob, Calldata, ContractName, HyliOutput, ProgramId, ProofData, StateCommitment,
    StructuredBlobData,
};
use k256::ecdsa::SigningKey;
use reth_ethereum::{chainspec::ChainSpec, evm::EthEvmConfig};
use reth_ethereum_primitives::{Block, EthereumReceipt};
use reth_primitives_traits::{Block as BlockTrait, SignerRecoverable};
use reth_stateless::{validation::stateless_validation, StatelessInput, UncompressedPublicKey};
use serde_json::{self, Map, Value};

pub fn verify(proof: &ProofData, program_id: &ProgramId) -> Result<Vec<HyliOutput>, Error> {
    let _ = program_id;
    let (calldata, stateless_input, evm_bytes) =
        deserialize_reth_payload(&proof.0).context("failed to decode reth proof payload")?;
    let chain_spec =
        parse_chain_spec(&evm_bytes).context("failed to extract chain spec from proof")?;
    let evm_config = EthEvmConfig::new(chain_spec.clone());
    let (initial_state_root, next_state_root) = derive_state_roots(&stateless_input)
        .context("failed to derive state commitments from stateless input")?;
    tracing::info!(
        target: "hyli::verifiers::reth",
        identity = %calldata.identity.0,
        tx_hash = %calldata.tx_hash.0,
        "Starting Reth proof verification"
    );

    tracing::debug!(
        target: "hyli::verifiers::reth",
        initial_state_len = initial_state_root.as_slice().len(),
        next_state_len = next_state_root.as_slice().len(),
        "Derived state commitments from block and witness"
    );

    let public_keys = recover_public_keys(&stateless_input.block)
        .context("failed to recover transaction public keys")?;
    tracing::debug!(
        target: "hyli::verifiers::reth",
        public_key_count = public_keys.len(),
        "Recovered transaction public keys"
    );

    let (block_hash, block_execution_output) = stateless_validation(
        stateless_input.block.clone(),
        public_keys,
        stateless_input.witness.clone(),
        chain_spec,
        evm_config,
    )
    .context("stateless validation failed")?;
    ensure_successful_receipts(block_hash, &block_execution_output.result.receipts)?;
    tracing::debug!(
        target: "hyli::verifiers::reth",
        "Stateless validation passed"
    );

    validate_blob_matches_block(&calldata, &stateless_input, program_id)
        .context("blob transaction does not match block contents")?;
    tracing::debug!(
        target: "hyli::verifiers::reth",
        "Calldata blob matches block contents"
    );

    let output = HyliOutput {
        version: 0,
        initial_state: StateCommitment(initial_state_root.as_slice().to_vec()),
        next_state: StateCommitment(next_state_root.as_slice().to_vec()),
        identity: calldata.identity.clone(),
        index: calldata.index,
        blobs: calldata.blobs.clone(),
        tx_blob_count: calldata.tx_blob_count,
        tx_hash: calldata.tx_hash.clone(),
        success: true,
        state_reads: Vec::new(),
        tx_ctx: calldata.tx_ctx.clone(),
        onchain_effects: Vec::new(),
        program_outputs: Vec::new(),
    };

    tracing::info!(
        target: "hyli::verifiers::reth",
        identity = %output.identity.0,
        tx_hash = %output.tx_hash.0,
        blob_count = output.tx_blob_count,
        "Reth proof verification succeeded"
    );

    Ok(vec![output])
}

fn parse_chain_spec(bytes: &[u8]) -> Result<Arc<ChainSpec>, Error> {
    let value: Value =
        serde_json::from_slice(bytes).context("failed to deserialize evm_config json value")?;

    let mut genesis = extract_genesis(&value)?;
    if let Some(chain_config) = extract_chain_config(&value)? {
        genesis.config = chain_config;
    }

    Ok(Arc::new(genesis.into()))
}

fn derive_state_roots(input: &StatelessInput) -> Result<(B256, B256), Error> {
    let mut parent_header: Option<Header> = None;
    for raw_header in &input.witness.headers {
        let header: Header = decode_exact(raw_header.as_ref())
            .context("failed to decode ancestor header from execution witness")?;
        let should_replace = parent_header
            .as_ref()
            .map(|current| header.number > current.number)
            .unwrap_or(true);
        if should_replace {
            parent_header = Some(header);
        }
    }

    let parent_header = parent_header.ok_or_else(|| {
        anyhow!("execution witness missing ancestor header with parent state root")
    })?;

    let initial_state = parent_header.state_root;
    let next_state = input.block.header().state_root;

    Ok((initial_state, next_state))
}

fn extract_genesis(value: &Value) -> Result<Genesis, Error> {
    if let Some(genesis_value) = find_value_by_key(value, "genesis") {
        return parse_genesis(genesis_value);
    }

    if let Some(genesis_json) =
        find_value_by_key(value, "genesis_json").or_else(|| find_value_by_key(value, "genesisJson"))
    {
        if let Some(json_str) = genesis_json.as_str() {
            return serde_json::from_str(json_str).context("failed to parse genesis json string");
        }
    }

    if let Some(chain_spec_value) =
        find_value_by_key(value, "chain_spec").or_else(|| find_value_by_key(value, "chainSpec"))
    {
        if let Some(genesis_value) = find_value_by_key(chain_spec_value, "genesis") {
            return parse_genesis(genesis_value);
        }
        if let Some(genesis_json) = find_value_by_key(chain_spec_value, "genesis_json")
            .or_else(|| find_value_by_key(chain_spec_value, "genesisJson"))
        {
            if let Some(json_str) = genesis_json.as_str() {
                return serde_json::from_str(json_str)
                    .context("failed to parse genesis json string inside chain spec");
            }
        }
    }

    if let Value::Object(map) = value {
        let mut trimmed: Map<String, Value> = map.clone();
        for key in [
            "initial_state_root",
            "initialStateRoot",
            "next_state_root",
            "nextStateRoot",
            "extra_data",
            "chain_spec",
            "chainSpec",
            "chain_config",
            "chainConfig",
            "chain_id",
            "chainId",
            "evm_config",
            "evmConfig",
            "block_assembler_extra_data",
            "blockAssemblerExtraData",
        ] {
            trimmed.remove(key);
        }

        if trimmed.contains_key("nonce") && trimmed.contains_key("timestamp") {
            if let Ok(genesis) = serde_json::from_value::<Genesis>(Value::Object(trimmed)) {
                return Ok(genesis);
            }
        }
    }

    bail!("evm config did not include a genesis specification")
}

fn parse_genesis(value: &Value) -> Result<Genesis, Error> {
    match value {
        Value::String(json) => {
            serde_json::from_str(json).context("failed to parse genesis string from evm config")
        }
        _ => serde_json::from_value(value.clone())
            .context("failed to parse genesis object from evm config"),
    }
}

fn extract_chain_config(value: &Value) -> Result<Option<ChainConfig>, Error> {
    for key in ["chain_config", "chainConfig"] {
        if let Some(node) = find_value_by_key(value, key) {
            return match node {
                Value::String(json) => {
                    let config = serde_json::from_str(json)
                        .context("failed to parse chain config string from evm config")?;
                    Ok(Some(config))
                }
                _ => {
                    let config = serde_json::from_value(node.clone())
                        .context("failed to parse chain config object from evm config")?;
                    Ok(Some(config))
                }
            };
        }
    }
    Ok(None)
}

fn deserialize_reth_payload(
    mut payload: &[u8],
) -> Result<(Calldata, StatelessInput, Vec<u8>), Error> {
    let calldata_bytes = take_segment(&mut payload).context("missing calldata bytes")?;
    let stateless_bytes = take_segment(&mut payload).context("missing stateless bytes")?;
    let evm_bytes = take_segment(&mut payload).context("missing evm summary bytes")?;

    if !payload.is_empty() {
        bail!("unexpected trailing bytes in proof payload");
    }

    let calldata =
        Calldata::try_from_slice(&calldata_bytes).context("failed to deserialize Hyli calldata")?;
    let stateless_input: StatelessInput = bincode::deserialize(&stateless_bytes)
        .context("failed to deserialize Reth stateless input")?;

    Ok((calldata, stateless_input, evm_bytes))
}

fn take_segment(buf: &mut &[u8]) -> Result<Vec<u8>, Error> {
    if buf.len() < 4 {
        bail!("proof payload truncated");
    }
    let (len_bytes, rest) = buf.split_at(4);
    let mut len_buf = [0u8; 4];
    len_buf.copy_from_slice(len_bytes);
    let len = u32::from_le_bytes(len_buf) as usize;
    if rest.len() < len {
        bail!("proof payload segment shorter than advertised length");
    }
    let (data, remaining) = rest.split_at(len);
    *buf = remaining;
    Ok(data.to_vec())
}

fn recover_public_keys(block: &Block) -> Result<Vec<UncompressedPublicKey>, Error> {
    block
        .body()
        .transactions()
        .enumerate()
        .map(|(index, tx)| {
            let key = tx
                .signature()
                .recover_from_prehash(&tx.signature_hash())
                .map_err(|err| anyhow!("failed to recover signer for tx #{index}: {err}"))?;
            let encoded = key.to_encoded_point(false);
            let bytes: [u8; 65] = encoded
                .as_bytes()
                .try_into()
                .map_err(|_| anyhow!("unexpected public key length"))?;
            Ok(UncompressedPublicKey(bytes))
        })
        .collect()
}

fn ensure_successful_receipts(block_hash: B256, receipts: &[EthereumReceipt]) -> Result<(), Error> {
    if let Some((index, receipt)) = receipts
        .iter()
        .enumerate()
        .find(|(_, receipt)| !receipt.success)
    {
        tracing::warn!(
            target: "hyli::verifiers::reth",
            block_hash = %block_hash,
            tx_index = index,
            cumulative_gas_used = receipt.cumulative_gas_used,
            "stateless validation produced a failing receipt"
        );
        bail!(
            "stateless validation returned failing receipt for tx #{index} in block {block_hash}: status=false cumulative_gas_used={}",
            receipt.cumulative_gas_used
        );
    }
    Ok(())
}

fn validate_blob_matches_block(
    calldata: &Calldata,
    stateless_input: &StatelessInput,
    program_id: &ProgramId,
) -> Result<(), Error> {
    let block = &stateless_input.block;
    if calldata.tx_blob_count == 0 {
        bail!("proof specified zero blobs for transaction");
    }
    if calldata.index.0 >= calldata.tx_blob_count {
        bail!(
            "calldata blob index {} out of bounds for tx blob count {}",
            calldata.index.0,
            calldata.tx_blob_count
        );
    }

    let blob = calldata
        .blobs
        .get(&calldata.index)
        .ok_or_else(|| anyhow!("calldata missing blob at index {}", calldata.index.0))?;

    let structured_payload = StructuredBlobData::<Vec<u8>>::try_from(blob.data.clone()).ok();
    let tx_payload = structured_payload
        .as_ref()
        .map(|data| data.parameters.clone())
        .unwrap_or_else(|| blob.data.0.clone());

    let block_txs = block.body().transactions().collect::<Vec<_>>();

    let tx_count = block_txs.len();
    if tx_count != 1 {
        bail!("block must contain exactly one transaction, found {tx_count}");
    }

    let Some(eth_block_tx) = block_txs.first().filter(|t| t.encoded_2718() == tx_payload) else {
        tracing::warn!(
            target: "hyli::verifiers::reth",
            identity = %calldata.identity.0,
            tx_hash = %calldata.tx_hash.0,
            blob_len = tx_payload.len(),
            "no block transaction matches blob payload"
        );
        bail!("block missing transaction matching blob payload");
    };

    // Recovers the block tx signer public key and compares it against the caller blob's derived program id
    let signer_public_key = eth_block_tx
        .signature()
        .recover_from_prehash(&eth_block_tx.signature_hash())
        .map_err(|err| anyhow!("failed to recover signer public key: {err}"))?;
    let signer_program_id =
        program_id_from_uncompressed_bytes(signer_public_key.to_encoded_point(false).as_bytes())?;

    // program_id stores the full uncompressed pubkey (65 bytes); recover_signer returns only the
    // 20-byte address, so we compare against the recovered public key instead.
    let _ = eth_block_tx
        .recover_signer()
        .map_err(|err| anyhow!("failed to recover signer address: {err}"))?;

    if signer_program_id == *program_id {
        let structured_payload = structured_payload
            .ok_or_else(|| anyhow!("structured blob required when tx signer matches program id"))?;
        let caller_index = structured_payload
            .caller
            .ok_or_else(|| anyhow!("caller blob required when tx signer matches program id"))?;
        let caller_blob = calldata
            .blobs
            .get(&caller_index)
            .ok_or_else(|| anyhow!("calldata missing caller blob at index {}", caller_index.0))?;
        validate_program_signer_caller(caller_blob, program_id)?;
    }

    Ok(())
}

fn find_value_by_key<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    match value {
        Value::Object(map) => {
            if let Some(found) = map.get(key) {
                return Some(found);
            }
            for nested in map.values() {
                if let Some(found) = find_value_by_key(nested, key) {
                    return Some(found);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                if let Some(found) = find_value_by_key(item, key) {
                    return Some(found);
                }
            }
        }
        _ => {}
    }
    None
}

fn validate_program_signer_caller(caller_blob: &Blob, program_id: &ProgramId) -> Result<(), Error> {
    let derived_program_id = derive_program_pubkey(&caller_blob.contract_name);
    if derived_program_id != *program_id {
        bail!(
            "program id does not match derived caller program id: expected {}, got {}",
            hex::encode(&derived_program_id.0),
            hex::encode(&program_id.0)
        );
    }
    Ok(())
}

fn derive_program_pubkey(contract_name: &ContractName) -> ProgramId {
    let mut seed: [u8; 32] = keccak256(contract_name.0.as_bytes()).into();
    let signing_key = loop {
        match SigningKey::from_slice(&seed) {
            Ok(key) => break key,
            Err(_) => {
                seed = keccak256(seed).into();
            }
        }
    };
    program_id_from_uncompressed_bytes(
        signing_key
            .verifying_key()
            .to_encoded_point(false)
            .as_bytes(),
    )
    .expect("derived program public key should be uncompressed (65 bytes)")
}

fn program_id_from_uncompressed_bytes(bytes: &[u8]) -> Result<ProgramId, Error> {
    if bytes.len() != 65 {
        bail!("unexpected public key length: {}", bytes.len());
    }
    Ok(ProgramId(bytes.to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::{Header as AlloyHeader, SignableTransaction, TxEip1559, TxEnvelope};
    use alloy_genesis::ChainConfig;
    use alloy_primitives::{Address, Bytes, ChainId, FixedBytes, Signature, TxKind, U256};
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;
    use borsh::to_vec;
    use hyli_model::{
        Blob, BlobData, BlobIndex, Calldata, ContractName, Identity, IndexedBlobs, TxHash,
    };
    use reth_ethereum_primitives::{Block as EthBlock, BlockBody, TransactionSigned, TxType};
    use reth_stateless::{ExecutionWitness, StatelessInput};

    fn sample_tx(signer: &PrivateKeySigner) -> TransactionSigned {
        let tx = TxEip1559 {
            chain_id: ChainId::from(1u64),
            nonce: 0,
            max_fee_per_gas: 1u128,
            max_priority_fee_per_gas: 1u128,
            gas_limit: 21_000,
            to: TxKind::Call(Address::ZERO),
            value: U256::ZERO,
            input: Bytes::new(),
            access_list: Default::default(),
        };
        let signature = signer.sign_hash_sync(&tx.signature_hash()).unwrap();
        let envelope: TxEnvelope = tx.into_signed(signature).into();
        envelope.clone().into()
    }

    #[test]
    fn derive_program_pubkey_is_deterministic_and_uncompressed() {
        let name = ContractName("test/contract".to_string());
        let first = derive_program_pubkey(&name);
        let second = derive_program_pubkey(&name);
        assert_eq!(
            first, second,
            "program id derivation should be deterministic"
        );
        assert_eq!(
            first.0.len(),
            65,
            "expected uncompressed secp256k1 public key (65 bytes)"
        );
    }

    #[test]
    fn validate_program_signer_caller_accepts_matching_program_id() {
        let caller_blob = Blob {
            contract_name: ContractName("caller-contract".to_string()),
            data: BlobData(Vec::new()),
        };
        let program_id = derive_program_pubkey(&caller_blob.contract_name);
        validate_program_signer_caller(&caller_blob, &program_id)
            .expect("matching program id should succeed");
    }

    #[test]
    fn validate_program_signer_caller_rejects_mismatching_program_id() {
        let caller_blob = Blob {
            contract_name: ContractName("caller-contract".to_string()),
            data: BlobData(Vec::new()),
        };
        let correct_program_id = derive_program_pubkey(&caller_blob.contract_name);
        let wrong_program_id = derive_program_pubkey(&ContractName("other".to_string()));
        assert_ne!(correct_program_id, wrong_program_id);
        let err = validate_program_signer_caller(&caller_blob, &wrong_program_id)
            .expect_err("mismatched program id should fail");
        assert!(
            err.to_string()
                .contains("program id does not match derived caller program id"),
            "unexpected error message: {err:?}"
        );
    }

    #[test]
    fn verify_enforces_caller_when_program_signs() {
        // Set up a caller blob and derive the program id from it.
        let caller_contract = ContractName("caller-contract".to_string());
        let program_id = derive_program_pubkey(&caller_contract);

        // Craft a tx signed with the program_id-derived key.
        let signing_key =
            SigningKey::from_slice(keccak256(caller_contract.0.as_bytes()).as_slice()).unwrap();
        let signer = PrivateKeySigner::from(signing_key);
        let tx = TxEip1559 {
            chain_id: ChainId::from(1u64),
            nonce: 0,
            max_fee_per_gas: 1u128,
            max_priority_fee_per_gas: 1u128,
            gas_limit: 21_000,
            to: TxKind::Call(Address::ZERO),
            value: U256::ZERO,
            input: Bytes::new(),
            access_list: Default::default(),
        };
        let signature = signer.sign_hash_sync(&tx.signature_hash()).unwrap();
        let envelope: TxEnvelope = tx.into_signed(signature).into();
        let tx_bytes = envelope.encoded_2718();
        let tx_signed: TransactionSigned = envelope.clone().into();

        // Build a structured blob for the program signer (index 0) that points to caller index 1.
        let structured = StructuredBlobData {
            caller: Some(BlobIndex(1)),
            callees: None,
            parameters: tx_bytes.clone(),
        };
        let program_blob = Blob {
            contract_name: ContractName("program-contract".to_string()),
            data: BlobData(to_vec(&structured).unwrap()),
        };

        // Caller blob that determines the program id.
        let caller_blob = Blob {
            contract_name: caller_contract.clone(),
            data: BlobData(Vec::new()),
        };

        let blobs: IndexedBlobs = vec![program_blob.clone(), caller_blob.clone()].into();

        // Minimal block with the signed tx.
        let header = AlloyHeader {
            parent_hash: B256::ZERO,
            ommers_hash: B256::ZERO,
            beneficiary: Address::ZERO,
            state_root: B256::ZERO,
            transactions_root: B256::ZERO,
            receipts_root: B256::ZERO,
            withdrawals_root: None,
            logs_bloom: Default::default(),
            difficulty: U256::ZERO,
            number: 0,
            gas_limit: 30_000_000,
            gas_used: 0,
            timestamp: 0,
            extra_data: Default::default(),
            mix_hash: B256::ZERO,
            nonce: FixedBytes::ZERO,
            base_fee_per_gas: Some(0),
            blob_gas_used: None,
            excess_blob_gas: None,
            parent_beacon_block_root: None,
            requests_hash: None,
        };

        let block = EthBlock {
            header,
            body: BlockBody {
                transactions: vec![tx_signed.clone()],
                ..Default::default()
            },
        };

        // Stateless input placeholder: empty witness is OK for this check path.
        let stateless_input = StatelessInput {
            block: block.clone(),
            witness: ExecutionWitness::default(),
            chain_config: ChainConfig::default(),
        };

        // Calldata referencing the program blob (index 0) with correct blob data.
        let calldata = Calldata {
            tx_hash: TxHash(format!("0x{}", hex::encode(tx_signed.tx_hash()))),
            identity: Identity("id".to_string()),
            blobs: blobs.clone(),
            tx_blob_count: blobs.len(),
            index: BlobIndex(0),
            tx_ctx: None,
            private_input: Vec::new(),
        };

        // Should succeed because caller blob derives the same program id as the signer.
        validate_blob_matches_block(&calldata, &stateless_input, &program_id)
            .expect("validation should accept matching caller/program_id");

        // Now tamper the caller contract name so derivation mismatches.
        let bad_caller_blob = Blob {
            contract_name: ContractName("different".to_string()),
            data: BlobData(Vec::new()),
        };
        let bad_blobs: IndexedBlobs = vec![program_blob, bad_caller_blob].into();
        let bad_calldata = Calldata {
            blobs: bad_blobs,
            ..calldata
        };
        let err = validate_blob_matches_block(&bad_calldata, &stateless_input, &program_id)
            .expect_err("validation should fail when caller-derived program id mismatches");
        assert!(err
            .to_string()
            .contains("program id does not match derived caller program id"));
    }

    #[test]
    fn blob_payload_must_match_transaction() {
        let signer = PrivateKeySigner::random();
        let tx_signed = sample_tx(&signer);

        let block = EthBlock {
            header: Default::default(),
            body: BlockBody {
                transactions: vec![tx_signed.clone()],
                ..Default::default()
            },
        };

        let stateless_input = StatelessInput {
            block,
            witness: ExecutionWitness::default(),
            chain_config: ChainConfig::default(),
        };

        let wrong_blob = Blob {
            contract_name: ContractName("program".into()),
            data: BlobData(vec![0u8; tx_signed.encoded_2718().len()]), // incorrect payload
        };
        let caller_blob = Blob {
            contract_name: ContractName("caller".into()),
            data: BlobData(Vec::new()),
        };
        let blobs: IndexedBlobs = vec![wrong_blob, caller_blob].into();
        let calldata = Calldata {
            tx_hash: TxHash(format!("0x{}", hex::encode(tx_signed.tx_hash()))),
            identity: Identity("id".to_string()),
            blobs,
            tx_blob_count: 2,
            index: BlobIndex(0),
            tx_ctx: None,
            private_input: Vec::new(),
        };

        let err = validate_blob_matches_block(
            &calldata,
            &stateless_input,
            &derive_program_pubkey(&ContractName("program".into())),
        )
        .expect_err("blob payload not matching tx should fail");
        assert!(
            err.to_string()
                .contains("transaction matching blob payload"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn block_must_contain_single_transaction() {
        let signer = PrivateKeySigner::random();
        let tx_a = sample_tx(&signer);
        let tx_b = sample_tx(&signer);

        let block = EthBlock {
            header: Default::default(),
            body: BlockBody {
                transactions: vec![tx_a.clone(), tx_b],
                ..Default::default()
            },
        };

        let stateless_input = StatelessInput {
            block,
            witness: ExecutionWitness::default(),
            chain_config: ChainConfig::default(),
        };

        let program_blob = Blob {
            contract_name: ContractName("program".into()),
            data: BlobData(tx_a.encoded_2718()),
        };
        let caller_blob = Blob {
            contract_name: ContractName("caller".into()),
            data: BlobData(Vec::new()),
        };
        let blobs: IndexedBlobs = vec![program_blob, caller_blob].into();
        let calldata = Calldata {
            tx_hash: TxHash(format!("0x{}", hex::encode(tx_a.tx_hash()))),
            identity: Identity("id".to_string()),
            blobs,
            tx_blob_count: 2,
            index: BlobIndex(0),
            tx_ctx: None,
            private_input: Vec::new(),
        };

        let err = validate_blob_matches_block(
            &calldata,
            &stateless_input,
            &derive_program_pubkey(&ContractName("program".into())),
        )
        .expect_err("blocks with multiple transactions should be rejected");
        assert!(
            err.to_string()
                .contains("block must contain exactly one transaction"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn invalid_signature_recovery_fails() {
        let signer = PrivateKeySigner::random();
        let tx_signed = sample_tx(&signer);

        // Use an obviously invalid signature so recovery fails.
        let invalid_sig = Signature::new(U256::ZERO, U256::ZERO, false);
        let transaction = tx_signed.clone().into_typed_transaction();
        let tx_invalid = TransactionSigned::new_unchecked(transaction, invalid_sig, B256::ZERO);

        let block = EthBlock {
            header: Default::default(),
            body: BlockBody {
                transactions: vec![tx_invalid],
                ..Default::default()
            },
        };

        let err = recover_public_keys(&block)
            .expect_err("invalid signatures should not recover to public keys");
        assert!(
            err.to_string().contains("failed to recover signer"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn receipts_with_failure_error() {
        let receipts = vec![
            EthereumReceipt {
                tx_type: TxType::Legacy,
                success: true,
                cumulative_gas_used: 1,
                logs: Vec::new(),
            },
            EthereumReceipt {
                tx_type: TxType::Legacy,
                success: false,
                cumulative_gas_used: 2,
                logs: Vec::new(),
            },
        ];

        let err = ensure_successful_receipts(B256::ZERO, &receipts).unwrap_err();
        assert!(
            err.to_string().contains("failing receipt"),
            "expected failing receipt error, got: {err}"
        );
    }

    #[test]
    fn receipts_all_success_ok() {
        let receipts = vec![EthereumReceipt {
            tx_type: TxType::Legacy,
            success: true,
            cumulative_gas_used: 1,
            logs: Vec::new(),
        }];

        ensure_successful_receipts(B256::ZERO, &receipts).expect("all receipts succeeded");
    }
}
