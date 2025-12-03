use std::sync::Arc;

use alloy_consensus::{Header, Transaction as _};
use alloy_eips::eip2718::Encodable2718;
use alloy_genesis::{ChainConfig, Genesis};
use alloy_primitives::{Address, B256};
use alloy_rlp::decode_exact;
use anyhow::{anyhow, bail, Context, Error};
use borsh::de::BorshDeserialize;
use hyli_model::{Calldata, HyliOutput, ProgramId, ProofData, StateCommitment, StructuredBlobData};
use reth_ethereum::{chainspec::ChainSpec, evm::EthEvmConfig};
use reth_ethereum_primitives::Block;
use reth_primitives_traits::{Block as BlockTrait, SignerRecoverable};
use reth_stateless::{
    trie::StatelessSparseTrie, validation::stateless_validation, StatelessInput,
    UncompressedPublicKey,
};
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

    stateless_validation(
        stateless_input.block.clone(),
        public_keys,
        stateless_input.witness.clone(),
        chain_spec,
        evm_config,
    )
    .context("stateless validation failed")?;
    tracing::debug!(
        target: "hyli::verifiers::reth",
        "Stateless validation passed"
    );

    validate_blob_matches_block(&calldata, &stateless_input, initial_state_root)
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
            Ok(bytes)
        })
        .collect()
}

fn validate_blob_matches_block(
    calldata: &Calldata,
    stateless_input: &StatelessInput,
    parent_state_root: B256,
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
    let (tx_index, tx) = block
        .body()
        .transactions()
        .enumerate()
        .find(|(_, tx)| tx.encoded_2718() == tx_payload)
        .ok_or_else(|| {
            tracing::warn!(
                target: "hyli::verifiers::reth",
                identity = %calldata.identity.0,
                tx_hash = %calldata.tx_hash.0,
                blob_len = tx_payload.len(),
                "no block transaction matches blob payload"
            );
            anyhow!("block missing transaction matching blob payload")
        })?;
    let tx_raw = tx.encoded_2718();

    let block_tx_hash = format!("0x{}", hex::encode(tx.tx_hash()));
    tracing::debug!(
        target: "hyli::verifiers::reth",
        proof_tx_hash = %calldata.tx_hash.0,
        block_tx_hash = %block_tx_hash,
        tx_index,
        "Block transaction hash compared against proof payload"
    );

    let signer = tx
        .recover_signer()
        .map_err(|err| anyhow!("failed to recover signer address: {err}"))?;
    let expected_identity = format!(
        "0x{}@{}",
        hex::encode(signer.as_slice()),
        blob.contract_name.0
    );
    // if calldata.identity.0 != expected_identity {
    // tracing::warn!(
    //         target: "hyli::verifiers::reth",
    //         expected_identity = %expected_identity,
    //         provided_identity = %calldata.identity.0,
    //         tx_hash = %calldata.tx_hash.0,
    //         "identity mismatch while validating reth blob"
    //     );
    //     bail!(
    //         "identity mismatch: expected {}, got {}",
    //         expected_identity,
    //         calldata.identity.0
    //     );
    // }

    let contract_address: Address = match tx.kind() {
        alloy_primitives::TxKind::Call(address) => address,
        alloy_primitives::TxKind::Create => {
            bail!("transaction must target an existing contract, found contract creation")
        }
    };

    let (trie, bytecode_map) =
        StatelessSparseTrie::new(&stateless_input.witness, parent_state_root)
            .context("failed to reconstruct trie from execution witness")?;

    // debug line show the whole content of the trie
    tracing::debug!(
        target: "hyli::verifiers::reth",
        trie_content = ?trie,
        "Reconstructed trie content from execution witness"
    );

    let account = match trie
        .account(contract_address)
        .context("failed to fetch contract account from witness")?
    {
        Some(account) => account,
        None => {
            tracing::warn!(
                target: "hyli::verifiers::reth",
                contract = %format!("0x{}", hex::encode(contract_address.as_slice())),
                tx_hash = %calldata.tx_hash.0,
                "execution witness missing account data for contract; skipping bytecode check"
            );
            return Ok(());
        }
    };

    if !bytecode_map.contains_key(&account.code_hash) {
        tracing::warn!(
            target: "hyli::verifiers::reth",
            contract = %format!("0x{}", hex::encode(contract_address.as_slice())),
            tx_hash = %calldata.tx_hash.0,
            code_hash = %hex::encode(account.code_hash),
            "execution witness missing bytecode for contract"
        );
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
