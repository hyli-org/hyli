use std::sync::Arc;

use alloy_eips::eip2718::Encodable2718;
use alloy_genesis::{ChainConfig, Genesis};
use alloy_primitives::Bytes;
use anyhow::{anyhow, bail, Context, Error};
use borsh::de::BorshDeserialize;
use hyli_model::{Calldata, HyliOutput, ProgramId, ProofData, StateCommitment};
use reth_ethereum::{chainspec::ChainSpec, evm::EthEvmConfig};
use reth_ethereum_primitives::Block;
use reth_primitives_traits::{Block as BlockTrait, SignerRecoverable};
use reth_stateless::{validation::stateless_validation, StatelessInput, UncompressedPublicKey};
use serde::Deserialize;
use serde_json;

#[derive(Deserialize)]
struct EvmConfigSummary {
    chain_id: u64,
    extra_data: String,
    initial_state_root: String,
    next_state_root: String,
}

pub fn verify(proof: &ProofData, _program_id: &ProgramId) -> Result<Vec<HyliOutput>, Error> {
    let (calldata, stateless_input, evm_summary) =
        deserialize_reth_payload(&proof.0).context("failed to decode reth proof payload")?;

    tracing::info!(
        target: "hyli::verifiers::reth",
        identity = %calldata.identity.0,
        tx_hash = %calldata.tx_hash.0,
        chain_id = evm_summary.chain_id,
        "Starting Reth proof verification"
    );

    let chain_spec = custom_chain_spec().context("failed to build custom chain spec")?;
    tracing::debug!(
        target: "hyli::verifiers::reth",
        configured_chain_id = chain_spec.chain().id(),
        summary_chain_id = evm_summary.chain_id,
        "Loaded custom chain spec"
    );
    if chain_spec.chain().id() != evm_summary.chain_id {
        bail!(
            "chain id mismatch: proof={} chain_spec={}",
            evm_summary.chain_id,
            chain_spec.chain().id()
        );
    }

    let mut evm_config = EthEvmConfig::new(chain_spec.clone());
    if !evm_summary.extra_data.is_empty() {
        let data = hex::decode(evm_summary.extra_data.trim_start_matches("0x"))
            .context("invalid EVM extra data")?;
        evm_config.block_assembler.extra_data = Bytes::from(data);
        tracing::debug!(
            target: "hyli::verifiers::reth",
            extra_data_len = evm_config.block_assembler.extra_data.len(),
            "Applied EVM extra data override"
        );
    }

    let initial_state = hex::decode(evm_summary.initial_state_root.trim_start_matches("0x"))
        .context("invalid initial state root")?;
    let next_state = hex::decode(evm_summary.next_state_root.trim_start_matches("0x"))
        .context("invalid next state root")?;
    tracing::debug!(
        target: "hyli::verifiers::reth",
        initial_state_len = initial_state.len(),
        next_state_len = next_state.len(),
        "Decoded state commitments from proof payload"
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

    validate_blob_matches_block(&calldata, &stateless_input.block)
        .context("blob transaction does not match block contents")?;
    tracing::debug!(
        target: "hyli::verifiers::reth",
        "Calldata blob matches block contents"
    );

    let output = HyliOutput {
        version: 0,
        initial_state: StateCommitment(initial_state),
        next_state: StateCommitment(next_state),
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

fn deserialize_reth_payload(
    mut payload: &[u8],
) -> Result<(Calldata, StatelessInput, EvmConfigSummary), Error> {
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
    let evm_summary: EvmConfigSummary =
        serde_json::from_slice(&evm_bytes).context("failed to deserialize EVM config summary")?;

    Ok((calldata, stateless_input, evm_summary))
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

fn validate_blob_matches_block(calldata: &Calldata, block: &Block) -> Result<(), Error> {
    if calldata.tx_blob_count != 1 {
        bail!(
            "proof expected a single blob, found {}",
            calldata.tx_blob_count
        );
    }

    let (blob_index, blob) = calldata
        .blobs
        .iter()
        .next()
        .ok_or_else(|| anyhow!("calldata carried no blobs"))?;
    if blob_index.0 != 0 {
        bail!("blob index must be zero for single-blob transactions");
    }

    let mut txs = block.body().transactions();
    let tx = txs
        .next()
        .ok_or_else(|| anyhow!("block contained no transactions"))?;
    let tx_raw = tx.encoded_2718();
    if blob.data.0 != tx_raw {
        bail!("blob payload does not match encoded transaction bytes");
    }

    let block_tx_hash = format!("0x{}", hex::encode(tx.tx_hash()));
    tracing::debug!(
        target: "hyli::verifiers::reth",
        proof_tx_hash = %calldata.tx_hash.0,
        block_tx_hash = %block_tx_hash,
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
    if calldata.identity.0 != expected_identity {
        bail!(
            "identity mismatch: expected {}, got {}",
            expected_identity,
            calldata.identity.0
        );
    }

    Ok(())
}

fn custom_chain_spec() -> Result<Arc<ChainSpec>, Error> {
    let chain_conf = r#"{
        "chainId": 2600,
        "homesteadBlock": 0,
        "daoForkBlock": 0,
        "daoForkSupport": true,
        "eip150Block": 0,
        "eip155Block": 0,
        "eip158Block": 0,
        "byzantiumBlock": 0,
        "constantinopleBlock": 0,
        "petersburgBlock": 0,
        "istanbulBlock": 0,
        "muirGlacierBlock": 0,
        "berlinBlock": 0,
        "londonBlock": 0,
        "arrowGlacierBlock": 0,
        "grayGlacierBlock": 0,
        "bedrockBlock": 0,
        "mergeNetsplitBlock": 0,
        "terminalTotalDifficulty": "0",
        "regolithTime": 0,
        "shanghaiTime": 1704992401,
        "canyonTime": 1704992401
    }"#;

    let chain_config: ChainConfig =
        serde_json::from_str(chain_conf).context("failed to parse custom chain config")?;

    let genesis_json = format!(
        r#"{{
    "nonce": "0x42",
    "timestamp": "1704992401",
    "extraData": "0x5343",
    "gasLimit": "0x5208000",
    "difficulty": "0x400000000",
    "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
    "coinbase": "0x0000000000000000000000000000000000000000",
    "alloc": {{
        "0x6Be02d1d3665660d22FF9624b7BE0551ee1Ac91b": {{
            "balance": "0x4a47e3c12448f4ad000000"
        }},
        "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266": {{
            "balance": "0xD3C21BCECCEDA1000000"
        }},
        "0x70997970C51812dc3A010C7d01b50e0d17dc79C8": {{
            "balance": "0xD3C21BCECCEDA1000000"
        }},
        "0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC": {{
            "balance": "0xD3C21BCECCEDA1000000"
        }}
    }},
    "number": "0x0",
    "gasUsed": "0x0",
    "parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
    "config": {config}
}}"#,
        config = serde_json::to_string(&chain_config)?
    );

    let genesis: Genesis =
        serde_json::from_str(&genesis_json).context("failed to parse genesis json")?;

    Ok(Arc::new(genesis.into()))
}
