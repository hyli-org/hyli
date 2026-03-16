use anyhow::{Context, Result};
use client_sdk::transaction_builder::{
    ProvableBlobTx, TxExecutorHandler as TxExecutorHandlerTrait,
};

pub mod metadata {
    pub const HYPERLANE_BRIDGE_ELF: &[u8] = include_bytes!("../../hyperlane-bridge.img");
    pub const PROGRAM_ID: [u8; 32] = sdk::str_to_u8(include_str!("../../hyperlane-bridge.txt"));
}
use sdk::{
    utils::as_hyli_output, BlobIndex, Calldata, ContractName, Identity, StateCommitment, ZkContract,
};
use smt_token::{client::tx_executor_handler::SmtTokenProvableState, SmtTokenAction};

use crate::{HyperlaneBridgeAction, HyperlaneBridgeState, BRIDGE_CONTRACT_NAME};

/// Client-side state handler for the hyperlane-bridge contract.
pub struct TxExecutorHandler {
    pub state: HyperlaneBridgeState,
    pub smt_executor: SmtTokenProvableState,
}

impl TxExecutorHandlerTrait for TxExecutorHandler {
    type Contract = HyperlaneBridgeState;

    fn handle(&mut self, calldata: &Calldata) -> Result<sdk::HyliOutput> {
        let initial_state = self.state.commit();
        let mut res = self.state.execute(calldata);
        let next_state = self.state.commit();
        Ok(as_hyli_output(
            initial_state,
            next_state,
            calldata,
            &mut res,
        ))
    }

    fn build_commitment_metadata(&self, _calldata: &Calldata) -> Result<Vec<u8>> {
        borsh::to_vec(&self.state).context("Failed to serialize HyperlaneBridgeState")
    }

    fn construct_state(
        _contract_name: &ContractName,
        _contract: &sdk::Contract,
        metadata: &Option<Vec<u8>>,
    ) -> Result<Self> {
        let state = match metadata {
            Some(m) => borsh::from_slice::<HyperlaneBridgeState>(m)
                .context("Failed to deserialize HyperlaneBridgeState")?,
            None => HyperlaneBridgeState::default(),
        };
        Ok(Self {
            state,
            smt_executor: SmtTokenProvableState::default(),
        })
    }

    fn get_state_commitment(&self) -> StateCommitment {
        self.state.commit()
    }
}

impl TxExecutorHandler {
    /// Add blobs for a Hyli → Ethereum transfer.
    ///
    /// Adds:
    /// 1. `hyperlane-bridge` blob: `TransferRemote { eth_recipient, amount }`
    /// 2. `smt-token` Transfer blob: `sender → "hyperlane-bridge"` for `amount`
    ///
    /// The caller must add the `hyperlane` reth blob separately (it requires
    /// building the Ethereum transaction + reth proof externally).
    pub fn transfer_remote(
        &self,
        builder: &mut ProvableBlobTx,
        sender: Identity,
        eth_recipient: [u8; 32],
        amount: u128,
    ) -> anyhow::Result<()> {
        // Blob N: hyperlane-bridge TransferRemote.
        // The smt-token and reth blobs are found by scanning — no callee wiring needed.
        builder.add_action(
            ContractName(BRIDGE_CONTRACT_NAME.to_string()),
            HyperlaneBridgeAction::TransferRemote {
                eth_recipient,
                amount,
            },
            None,
            None,
            None,
        )?;

        // Blob N+1: smt-token Transfer from sender to the bridge.
        builder.add_action(
            self.state.token_contract.clone(),
            SmtTokenAction::Transfer {
                sender,
                recipient: Identity::from(BRIDGE_CONTRACT_NAME),
                amount,
            },
            None,
            None,
            None,
        )?;
        Ok(())
    }

    /// Add blobs for an Ethereum → Hyli message processing.
    ///
    /// Adds:
    /// 1. `hyperlane-bridge` blob: `ProcessMessage`, callee = next blob index
    /// 2. `smt-token` Transfer blob: `"hyperlane-bridge" → recipient`, caller = bridge blob
    ///
    /// The caller must add the `hyperlane` reth blob separately.
    pub fn process_message(
        &self,
        builder: &mut ProvableBlobTx,
        recipient: Identity,
        amount: u128,
    ) -> anyhow::Result<()> {
        let bridge_idx = BlobIndex(builder.blobs.len());
        let smt_idx = BlobIndex(bridge_idx.0 + 1);

        // Blob bridge_idx: ProcessMessage with callee = smt_idx.
        builder.add_action(
            ContractName(BRIDGE_CONTRACT_NAME.to_string()),
            HyperlaneBridgeAction::ProcessMessage,
            None,
            None,
            Some(vec![smt_idx]),
        )?;

        // Blob smt_idx: smt-token Transfer FROM bridge TO recipient, caller = bridge_idx.
        builder.add_action(
            self.state.token_contract.clone(),
            SmtTokenAction::Transfer {
                sender: Identity::from(BRIDGE_CONTRACT_NAME),
                recipient,
                amount,
            },
            None,
            Some(bridge_idx),
            None,
        )?;
        Ok(())
    }
}
