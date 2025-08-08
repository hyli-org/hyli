use std::time::Duration;

use anyhow::Result;
use assertables::assert_ok;
use hyle_crypto::BlstCrypto;
use hyle_model::{Blob, BlobTransaction, ContractName, Hashed, Identity, NodeStateEvent};
use sha3::Digest;
use tracing::info;

use crate::{
    bus::{bus_client, BusClientReceiver, BusClientSender},
    mempool::api::RestApiMessage,
    model::verifiers::{BlstSignatureBlob, ShaBlob},
    rest::RestApi,
    utils::integration_test::NodeIntegrationCtxBuilder,
};

bus_client! {
    struct Client {
        sender(RestApiMessage),
        receiver(NodeStateEvent),
    }
}

#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn test_blst_native_verifier() {
    let contract_name: ContractName = "blst".into();
    let identity: Identity = format!("bob@{contract_name}").into();

    let crypto = BlstCrypto::new_random().expect("crypto");

    let data = vec![1, 2, 3, 4, 5, 6];

    let signature = crypto
        .sign([data.clone(), identity.0.as_bytes().to_vec()].concat())
        .expect("failed to sign");

    let blob = BlstSignatureBlob {
        identity: identity.clone(),
        data,
        signature: signature.signature.signature.0,
        public_key: crypto.validator_pubkey().0.clone(),
    };

    let res = scenario(identity, blob.as_blob()).await;
    assert_ok!(res);
}

#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn test_sha3_256_native_verifier() {
    let contract_name: ContractName = "sha3_256".into();
    let identity: Identity = format!("bob@{contract_name}").into();

    let data = vec![1, 2, 3, 4, 5, 6];

    let mut hasher = sha3::Sha3_256::new();
    hasher.update(&data);
    let sha = hasher.finalize().to_vec();

    let blob = ShaBlob {
        identity: identity.clone(),
        data,
        sha,
    };

    let res = scenario(identity, blob.as_blob(contract_name)).await;
    assert_ok!(res);
}

async fn scenario(identity: Identity, blob: Blob) -> Result<()> {
    let mut node_modules = NodeIntegrationCtxBuilder::new().await;
    node_modules.conf.consensus.slot_duration = Duration::from_millis(200);
    let mut node_modules = node_modules.skip::<RestApi>().build().await?;

    let mut node_client = Client::new_from_bus(node_modules.bus.new_handle()).await;

    // Wait until we process the genesis block
    node_modules.wait_for_processed_genesis().await?;

    let blob_tx = BlobTransaction::new(identity.clone(), vec![blob.clone()]);
    let blob_tx_hash = blob_tx.hashed();
    node_client.send(RestApiMessage::NewTx(blob_tx.clone().into()))?;

    // Wait until we commit this TX
    loop {
        let evt: NodeStateEvent = node_client.recv().await?;
        match evt {
            NodeStateEvent::NewBlock(block) => {
                info!("Got Block");
                if block
                    .successful_txs
                    .iter()
                    .any(|tx_hash| tx_hash == &blob_tx_hash)
                {
                    break;
                }
            }
        }
    }

    Ok(())
}
