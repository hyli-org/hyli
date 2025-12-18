use std::{collections::HashSet, time::Duration};

use crate::{
    bus::{
        dont_use_this::get_receiver, metrics::BusMetrics, BusClientSender, BusEnvelope,
        SharedMessageBus,
    },
    modules::{
        contract_listener::{ContractListener, ContractListenerConf, ContractListenerEvent},
        signal::ShutdownModule,
        Module, ShutdownClient,
    },
};
use anyhow::Result;
use hyli_model::{api::TransactionStatusDb, BlobIndex, BlockHeight, ContractName, TxHash};
use sdk::{indexer::MIGRATOR, BlockHash, ConsensusProposalHash};
use sqlx::{postgres::PgPoolOptions, types::chrono::Utc, PgPool};
use testcontainers_modules::{
    postgres::Postgres,
    testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt},
};

use tokio::{
    sync::broadcast::{error::RecvError, Receiver},
    time::timeout,
};
use tracing::warn;

struct PgTestCtx {
    _container: ContainerAsync<Postgres>,
    pool: PgPool,
    database_url: String,
}

async fn setup_pg() -> Result<PgTestCtx> {
    let container = Postgres::default()
        .with_tag("17-alpine")
        .start()
        .await
        .expect("failed to start postgres container");

    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get mapped port");
    let database_url = format!("postgres://postgres:postgres@localhost:{port}/postgres");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    MIGRATOR.run(&pool).await?;

    Ok(PgTestCtx {
        _container: container,
        pool,
        database_url,
    })
}

fn hash(value: &str) -> String {
    format!("{value:0<64}")
}

async fn insert_block(pool: &PgPool, hash: &str, parent_hash: &str, height: i64) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO blocks (hash, parent_hash, height, timestamp, total_txs)
        VALUES ($1, $2, $3, $4, 1)
        "#,
    )
    .bind(hash)
    .bind(parent_hash)
    .bind(height)
    .bind(Utc::now())
    .execute(pool)
    .await?;

    Ok(())
}

async fn drain_event(
    receiver: &mut Receiver<BusEnvelope<ContractListenerEvent>>,
) -> Result<ContractListenerEvent> {
    loop {
        match receiver.recv().await {
            Ok(msg) => return Ok(msg.into_message()),
            Err(RecvError::Lagged(skipped)) => {
                warn!("contract_listener test: receiver lagged by {skipped} messages");
            }
            Err(RecvError::Closed) => anyhow::bail!("contract_listener test: receiver closed"),
        };
    }
}

async fn insert_settled_tx(
    pool: &PgPool,
    contract: &ContractName,
    height: i64,
) -> Result<(TxHash, String)> {
    let block_hash = hash(&format!("block-{height}"));
    let parent_hash = hash(&format!("block-{}", height - 1));
    insert_block(pool, &block_hash, &parent_hash, height).await?;

    let tx_hash = TxHash(hash(&format!("settled-{height}")));
    let parent_dp_hash = hash("dp-settled");

    sqlx::query(
        r#"
        INSERT INTO transactions (parent_dp_hash, tx_hash, version, transaction_type, transaction_status, block_hash, block_height, lane_id, index, identity)
        VALUES ($1, $2, 1, 'blob_transaction', 'success', $3, $4, '', 0, 'id')
        "#,
    )
    .bind(&parent_dp_hash)
    .bind(&tx_hash.0)
    .bind(&block_hash)
    .bind(height)
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO txs_contracts (parent_dp_hash, tx_hash, contract_name)
        VALUES ($1, $2, $3)
        "#,
    )
    .bind(&parent_dp_hash)
    .bind(&tx_hash.0)
    .bind(&contract.0)
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO blobs (parent_dp_hash, tx_hash, blob_index, identity, contract_name, data)
        VALUES ($1, $2, 0, 'id', $3, E'\\x01')
        "#,
    )
    .bind(&parent_dp_hash)
    .bind(&tx_hash.0)
    .bind(&contract.0)
    .execute(pool)
    .await?;

    Ok((tx_hash, block_hash))
}

async fn insert_sequenced_tx(
    pool: &PgPool,
    contract: &ContractName,
    height: i64,
) -> Result<(TxHash, BlockHash)> {
    let block_hash = hash(&format!("block-{height}"));
    let parent_hash = hash(&format!("block-{}", height - 1));
    insert_block(pool, &block_hash, &parent_hash, height).await?;

    let tx_hash = TxHash(hash(&format!("sequenced-{height}")));
    let parent_dp_hash = hash("dp-sequenced");

    sqlx::query(
        r#"
        INSERT INTO transactions (parent_dp_hash, tx_hash, version, transaction_type, transaction_status, block_hash, block_height, lane_id, index, identity)
        VALUES ($1, $2, 1, 'blob_transaction', 'sequenced', $3, $4, '', 0, 'id')
        "#,
    )
    .bind(&parent_dp_hash)
    .bind(&tx_hash.0)
    .bind(&block_hash)
    .bind(height)
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO blobs (parent_dp_hash, tx_hash, blob_index, identity, contract_name, data)
        VALUES ($1, $2, 0, 'id', $3, E'\\x02')
        "#,
    )
    .bind(&parent_dp_hash)
    .bind(&tx_hash.0)
    .bind(&contract.0)
    .execute(pool)
    .await?;

    Ok((tx_hash, ConsensusProposalHash(block_hash)))
}

async fn notify_contract(
    pool: &PgPool,
    contract: &ContractName,
    height: BlockHeight,
) -> Result<()> {
    let payload = serde_json::to_string(&height)?;
    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(&contract.0)
        .bind(payload)
        .execute(pool)
        .await?;
    Ok(())
}

#[test_log::test(tokio::test)]
async fn emits_new_tx_on_notifications() -> Result<()> {
    let PgTestCtx {
        pool,
        database_url,
        _container,
    } = setup_pg().await?;
    let contract = ContractName("contract_1".to_string());

    let bus = SharedMessageBus::new(BusMetrics::global("contract-listener-notify".into()));
    let mut receiver: tokio::sync::broadcast::Receiver<
        crate::bus::BusEnvelope<ContractListenerEvent>,
    > = get_receiver::<ContractListenerEvent>(&bus).await;
    let mut shutdown = ShutdownClient::new_from_bus(bus.new_handle()).await;

    let conf = ContractListenerConf {
        database_url: database_url.clone(),
        contracts: HashSet::from([contract.clone()]),
        poll_interval: Duration::from_secs(5),
    };

    let mut module = ContractListener::build(bus.new_handle(), conf).await?;
    let handle = tokio::spawn(async move { module.run().await });

    let (tx_hash, block_hash) = insert_sequenced_tx(&pool, &contract, 2).await?;
    notify_contract(&pool, &contract, BlockHeight(2)).await?;

    let ContractListenerEvent::NewTx(seen_hash, blobs, ctx, status) =
        drain_event(&mut receiver).await?;

    assert_eq!(seen_hash, tx_hash);
    assert_eq!(ctx.block_hash, block_hash);
    assert_eq!(ctx.block_height, BlockHeight(2));
    assert_eq!(status, TransactionStatusDb::Sequenced);
    assert_eq!(blobs.len(), 1);
    assert_eq!(blobs[0].0, BlobIndex(0));
    assert_eq!(blobs[0].1.contract_name, contract);

    shutdown
        .send(ShutdownModule {
            module: std::any::type_name::<ContractListener>().to_string(),
        })
        .expect("failed to send shutdown");
    let _ = timeout(Duration::from_secs(2), handle).await??;

    Ok(())
}

#[test_log::test(tokio::test)]
async fn dispatches_unprocessed_blocks_on_startup() -> Result<()> {
    let PgTestCtx {
        pool,
        database_url,
        _container,
    } = setup_pg().await?;
    let contract = ContractName("contract_1".to_string());
    let (settled_tx_hash, _) = insert_settled_tx(&pool, &contract, 1).await?;
    let (sequenced_tx_hash, _) = insert_sequenced_tx(&pool, &contract, 2).await?;

    let bus = SharedMessageBus::new(BusMetrics::global("contract-listener-dispatch".into()));
    let mut receiver = get_receiver::<ContractListenerEvent>(&bus).await;
    let mut shutdown = ShutdownClient::new_from_bus(bus.new_handle()).await;

    let conf = ContractListenerConf {
        database_url: database_url.clone(),
        contracts: HashSet::from([contract.clone()]),
        poll_interval: Duration::from_secs(5),
    };

    let mut module = ContractListener::build(bus.new_handle(), conf).await?;
    let handle = tokio::spawn(async move { module.run().await });

    // Settled tx
    let ContractListenerEvent::NewTx(seen_hash, blobs, ctx, status) =
        drain_event(&mut receiver).await?;

    assert_eq!(seen_hash, settled_tx_hash);
    assert_eq!(ctx.block_height, BlockHeight(1));
    assert_eq!(status, TransactionStatusDb::Success);
    assert_eq!(blobs.len(), 1);
    assert_eq!(blobs[0].0, BlobIndex(0));
    assert_eq!(blobs[0].1.contract_name, contract);

    // Sequenced tx
    let ContractListenerEvent::NewTx(seen_hash, blobs, ctx, status) =
        drain_event(&mut receiver).await?;

    assert_eq!(seen_hash, sequenced_tx_hash);
    assert_eq!(ctx.block_height, BlockHeight(2));
    assert_eq!(status, TransactionStatusDb::Sequenced);
    assert_eq!(blobs.len(), 1);
    assert_eq!(blobs[0].0, BlobIndex(0));
    assert_eq!(blobs[0].1.contract_name, contract);

    shutdown
        .send(ShutdownModule {
            module: std::any::type_name::<ContractListener>().to_string(),
        })
        .expect("failed to send shutdown");
    let _ = timeout(Duration::from_secs(2), handle).await??;

    Ok(())
}
