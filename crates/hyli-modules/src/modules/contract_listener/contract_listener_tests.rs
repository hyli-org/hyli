use std::{collections::HashSet, time::Duration};

use crate::{
    bus::{
        dont_use_this::get_receiver, metrics::BusMetrics, BusClientSender, BusEnvelope,
        SharedMessageBus,
    },
    modules::{
        contract_listener::{ContractListener, ContractListenerConf, ContractListenerEvent},
        indexer::MIGRATOR,
        signal::ShutdownModule,
        Module, ShutdownClient,
    },
};
use anyhow::Result;
use hyli_model::{api::TransactionStatusDb, BlobIndex, BlockHeight, ContractName, LaneId, TxHash};
use sdk::{BlockHash, ConsensusProposalHash};
use sqlx::{postgres::PgPoolOptions, types::chrono::Utc, PgPool};
use tempfile::tempdir;
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
    use sha3::{Digest, Sha3_256};

    let mut hasher = Sha3_256::new();
    hasher.update(value.as_bytes());
    hex::encode(hasher.finalize())
}

async fn insert_block(pool: &PgPool, hash: &str, parent_hash: &str, height: i64) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO blocks (hash, parent_hash, height, timestamp, total_txs)
        VALUES ($1, $2, $3, $4, 1)
        ON CONFLICT (hash) DO NOTHING
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

async fn expect_event(
    receiver: &mut Receiver<BusEnvelope<ContractListenerEvent>>,
    expected_hash: &TxHash,
    expected_height: BlockHeight,
    expected_status: TransactionStatusDb,
) -> Result<()> {
    let mut seen: Vec<String> = Vec::new();
    let wait = timeout(Duration::from_secs(2), async {
        loop {
            let event = drain_event(receiver).await?;
            match event {
                ContractListenerEvent::SequencedTx(seen_hash, _blobs, ctx) => {
                    if expected_status == TransactionStatusDb::Sequenced
                        && &seen_hash == expected_hash
                        && ctx.block_height == expected_height
                    {
                        return Ok(());
                    }
                    seen.push(format!("sequenced:{}:{}", seen_hash, ctx.block_height.0));
                }
                ContractListenerEvent::SettledTx(seen_hash, _blobs, ctx, status) => {
                    if &seen_hash == expected_hash
                        && ctx.block_height == expected_height
                        && status == expected_status
                    {
                        return Ok(());
                    }
                    seen.push(format!(
                        "settled:{}:{}:{:?}",
                        seen_hash, ctx.block_height.0, status
                    ));
                }
            }
        }
    })
    .await;

    match wait {
        Ok(result) => result,
        Err(_) => anyhow::bail!(
            "timeout waiting for {expected_hash:?} at {expected_height:?} with {expected_status:?}; last seen: {seen:?}"
        ),
    }
}

async fn insert_settled_tx(
    pool: &PgPool,
    contract: &ContractName,
    height: i64,
) -> Result<(TxHash, String)> {
    insert_settled_tx_with_index(pool, contract, height, 0).await
}

async fn insert_settled_tx_with_index(
    pool: &PgPool,
    contract: &ContractName,
    height: i64,
    index: i32,
) -> Result<(TxHash, String)> {
    let block_hash = hash(&format!("block-{height}"));
    let parent_hash = hash(&format!("block-{}", height - 1));
    insert_block(pool, &block_hash, &parent_hash, height).await?;

    let tx_hash = TxHash::from(hash(&format!("settled-{height}-{index}")));
    let tx_hash_hex = hex::encode(&tx_hash.0);
    let parent_dp_hash = hash("dp-settled");
    let lane_id = LaneId::default().to_string();

    sqlx::query(
        r#"
        INSERT INTO transactions (parent_dp_hash, tx_hash, version, transaction_type, transaction_status, block_hash, block_height, lane_id, index, identity)
        VALUES ($1, $2, 1, 'blob_transaction', 'success', $3, $4, $5, $6, 'id')
        "#,
    )
    .bind(&parent_dp_hash)
    .bind(&tx_hash_hex)
    .bind(&block_hash)
    .bind(height)
    .bind(&lane_id)
    .bind(index)
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO txs_contracts (parent_dp_hash, tx_hash, contract_name)
        VALUES ($1, $2, $3)
        "#,
    )
    .bind(&parent_dp_hash)
    .bind(&tx_hash_hex)
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
    .bind(&tx_hash_hex)
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
    insert_sequenced_tx_with_index(pool, contract, height, 0).await
}

async fn insert_sequenced_tx_with_index(
    pool: &PgPool,
    contract: &ContractName,
    height: i64,
    index: i32,
) -> Result<(TxHash, BlockHash)> {
    let block_hash = hash(&format!("block-{height}"));
    let parent_hash = hash(&format!("block-{}", height - 1));
    insert_block(pool, &block_hash, &parent_hash, height).await?;

    let tx_hash = TxHash::from(hash(&format!("sequenced-{height}-{index}")));
    let tx_hash_hex = hex::encode(&tx_hash.0);
    let parent_dp_hash = hash("dp-sequenced");
    let lane_id = LaneId::default().to_string();

    sqlx::query(
        r#"
        INSERT INTO transactions (parent_dp_hash, tx_hash, version, transaction_type, transaction_status, block_hash, block_height, lane_id, index, identity)
        VALUES ($1, $2, 1, 'blob_transaction', 'sequenced', $3, $4, $5, $6, 'id')
        "#,
    )
    .bind(&parent_dp_hash)
    .bind(&tx_hash_hex)
    .bind(&block_hash)
    .bind(height)
    .bind(&lane_id)
    .bind(index)
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO blobs (parent_dp_hash, tx_hash, blob_index, identity, contract_name, data)
        VALUES ($1, $2, 0, 'id', $3, E'\\x02')
        "#,
    )
    .bind(&parent_dp_hash)
    .bind(&tx_hash_hex)
    .bind(&contract.0)
    .execute(pool)
    .await?;

    Ok((tx_hash, ConsensusProposalHash::from(block_hash)))
}

async fn update_tx_status(
    pool: &PgPool,
    tx_hash: &TxHash,
    status: &str,
    height: i64,
) -> Result<()> {
    let block_hash = hash(&format!("block-{height}"));
    let parent_hash = hash(&format!("block-{}", height - 1));
    let tx_hash_hex = hex::encode(&tx_hash.0);
    insert_block(pool, &block_hash, &parent_hash, height).await?;

    sqlx::query(
        r#"
        UPDATE transactions
        SET transaction_status = $1::transaction_status, block_hash = $2, block_height = $3
        WHERE tx_hash = $4
        "#,
    )
    .bind(status)
    .bind(&block_hash)
    .bind(height)
    .bind(&tx_hash_hex)
    .execute(pool)
    .await?;

    Ok(())
}

async fn update_tx_status_in_place(pool: &PgPool, tx_hash: &TxHash, status: &str) -> Result<()> {
    let tx_hash_hex = hex::encode(&tx_hash.0);
    sqlx::query(
        r#"
        UPDATE transactions
        SET transaction_status = $1::transaction_status
        WHERE tx_hash = $2
        "#,
    )
    .bind(status)
    .bind(&tx_hash_hex)
    .execute(pool)
    .await?;

    Ok(())
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

async fn expect_no_event(
    receiver: &mut Receiver<BusEnvelope<ContractListenerEvent>>,
    duration: Duration,
) -> Result<()> {
    match timeout(duration, receiver.recv()).await {
        Ok(Ok(msg)) => {
            let event = msg.into_message();
            let label = match event {
                ContractListenerEvent::SequencedTx(_, _, _) => "sequenced",
                ContractListenerEvent::SettledTx(_, _, _, _) => "settled",
            };
            anyhow::bail!("unexpected {label} event");
        }
        Ok(Err(RecvError::Lagged(skipped))) => {
            anyhow::bail!("unexpected lagged by {skipped} events")
        }
        Ok(Err(RecvError::Closed)) => anyhow::bail!("receiver closed"),
        Err(_) => Ok(()),
    }
}

#[test_log::test(tokio::test)]
async fn emits_new_tx_on_notifications() -> Result<()> {
    // Scenario: a sequenced tx arrives and a notification triggers a sequenced event with correct context/blobs.
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

    let data_dir = tempdir()?;
    let conf = ContractListenerConf {
        database_url: database_url.clone(),
        data_directory: data_dir.path().to_path_buf(),
        contracts: HashSet::from([contract.clone()]),
        poll_interval: Duration::from_secs(5),
    };

    let mut module = ContractListener::build(bus.new_handle(), conf).await?;
    let handle = tokio::spawn(async move { module.run().await });

    let (tx_hash, block_hash) = insert_sequenced_tx(&pool, &contract, 2).await?;
    notify_contract(&pool, &contract, BlockHeight(2)).await?;

    let event = drain_event(&mut receiver).await?;
    let (seen_hash, blobs, ctx) = match event {
        ContractListenerEvent::SequencedTx(seen_hash, blobs, ctx) => (seen_hash, blobs, ctx),
        ContractListenerEvent::SettledTx(_, _, _, _) => {
            anyhow::bail!("unexpected settled event for sequenced notification")
        }
    };

    assert_eq!(seen_hash, tx_hash);
    assert_eq!(ctx.block_hash, block_hash);
    assert_eq!(ctx.block_height, BlockHeight(2));
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
    // Scenario: pre-existing sequenced and settled txs are dispatched once on startup.
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

    let data_dir = tempdir()?;
    let conf = ContractListenerConf {
        database_url: database_url.clone(),
        data_directory: data_dir.path().to_path_buf(),
        contracts: HashSet::from([contract.clone()]),
        poll_interval: Duration::from_secs(5),
    };

    let mut module = ContractListener::build(bus.new_handle(), conf).await?;
    let handle = tokio::spawn(async move { module.run().await });

    let events = vec![
        drain_event(&mut receiver).await?,
        drain_event(&mut receiver).await?,
    ];

    let mut saw_settled = false;
    let mut saw_sequenced = false;

    for event in events {
        match event {
            ContractListenerEvent::SequencedTx(seen_hash, blobs, ctx) => {
                assert_eq!(seen_hash, sequenced_tx_hash);
                assert_eq!(ctx.block_height, BlockHeight(2));
                assert_eq!(blobs.len(), 1);
                assert_eq!(blobs[0].0, BlobIndex(0));
                assert_eq!(blobs[0].1.contract_name, contract);
                saw_sequenced = true;
            }
            ContractListenerEvent::SettledTx(seen_hash, blobs, ctx, status) => {
                assert_eq!(status, TransactionStatusDb::Success);
                assert_eq!(seen_hash, settled_tx_hash);
                assert_eq!(ctx.block_height, BlockHeight(1));
                assert_eq!(blobs.len(), 1);
                assert_eq!(blobs[0].0, BlobIndex(0));
                assert_eq!(blobs[0].1.contract_name, contract);
                saw_settled = true;
            }
        }
    }

    assert!(saw_settled, "missing settled tx event on startup");
    assert!(saw_sequenced, "missing sequenced tx event on startup");

    shutdown
        .send(ShutdownModule {
            module: std::any::type_name::<ContractListener>().to_string(),
        })
        .expect("failed to send shutdown");
    let _ = timeout(Duration::from_secs(2), handle).await??;

    Ok(())
}

#[test_log::test(tokio::test)]
async fn emits_status_updates_for_same_tx_and_new_txs() -> Result<()> {
    // Scenario: a sequenced tx transitions to each terminal status and emits the expected events.
    let PgTestCtx {
        pool,
        database_url,
        _container,
    } = setup_pg().await?;
    let contract = ContractName("contract_1".to_string());

    let bus = SharedMessageBus::new(BusMetrics::global("contract-listener-status".into()));
    let mut receiver = get_receiver::<ContractListenerEvent>(&bus).await;
    let mut shutdown = ShutdownClient::new_from_bus(bus.new_handle()).await;

    let data_dir = tempdir()?;
    let conf = ContractListenerConf {
        database_url: database_url.clone(),
        data_directory: data_dir.path().to_path_buf(),
        contracts: HashSet::from([contract.clone()]),
        poll_interval: Duration::from_secs(5),
    };

    let mut module = ContractListener::build(bus.new_handle(), conf).await?;
    let handle = tokio::spawn(async move { module.run().await });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Sequenced -> Success.
    let (tx1_hash, _) = insert_sequenced_tx(&pool, &contract, 1).await?;
    notify_contract(&pool, &contract, BlockHeight(1)).await?;
    expect_event(
        &mut receiver,
        &tx1_hash,
        BlockHeight(1),
        TransactionStatusDb::Sequenced,
    )
    .await?;

    update_tx_status(&pool, &tx1_hash, "success", 2).await?;
    notify_contract(&pool, &contract, BlockHeight(2)).await?;
    expect_event(
        &mut receiver,
        &tx1_hash,
        BlockHeight(2),
        TransactionStatusDb::Success,
    )
    .await?;

    // Sequenced -> Failure.
    let (tx2_hash, _) = insert_sequenced_tx(&pool, &contract, 3).await?;
    notify_contract(&pool, &contract, BlockHeight(3)).await?;
    expect_event(
        &mut receiver,
        &tx2_hash,
        BlockHeight(3),
        TransactionStatusDb::Sequenced,
    )
    .await?;

    update_tx_status(&pool, &tx2_hash, "failure", 4).await?;
    notify_contract(&pool, &contract, BlockHeight(4)).await?;
    expect_event(
        &mut receiver,
        &tx2_hash,
        BlockHeight(4),
        TransactionStatusDb::Failure,
    )
    .await?;

    // Sequenced -> TimedOut.
    let (tx3_hash, _) = insert_sequenced_tx(&pool, &contract, 5).await?;
    notify_contract(&pool, &contract, BlockHeight(5)).await?;
    expect_event(
        &mut receiver,
        &tx3_hash,
        BlockHeight(5),
        TransactionStatusDb::Sequenced,
    )
    .await?;

    update_tx_status(&pool, &tx3_hash, "timed_out", 6).await?;
    notify_contract(&pool, &contract, BlockHeight(6)).await?;
    expect_event(
        &mut receiver,
        &tx3_hash,
        BlockHeight(6),
        TransactionStatusDb::TimedOut,
    )
    .await?;

    shutdown
        .send(ShutdownModule {
            module: std::any::type_name::<ContractListener>().to_string(),
        })
        .expect("failed to send shutdown");
    let _ = timeout(Duration::from_secs(2), handle).await??;

    Ok(())
}

#[test_log::test(tokio::test)]
async fn emits_sequenced_events_within_same_block_by_index() -> Result<()> {
    // Scenario: multiple sequenced txs in the same block are emitted in index order without duplicates.
    let PgTestCtx {
        pool,
        database_url,
        _container,
    } = setup_pg().await?;
    let contract = ContractName("contract_1".to_string());

    let bus = SharedMessageBus::new(BusMetrics::global(
        "contract-listener-sequenced-index".into(),
    ));
    let mut receiver = get_receiver::<ContractListenerEvent>(&bus).await;
    let mut shutdown = ShutdownClient::new_from_bus(bus.new_handle()).await;

    let data_dir = tempdir()?;
    let conf = ContractListenerConf {
        database_url: database_url.clone(),
        data_directory: data_dir.path().to_path_buf(),
        contracts: HashSet::from([contract.clone()]),
        poll_interval: Duration::from_secs(5),
    };

    let mut module = ContractListener::build(bus.new_handle(), conf).await?;
    let handle = tokio::spawn(async move { module.run().await });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let (tx1_hash, _) = insert_sequenced_tx_with_index(&pool, &contract, 7, 0).await?;
    notify_contract(&pool, &contract, BlockHeight(7)).await?;
    expect_event(
        &mut receiver,
        &tx1_hash,
        BlockHeight(7),
        TransactionStatusDb::Sequenced,
    )
    .await?;

    let (tx2_hash, _) = insert_sequenced_tx_with_index(&pool, &contract, 7, 1).await?;
    notify_contract(&pool, &contract, BlockHeight(7)).await?;
    expect_event(
        &mut receiver,
        &tx2_hash,
        BlockHeight(7),
        TransactionStatusDb::Sequenced,
    )
    .await?;
    expect_no_event(&mut receiver, Duration::from_millis(200)).await?;

    shutdown
        .send(ShutdownModule {
            module: std::any::type_name::<ContractListener>().to_string(),
        })
        .expect("failed to send shutdown");
    let _ = timeout(Duration::from_secs(2), handle).await??;

    Ok(())
}

#[test_log::test(tokio::test)]
async fn emits_settled_events_within_same_block_by_index() -> Result<()> {
    // Scenario: multiple settled txs in the same block are emitted in index order without duplicates.
    let PgTestCtx {
        pool,
        database_url,
        _container,
    } = setup_pg().await?;
    let contract = ContractName("contract_1".to_string());

    let bus = SharedMessageBus::new(BusMetrics::global("contract-listener-settled-index".into()));
    let mut receiver = get_receiver::<ContractListenerEvent>(&bus).await;
    let mut shutdown = ShutdownClient::new_from_bus(bus.new_handle()).await;

    let data_dir = tempdir()?;
    let conf = ContractListenerConf {
        database_url: database_url.clone(),
        data_directory: data_dir.path().to_path_buf(),
        contracts: HashSet::from([contract.clone()]),
        poll_interval: Duration::from_secs(5),
    };

    let mut module = ContractListener::build(bus.new_handle(), conf).await?;
    let handle = tokio::spawn(async move { module.run().await });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let (tx1_hash, _) = insert_settled_tx_with_index(&pool, &contract, 8, 0).await?;
    notify_contract(&pool, &contract, BlockHeight(8)).await?;
    expect_event(
        &mut receiver,
        &tx1_hash,
        BlockHeight(8),
        TransactionStatusDb::Success,
    )
    .await?;

    let (tx2_hash, _) = insert_settled_tx_with_index(&pool, &contract, 8, 1).await?;
    notify_contract(&pool, &contract, BlockHeight(8)).await?;
    expect_event(
        &mut receiver,
        &tx2_hash,
        BlockHeight(8),
        TransactionStatusDb::Success,
    )
    .await?;
    expect_no_event(&mut receiver, Duration::from_millis(200)).await?;

    shutdown
        .send(ShutdownModule {
            module: std::any::type_name::<ContractListener>().to_string(),
        })
        .expect("failed to send shutdown");
    let _ = timeout(Duration::from_secs(2), handle).await??;

    Ok(())
}

#[test_log::test(tokio::test)]
async fn emits_settled_update_below_last_seen_height() -> Result<()> {
    // Scenario: a settled status for a lower-height tx is still emitted after higher sequenced activity.
    let PgTestCtx {
        pool,
        database_url,
        _container,
    } = setup_pg().await?;
    let contract = ContractName("contract_1".to_string());

    let bus = SharedMessageBus::new(BusMetrics::global(
        "contract-listener-settled-backfill".into(),
    ));
    let mut receiver = get_receiver::<ContractListenerEvent>(&bus).await;
    let mut shutdown = ShutdownClient::new_from_bus(bus.new_handle()).await;

    let data_dir = tempdir()?;
    let conf = ContractListenerConf {
        database_url: database_url.clone(),
        data_directory: data_dir.path().to_path_buf(),
        contracts: HashSet::from([contract.clone()]),
        poll_interval: Duration::from_secs(5),
    };

    let mut module = ContractListener::build(bus.new_handle(), conf).await?;
    let handle = tokio::spawn(async move { module.run().await });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let (high_hash, _) = insert_sequenced_tx(&pool, &contract, 10).await?;
    notify_contract(&pool, &contract, BlockHeight(10)).await?;
    expect_event(
        &mut receiver,
        &high_hash,
        BlockHeight(10),
        TransactionStatusDb::Sequenced,
    )
    .await?;

    let (low_hash, _) = insert_sequenced_tx(&pool, &contract, 3).await?;
    update_tx_status_in_place(&pool, &low_hash, "success").await?;
    notify_contract(&pool, &contract, BlockHeight(11)).await?;
    expect_event(
        &mut receiver,
        &low_hash,
        BlockHeight(3),
        TransactionStatusDb::Success,
    )
    .await?;

    shutdown
        .send(ShutdownModule {
            module: std::any::type_name::<ContractListener>().to_string(),
        })
        .expect("failed to send shutdown");
    let _ = timeout(Duration::from_secs(2), handle).await??;

    Ok(())
}
