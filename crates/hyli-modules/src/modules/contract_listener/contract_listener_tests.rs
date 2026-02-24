use std::{collections::HashSet, time::Duration};

use crate::{
    bus::{dont_use_this::get_receiver, BusClientSender, BusEnvelope, SharedMessageBus},
    modules::{
        contract_listener::{ContractListener, ContractListenerConf, ContractListenerEvent},
        indexer::MIGRATOR,
        signal::ShutdownModule,
        Module, ShutdownClient,
    },
};
use anyhow::Result;
use hyli_model::{
    api::{ContractChangeType, TransactionStatusDb},
    BlockHeight, ContractName, LaneId, TxHash,
};
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
            Ok(msg) => {
                let event = msg.into_message();
                if matches!(event, ContractListenerEvent::BackfillComplete(_)) {
                    continue;
                }
                return Ok(event);
            }
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
                ContractListenerEvent::SequencedTx(tx_data) => {
                    let seen_hash = &tx_data.tx_id.1;
                    if expected_status == TransactionStatusDb::Sequenced
                        && seen_hash == expected_hash
                        && tx_data.tx_ctx.block_height == expected_height
                    {
                        return Ok(());
                    }
                    seen.push(format!(
                        "sequenced:{}:{}",
                        seen_hash, tx_data.tx_ctx.block_height.0
                    ));
                }
                ContractListenerEvent::SettledTx(tx_data) => {
                    let seen_hash = &tx_data.tx_id.1;
                    if seen_hash == expected_hash
                        && tx_data.tx_ctx.block_height == expected_height
                        && tx_data.status == expected_status
                    {
                        return Ok(());
                    }
                    seen.push(format!(
                        "settled:{}:{}:{:?}",
                        seen_hash, tx_data.tx_ctx.block_height.0, tx_data.status
                    ));
                }
                ContractListenerEvent::BackfillComplete(_) => {}
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

    let tx_hash =
        TxHash::from_hex(&hash(&format!("settled-{height}-{index}"))).expect("tx_hash hex");
    let tx_hash_hex = tx_hash.to_string();
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

async fn insert_program_id_update_history_for_tx(
    pool: &PgPool,
    contract: &ContractName,
    tx_hash: &TxHash,
    block_height: i64,
    tx_index: i32,
    program_id: Vec<u8>,
) -> Result<()> {
    let parent_dp_hash = hash("dp-settled");
    sqlx::query(
        r#"
        INSERT INTO contract_history (
            contract_name, block_height, tx_index, change_type,
            verifier, program_id, state_commitment, soft_timeout, hard_timeout,
            deleted_at_height, parent_dp_hash, tx_hash
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, NULL, NULL, $8, $9)
        "#,
    )
    .bind(&contract.0)
    .bind(block_height)
    .bind(tx_index)
    .bind(vec![ContractChangeType::ProgramIdUpdated])
    .bind("verifier_1")
    .bind(program_id)
    .bind(vec![0xAAu8, 0xBBu8, 0xCCu8])
    .bind(parent_dp_hash)
    .bind(tx_hash.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_blob_for_existing_settled_tx(
    pool: &PgPool,
    contract: &ContractName,
    tx_hash: &TxHash,
    blob_index: i32,
) -> Result<()> {
    let parent_dp_hash = hash("dp-settled");
    let tx_hash_hex = tx_hash.to_string();
    sqlx::query(
        r#"
        INSERT INTO blobs (parent_dp_hash, tx_hash, blob_index, identity, contract_name, data)
        VALUES ($1, $2, $3, 'id', $4, E'\\x03')
        "#,
    )
    .bind(&parent_dp_hash)
    .bind(&tx_hash_hex)
    .bind(blob_index)
    .bind(&contract.0)
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO txs_contracts (parent_dp_hash, tx_hash, contract_name)
        VALUES ($1, $2, $3)
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(&parent_dp_hash)
    .bind(&tx_hash_hex)
    .bind(&contract.0)
    .execute(pool)
    .await?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn insert_contract_registration_history_for_tx(
    pool: &PgPool,
    contract: &ContractName,
    tx_hash: &TxHash,
    block_height: i64,
    tx_index: i32,
    verifier: &str,
    program_id: Vec<u8>,
    state_commitment: Vec<u8>,
    soft_timeout: Option<i64>,
    hard_timeout: Option<i64>,
    metadata: Option<Vec<u8>>,
) -> Result<()> {
    let parent_dp_hash = hash("dp-settled");
    sqlx::query(
        r#"
        INSERT INTO contract_history (
            contract_name, block_height, tx_index, change_type,
            metadata, verifier, program_id, state_commitment, soft_timeout, hard_timeout,
            deleted_at_height, parent_dp_hash, tx_hash
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NULL, $11, $12)
        "#,
    )
    .bind(&contract.0)
    .bind(block_height)
    .bind(tx_index)
    .bind(vec![ContractChangeType::Registered])
    .bind(metadata)
    .bind(verifier)
    .bind(program_id)
    .bind(state_commitment)
    .bind(soft_timeout)
    .bind(hard_timeout)
    .bind(parent_dp_hash)
    .bind(tx_hash.to_string())
    .execute(pool)
    .await?;
    Ok(())
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

    let tx_hash =
        TxHash::from_hex(&hash(&format!("sequenced-{height}-{index}"))).expect("tx_hash hex");
    let tx_hash_hex = tx_hash.to_string();
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

    Ok((
        tx_hash,
        ConsensusProposalHash::from_hex(&block_hash).expect("block_hash hex"),
    ))
}

async fn insert_sequenced_tx_with_null_tx_identity(
    pool: &PgPool,
    contract: &ContractName,
    height: i64,
) -> Result<(TxHash, BlockHash)> {
    let block_hash = hash(&format!("block-{height}"));
    let parent_hash = hash(&format!("block-{}", height - 1));
    insert_block(pool, &block_hash, &parent_hash, height).await?;

    let tx_hash =
        TxHash::from_hex(&hash(&format!("sequenced-null-id-{height}"))).expect("tx_hash hex");
    let tx_hash_hex = tx_hash.to_string();
    let parent_dp_hash = hash("dp-sequenced-null-id");
    let lane_id = LaneId::default().to_string();

    sqlx::query(
        r#"
        INSERT INTO transactions (parent_dp_hash, tx_hash, version, transaction_type, transaction_status, block_hash, block_height, lane_id, index, identity)
        VALUES ($1, $2, 1, 'blob_transaction', 'sequenced', $3, $4, $5, 0, NULL)
        "#,
    )
    .bind(&parent_dp_hash)
    .bind(&tx_hash_hex)
    .bind(&block_hash)
    .bind(height)
    .bind(&lane_id)
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

    Ok((
        tx_hash,
        ConsensusProposalHash::from_hex(&block_hash).expect("block_hash hex"),
    ))
}

async fn update_tx_status(
    pool: &PgPool,
    tx_hash: &TxHash,
    status: &str,
    height: i64,
) -> Result<()> {
    let block_hash = hash(&format!("block-{height}"));
    let parent_hash = hash(&format!("block-{}", height - 1));
    let tx_hash_hex = tx_hash.to_string();
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
    let tx_hash_hex = tx_hash.to_string();
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
    let deadline = tokio::time::Instant::now() + duration;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Ok(());
        }
        match timeout(remaining, receiver.recv()).await {
            Ok(Ok(msg)) => {
                let event = msg.into_message();
                match event {
                    ContractListenerEvent::BackfillComplete(_) => {
                        continue;
                    }
                    ContractListenerEvent::SequencedTx(_) => {
                        anyhow::bail!("unexpected sequenced event")
                    }
                    ContractListenerEvent::SettledTx(_) => {
                        anyhow::bail!("unexpected settled event")
                    }
                }
            }
            Ok(Err(RecvError::Lagged(skipped))) => {
                anyhow::bail!("unexpected lagged by {skipped} events")
            }
            Ok(Err(RecvError::Closed)) => anyhow::bail!("receiver closed"),
            Err(_) => return Ok(()),
        }
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

    let bus = SharedMessageBus::new();
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
        replay_settled_from_start: false,
    };

    let mut module = ContractListener::build(bus.new_handle(), conf).await?;
    let handle = tokio::spawn(async move { module.run().await });

    let (tx_hash, block_hash) = insert_sequenced_tx(&pool, &contract, 2).await?;
    notify_contract(&pool, &contract, BlockHeight(2)).await?;

    let event = drain_event(&mut receiver).await?;
    let (seen_hash, blobs, ctx) = match event {
        ContractListenerEvent::SequencedTx(tx_data) => {
            (tx_data.tx_id.1, tx_data.tx.blobs.clone(), tx_data.tx_ctx)
        }
        ContractListenerEvent::SettledTx(_) => {
            anyhow::bail!("unexpected settled event for sequenced notification")
        }
        ContractListenerEvent::BackfillComplete(_) => {
            anyhow::bail!("unexpected backfill complete event for sequenced notification")
        }
    };

    assert_eq!(seen_hash, tx_hash);
    assert_eq!(ctx.block_hash, block_hash);
    assert_eq!(ctx.block_height, BlockHeight(2));
    assert_eq!(blobs.len(), 1);
    assert_eq!(blobs[0].contract_name, contract);

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

    let bus = SharedMessageBus::new();
    let mut receiver = get_receiver::<ContractListenerEvent>(&bus).await;
    let mut shutdown = ShutdownClient::new_from_bus(bus.new_handle()).await;

    let data_dir = tempdir()?;
    let conf = ContractListenerConf {
        database_url: database_url.clone(),
        data_directory: data_dir.path().to_path_buf(),
        contracts: HashSet::from([contract.clone()]),
        poll_interval: Duration::from_secs(5),
        replay_settled_from_start: false,
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
            ContractListenerEvent::SequencedTx(tx_data) => {
                assert_eq!(tx_data.tx_id.1, sequenced_tx_hash);
                assert_eq!(tx_data.tx_ctx.block_height, BlockHeight(2));
                assert_eq!(tx_data.tx.blobs.len(), 1);
                assert_eq!(tx_data.tx.blobs[0].contract_name, contract);
                saw_sequenced = true;
            }
            ContractListenerEvent::SettledTx(tx_data) => {
                assert_eq!(tx_data.status, TransactionStatusDb::Success);
                assert_eq!(tx_data.tx_id.1, settled_tx_hash);
                assert_eq!(tx_data.tx_ctx.block_height, BlockHeight(1));
                assert_eq!(tx_data.tx.blobs.len(), 1);
                assert_eq!(tx_data.tx.blobs[0].contract_name, contract);
                saw_settled = true;
            }
            ContractListenerEvent::BackfillComplete(_) => {}
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

    let bus = SharedMessageBus::new();
    let mut receiver = get_receiver::<ContractListenerEvent>(&bus).await;
    let mut shutdown = ShutdownClient::new_from_bus(bus.new_handle()).await;

    let data_dir = tempdir()?;
    let conf = ContractListenerConf {
        database_url: database_url.clone(),
        data_directory: data_dir.path().to_path_buf(),
        contracts: HashSet::from([contract.clone()]),
        poll_interval: Duration::from_secs(5),
        replay_settled_from_start: false,
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

    let bus = SharedMessageBus::new();
    let mut receiver = get_receiver::<ContractListenerEvent>(&bus).await;
    let mut shutdown = ShutdownClient::new_from_bus(bus.new_handle()).await;

    let data_dir = tempdir()?;
    let conf = ContractListenerConf {
        database_url: database_url.clone(),
        data_directory: data_dir.path().to_path_buf(),
        contracts: HashSet::from([contract.clone()]),
        poll_interval: Duration::from_secs(5),
        replay_settled_from_start: false,
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
async fn emits_sequenced_event_when_tx_identity_is_null() -> Result<()> {
    // Scenario: transactions.identity may be NULL; listener should rebuild identity from blobs.
    let PgTestCtx {
        pool,
        database_url,
        _container,
    } = setup_pg().await?;
    let contract = ContractName("contract_1".to_string());

    let bus = SharedMessageBus::new();
    let mut receiver = get_receiver::<ContractListenerEvent>(&bus).await;
    let mut shutdown = ShutdownClient::new_from_bus(bus.new_handle()).await;

    let data_dir = tempdir()?;
    let conf = ContractListenerConf {
        database_url: database_url.clone(),
        data_directory: data_dir.path().to_path_buf(),
        contracts: HashSet::from([contract.clone()]),
        poll_interval: Duration::from_secs(5),
        replay_settled_from_start: false,
    };

    let mut module = ContractListener::build(bus.new_handle(), conf).await?;
    let handle = tokio::spawn(async move { module.run().await });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let (tx_hash, _) = insert_sequenced_tx_with_null_tx_identity(&pool, &contract, 12).await?;
    notify_contract(&pool, &contract, BlockHeight(12)).await?;
    expect_event(
        &mut receiver,
        &tx_hash,
        BlockHeight(12),
        TransactionStatusDb::Sequenced,
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
async fn emits_settled_events_within_same_block_by_index() -> Result<()> {
    // Scenario: multiple settled txs in the same block are emitted in index order without duplicates.
    let PgTestCtx {
        pool,
        database_url,
        _container,
    } = setup_pg().await?;
    let contract = ContractName("contract_1".to_string());

    let bus = SharedMessageBus::new();
    let mut receiver = get_receiver::<ContractListenerEvent>(&bus).await;
    let mut shutdown = ShutdownClient::new_from_bus(bus.new_handle()).await;

    let data_dir = tempdir()?;
    let conf = ContractListenerConf {
        database_url: database_url.clone(),
        data_directory: data_dir.path().to_path_buf(),
        contracts: HashSet::from([contract.clone()]),
        poll_interval: Duration::from_secs(5),
        replay_settled_from_start: false,
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

    let bus = SharedMessageBus::new();
    let mut receiver = get_receiver::<ContractListenerEvent>(&bus).await;
    let mut shutdown = ShutdownClient::new_from_bus(bus.new_handle()).await;

    let data_dir = tempdir()?;
    let conf = ContractListenerConf {
        database_url: database_url.clone(),
        data_directory: data_dir.path().to_path_buf(),
        contracts: HashSet::from([contract.clone()]),
        poll_interval: Duration::from_secs(5),
        replay_settled_from_start: false,
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

#[test_log::test(tokio::test)]
async fn settled_event_contains_program_id_update_from_contract_history() -> Result<()> {
    let PgTestCtx {
        pool,
        database_url,
        _container,
    } = setup_pg().await?;
    let contract = ContractName("contract_1".to_string());

    let bus = SharedMessageBus::new();
    let mut receiver = get_receiver::<ContractListenerEvent>(&bus).await;
    let mut shutdown = ShutdownClient::new_from_bus(bus.new_handle()).await;

    let data_dir = tempdir()?;
    let conf = ContractListenerConf {
        database_url: database_url.clone(),
        data_directory: data_dir.path().to_path_buf(),
        contracts: HashSet::from([contract.clone()]),
        poll_interval: Duration::from_secs(5),
        replay_settled_from_start: false,
    };

    let mut module = ContractListener::build(bus.new_handle(), conf).await?;
    let handle = tokio::spawn(async move { module.run().await });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let (tx_hash, _) = insert_settled_tx_with_index(&pool, &contract, 20, 0).await?;
    let expected_program_id = vec![0x10, 0x20, 0x30, 0x40];
    insert_program_id_update_history_for_tx(
        &pool,
        &contract,
        &tx_hash,
        20,
        0,
        expected_program_id.clone(),
    )
    .await?;

    notify_contract(&pool, &contract, BlockHeight(20)).await?;

    let event = drain_event(&mut receiver).await?;
    match event {
        ContractListenerEvent::SettledTx(tx_data) => {
            assert_eq!(tx_data.tx_id.1, tx_hash);
            assert_eq!(tx_data.tx_ctx.block_height, BlockHeight(20));
            assert_eq!(tx_data.status, TransactionStatusDb::Success);

            let change = tx_data
                .contract_changes
                .get(&contract)
                .cloned()
                .expect("expected contract_changes on settled tx");
            assert_eq!(
                change.change_types,
                vec![ContractChangeType::ProgramIdUpdated]
            );
            assert_eq!(change.program_id, expected_program_id);
        }
        ContractListenerEvent::SequencedTx(_) => {
            anyhow::bail!("unexpected sequenced event")
        }
        ContractListenerEvent::BackfillComplete(_) => {
            anyhow::bail!("unexpected backfill complete event")
        }
    }

    shutdown
        .send(ShutdownModule {
            module: std::any::type_name::<ContractListener>().to_string(),
        })
        .expect("failed to send shutdown");
    let _ = timeout(Duration::from_secs(2), handle).await??;

    Ok(())
}

#[test_log::test(tokio::test)]
async fn settled_event_contains_contract_registration_from_contract_history() -> Result<()> {
    let PgTestCtx {
        pool,
        database_url,
        _container,
    } = setup_pg().await?;
    let registered_contract = ContractName("contract_1".to_string());
    let hyli_contract = ContractName("hyli".to_string());

    let bus = SharedMessageBus::new();
    let mut receiver = get_receiver::<ContractListenerEvent>(&bus).await;
    let mut shutdown = ShutdownClient::new_from_bus(bus.new_handle()).await;

    let data_dir = tempdir()?;
    let conf = ContractListenerConf {
        database_url: database_url.clone(),
        data_directory: data_dir.path().to_path_buf(),
        contracts: HashSet::from([registered_contract.clone()]),
        poll_interval: Duration::from_secs(5),
        replay_settled_from_start: false,
    };

    let mut module = ContractListener::build(bus.new_handle(), conf).await?;
    let handle = tokio::spawn(async move { module.run().await });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Contract registration txs are blobs on `hyli`, while history is keyed by the registered contract.
    let (tx_hash, _) = insert_settled_tx_with_index(&pool, &hyli_contract, 21, 0).await?;
    let expected_verifier = "test_verifier";
    let expected_program_id = vec![0xAB, 0xCD, 0xEF];
    let expected_state_commitment = vec![0x01, 0x02, 0x03, 0x04];
    let expected_soft_timeout = Some(123);
    let expected_hard_timeout = Some(456);
    let expected_metadata = vec![0x42, 0x43];
    insert_contract_registration_history_for_tx(
        &pool,
        &registered_contract,
        &tx_hash,
        21,
        0,
        expected_verifier,
        expected_program_id.clone(),
        expected_state_commitment.clone(),
        expected_soft_timeout,
        expected_hard_timeout,
        Some(expected_metadata.clone()),
    )
    .await?;

    notify_contract(&pool, &registered_contract, BlockHeight(21)).await?;

    let event = drain_event(&mut receiver).await?;
    match event {
        ContractListenerEvent::SettledTx(tx_data) => {
            assert_eq!(tx_data.tx_id.1, tx_hash);
            assert_eq!(tx_data.tx_ctx.block_height, BlockHeight(21));
            assert_eq!(tx_data.status, TransactionStatusDb::Success);
            assert_eq!(tx_data.tx.blobs[0].contract_name, hyli_contract);

            let change = tx_data
                .contract_changes
                .get(&registered_contract)
                .cloned()
                .expect("expected contract_changes on settled tx");
            assert_eq!(change.change_types, vec![ContractChangeType::Registered]);
            assert_eq!(change.verifier, expected_verifier);
            assert_eq!(change.program_id, expected_program_id);
            assert_eq!(change.state_commitment, expected_state_commitment);
            assert_eq!(change.soft_timeout, expected_soft_timeout);
            assert_eq!(change.hard_timeout, expected_hard_timeout);
            assert_eq!(change.deleted_at_height, None);
            assert_eq!(change.metadata, Some(expected_metadata));
        }
        ContractListenerEvent::SequencedTx(_) => {
            anyhow::bail!("unexpected sequenced event")
        }
        ContractListenerEvent::BackfillComplete(_) => {
            anyhow::bail!("unexpected backfill complete event")
        }
    }

    shutdown
        .send(ShutdownModule {
            module: std::any::type_name::<ContractListener>().to_string(),
        })
        .expect("failed to send shutdown");
    let _ = timeout(Duration::from_secs(2), handle).await??;

    Ok(())
}

#[test_log::test(tokio::test)]
async fn settled_tx_with_multiple_contract_updates_emits_once_with_all_changes() -> Result<()> {
    let PgTestCtx {
        pool,
        database_url,
        _container,
    } = setup_pg().await?;
    let contract_a = ContractName("contract_a".to_string());
    let contract_b = ContractName("contract_b".to_string());

    let bus = SharedMessageBus::new();
    let mut receiver = get_receiver::<ContractListenerEvent>(&bus).await;
    let mut shutdown = ShutdownClient::new_from_bus(bus.new_handle()).await;

    let data_dir = tempdir()?;
    let conf = ContractListenerConf {
        database_url: database_url.clone(),
        data_directory: data_dir.path().to_path_buf(),
        contracts: HashSet::from([contract_a.clone(), contract_b.clone()]),
        poll_interval: Duration::from_secs(5),
        replay_settled_from_start: false,
    };

    let mut module = ContractListener::build(bus.new_handle(), conf).await?;
    let handle = tokio::spawn(async move { module.run().await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (tx_hash, _) = insert_settled_tx_with_index(&pool, &contract_a, 30, 0).await?;
    insert_blob_for_existing_settled_tx(&pool, &contract_b, &tx_hash, 1).await?;
    insert_program_id_update_history_for_tx(&pool, &contract_a, &tx_hash, 30, 0, vec![1, 2, 3])
        .await?;
    insert_program_id_update_history_for_tx(&pool, &contract_b, &tx_hash, 30, 0, vec![4, 5, 6])
        .await?;

    notify_contract(&pool, &contract_a, BlockHeight(30)).await?;
    let event = drain_event(&mut receiver).await?;
    let tx_data = match event {
        ContractListenerEvent::SettledTx(tx_data) => tx_data,
        ContractListenerEvent::SequencedTx(_) => anyhow::bail!("unexpected sequenced event"),
        ContractListenerEvent::BackfillComplete(_) => {
            anyhow::bail!("unexpected backfill complete event")
        }
    };
    assert_eq!(tx_data.tx_id.1, tx_hash);
    assert_eq!(tx_data.contract_changes.len(), 2);
    assert!(tx_data.contract_changes.contains_key(&contract_a));
    assert!(tx_data.contract_changes.contains_key(&contract_b));

    notify_contract(&pool, &contract_b, BlockHeight(30)).await?;
    expect_no_event(&mut receiver, Duration::from_millis(400)).await?;

    shutdown
        .send(ShutdownModule {
            module: std::any::type_name::<ContractListener>().to_string(),
        })
        .expect("failed to send shutdown");
    let _ = timeout(Duration::from_secs(2), handle).await??;

    Ok(())
}
