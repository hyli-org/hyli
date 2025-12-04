#![allow(clippy::all)]
#![cfg(feature = "turmoil")]
#![cfg(test)]

mod fixtures;

use std::time::Duration;

use client_sdk::rest_client::NodeApiClient;
use fixtures::turmoil::TurmoilHost;
use hyli_model::{
    BlobTransaction, ContractName, ProgramId, RegisterContractAction, StateCommitment,
};
use hyli_modules::log_error;
use hyli_net::net::Sim;
use rand::{rngs::StdRng, SeedableRng};
use tracing::warn;

use crate::fixtures::{test_helpers::wait_height, turmoil::TurmoilCtx};

pub fn make_register_contract_tx(name: ContractName) -> BlobTransaction {
    let register_contract_action = RegisterContractAction {
        verifier: "test".into(),
        program_id: ProgramId(vec![]),
        state_commitment: StateCommitment(vec![0, 1, 2, 3]),
        contract_name: name.clone(),
        constructor_metadata: Some(vec![1]),
        ..Default::default()
    };
    BlobTransaction::new(
        "hyli@hyli",
        vec![
            register_contract_action.as_blob("hyli".into()),
            register_contract_action.as_blob(name),
        ],
    )
}

fn assert_converged(
    ctx: &crate::fixtures::turmoil::TurmoilCtx,
    sim: &mut Sim<'_>,
    min_height: u64,
) -> anyhow::Result<()> {
    let ctx_clone = ctx.clone();

    sim.client("convergence", async move {
        ctx_clone
            .assert_cluster_converged(min_height)
            .await
            .expect("cluster converged");
        Ok(())
    });

    loop {
        let finished = sim.step().map_err(|s| anyhow::anyhow!(s.to_string()))?;
        if finished {
            break;
        }
    }

    Ok(())
}

macro_rules! turmoil_simple {
    ($seed:literal, $simulation:ident, $test:ident) => {
        paste::paste! {
        #[test_log::test]
            fn [<turmoil_ $simulation _ $seed _ $test>]() -> anyhow::Result<()> {
                tracing::info!("Starting test {} with seed {}", stringify!([<turmoil_ $simulation _ $seed _ $test>]), $seed);
                let rng = StdRng::seed_from_u64($seed);
                let mut sim = hyli_net::turmoil::Builder::new()
                    .simulation_duration(Duration::from_secs(180))
                    .tick_duration(Duration::from_millis(20))
                    .min_message_latency(Duration::from_millis(20))
                .tcp_capacity(256)
                .enable_tokio_io()
                    .build_with_rng(Box::new(rng));

                let mut ctx = TurmoilCtx::new_multi(4, 500, $seed, &mut sim)?;

                for node in ctx.nodes.iter() {
                    let cloned_node = node.clone();
                    sim.client(format!("client {}", node.conf.id.clone()), async move {
                        _ = $test(cloned_node).await?;
                        Ok(())
                    });
                }

                $simulation(&mut ctx, &mut sim)?;
                if stringify!($simulation) != "simulation_restart_node" {
                    assert_converged(&ctx, &mut sim, 1)?;
                }

                Ok(())
            }
        }
    };

    ($seed_from:literal..=$seed_to:literal, $simulation:ident, $test:ident) => {
        seq_macro::seq!(SEED in $seed_from..=$seed_to {
            turmoil_simple!(SEED, $simulation, $test);
        });
    };
}

turmoil_simple!(411..=420, simulation_basic, submit_10_contracts);
turmoil_simple!(511..=520, simulation_slow_node, submit_10_contracts);
turmoil_simple!(511..=520, simulation_two_slow_nodes, submit_10_contracts);
turmoil_simple!(511..=520, simulation_slow_network, submit_10_contracts);
turmoil_simple!(511..=520, simulation_hold, submit_10_contracts);
turmoil_simple!(611..=620, simulation_one_more_node, submit_10_contracts);
turmoil_simple!(621..=630, simulation_partition, submit_10_contracts);
turmoil_simple!(631..=640, simulation_drop_storm, submit_10_contracts);
turmoil_simple!(641..=650, simulation_restart_node, submit_10_contracts);
turmoil_simple!(651..=655, simulation_asymmetric_partition, submit_10_contracts);
turmoil_simple!(656..=660, simulation_long_isolation, submit_10_contracts);

/// Stress runner to try and systematically reproduce drop-storm desyncs.
/// Ignored by default because it can take time; run manually when hunting regressions.
#[test_log::test]
#[ignore]
fn turmoil_drop_storm_repro_runner() -> anyhow::Result<()> {
    // Fewer attempts to keep runtime reasonable, but with aggressive fault patterns.
    let attempts = 25;
    let mut seen_failure: Option<(u64, String)> = None;

    for run in 0..attempts {
        // Focus seeds around the historical failure range (630s) and vary them.
        let seed = 630 + run as u64 * 2 + 1;
        tracing::info!("drop-storm repro attempt {} seed {}", run + 1, seed);

        let rng = StdRng::seed_from_u64(seed);
        let mut sim = hyli_net::turmoil::Builder::new()
            .simulation_duration(Duration::from_secs(200))
            .tick_duration(Duration::from_millis(20))
            .min_message_latency(Duration::from_millis(20))
            .tcp_capacity(256)
            .enable_tokio_io()
            .build_with_rng(Box::new(rng));

        // Slightly faster slots to increase commit cadence under fault.
        let mut ctx = TurmoilCtx::new_multi(4, 400, seed, &mut sim)?;
        let leader_id = ctx.nodes.first().unwrap().conf.id.clone();

        for node in ctx.nodes.iter() {
            let cloned_node = node.clone();
            sim.client(format!("client {}", node.conf.id.clone()), async move {
                _ = submit_10_contracts(cloned_node).await?;
                Ok(())
            });
        }

        let res = (|| -> anyhow::Result<()> {
            // Hard mode: combine drop-storm + leader restart + post-heal long isolation.
            hard_drop_storm_scenario(&mut ctx, &mut sim, leader_id.clone())?;
            assert_converged(&ctx, &mut sim, 1)?;
            Ok(())
        })();

        if let Err(e) = res {
            seen_failure = Some((seed, format!("{e:?}")));
            break;
        }
    }

    if let Some((seed, msg)) = seen_failure {
        anyhow::bail!("drop-storm reproduced at seed {seed}: {msg}");
    }

    tracing::info!("No drop-storm desync reproduced after {} attempts", attempts);
    Ok(())
}

/// Hard-mode scenario to try and trigger desyncs:
/// 1) Run drop-storm with more aggressive holds.
/// 2) Mid-storm, isolate the leader (simulate restart) for a downtime.
/// 3) After healing, isolate a random node for a long catchup window.
fn hard_drop_storm_scenario(ctx: &mut TurmoilCtx, sim: &mut Sim<'_>, leader_id: String) -> anyhow::Result<()> {
    // More aggressive storm than the default: shorter interval, longer holds.
    let warmup = Duration::from_secs(5);
    let storm_duration = Duration::from_secs(30);
    let hold_interval = Duration::from_secs(1);
    let mut last_hold = Duration::from_secs(0);
    let mut healed = false;

    #[derive(Clone)]
    struct ActiveHold {
        from: String,
        to: String,
        release_at: Duration,
    }
    let mut holds: Vec<ActiveHold> = Vec::new();

    // Leader restart timing and downtime.
    let restart_at = warmup + Duration::from_secs(6);
    let restart_downtime = Duration::from_secs(12);
    let mut leader_offline_until: Option<Duration> = None;
    let mut leader_isolated = false;

    // Post-heal long isolation for catchup probing.
    let mut post_heal_isolated = false;
    let post_heal_target = ctx.random_id();
    let post_heal_isolation = Duration::from_secs(18);

    loop {
        let finished = sim.step().map_err(|s| anyhow::anyhow!(s.to_string()))?;
        let now = sim.elapsed();

        // Aggressive storm holds.
        if now > warmup
            && now < warmup + storm_duration
            && now.saturating_sub(last_hold) > hold_interval
        {
            let (from, to) = ctx.random_id_pair();
            let hold_len = Duration::from_secs(ctx.random_between(5, 10));
            sim.hold(from.clone(), to.clone());
            sim.hold(to.clone(), from.clone());
            holds.push(ActiveHold {
                from,
                to,
                release_at: now + hold_len,
            });
            last_hold = now;
        }

        // release expired holds
        let mut to_release = Vec::new();
        holds.retain(|h| {
            if now >= h.release_at {
                to_release.push((h.from.clone(), h.to.clone()));
                false
            } else {
                true
            }
        });
        for (from, to) in to_release {
            sim.release(from.clone(), to.clone());
            sim.release(to, from);
        }

        // Leader restart (isolate both directions for downtime)
        if !leader_isolated && now > restart_at {
            leader_isolated = true;
            for other in ctx
                .nodes
                .clone()
                .iter()
                .filter(|n| n.conf.id != leader_id)
            {
                sim.hold(leader_id.clone(), other.conf.id.clone());
                sim.hold(other.conf.id.clone(), leader_id.clone());
            }
            leader_offline_until = Some(now + restart_downtime);
        }
        if let Some(until) = leader_offline_until {
            if now >= until {
                leader_offline_until = None;
                for other in ctx
                    .nodes
                    .clone()
                    .iter()
                    .filter(|n| n.conf.id != leader_id)
                {
                    sim.release(leader_id.clone(), other.conf.id.clone());
                    sim.release(other.conf.id.clone(), leader_id.clone());
                }
            }
        }

        // Heal all links after the storm window
        if !healed && now > warmup + storm_duration {
            healed = true;
            for node in ctx.nodes.clone().iter() {
                for other in ctx
                    .nodes
                    .clone()
                    .iter()
                    .filter(|n| n.conf.id != node.conf.id)
                {
                    sim.release(node.conf.id.clone(), other.conf.id.clone());
                }
            }
        }

        // Post-heal: long isolation of a random node to force catchup/backfill.
        if healed && !post_heal_isolated && now > warmup + storm_duration + Duration::from_secs(3) {
            post_heal_isolated = true;
            for other in ctx
                .nodes
                .clone()
                .iter()
                .filter(|n| n.conf.id != post_heal_target)
            {
                sim.hold(post_heal_target.clone(), other.conf.id.clone());
                sim.hold(other.conf.id.clone(), post_heal_target.clone());
            }
        }
        if post_heal_isolated
            && now > warmup + storm_duration + Duration::from_secs(3) + post_heal_isolation
        {
            post_heal_isolated = false;
            for other in ctx
                .nodes
                .clone()
                .iter()
                .filter(|n| n.conf.id != post_heal_target)
            {
                sim.release(post_heal_target.clone(), other.conf.id.clone());
                sim.release(other.conf.id.clone(), post_heal_target.clone());
            }
        }

        // Stop when clients are done and we've passed the post-heal window.
        if finished && now > warmup + storm_duration + post_heal_isolation + Duration::from_secs(15) {
            return Ok(());
        }
    }
}

/// **Simulation**
///
/// Simulate a slow network (with fixed random latencies)
/// *realistic* -> min = 20, max = 500, lambda = 0.025
/// *slow*      -> min = 50, max = 1000, lambda = 0.01
pub fn simulation_realistic_network(ctx: &mut TurmoilCtx, sim: &mut Sim<'_>) -> anyhow::Result<()> {
    for node in ctx.nodes.clone().iter() {
        for other_node in ctx
            .nodes
            .clone()
            .iter()
            .filter(|n| n.conf.id != node.conf.id)
        {
            sim.set_link_max_message_latency(
                node.conf.id.clone(),
                other_node.conf.id.clone(),
                Duration::from_millis(500),
            );
        }
    }

    sim.set_message_latency_curve(0.01);

    loop {
        let is_finished = sim.step().map_err(|s| anyhow::anyhow!(s.to_string()))?;

        if is_finished {
            tracing::info!("Time spent {}", sim.elapsed().as_millis());
            return Ok(());
        }
    }
}

/// **Simulation**
///
/// Simulate a slow network (with fixed random latencies)
pub fn simulation_slow_network(ctx: &mut TurmoilCtx, sim: &mut Sim<'_>) -> anyhow::Result<()> {
    for node in ctx.nodes.clone().iter() {
        for other_node in ctx
            .nodes
            .clone()
            .iter()
            .filter(|n| n.conf.id != node.conf.id)
        {
            let slowness = Duration::from_millis(ctx.random_between(250, 500));
            sim.set_link_latency(node.conf.id.clone(), other_node.conf.id.clone(), slowness);
        }
    }

    loop {
        let is_finished = sim.step().map_err(|s| anyhow::anyhow!(s.to_string()))?;

        if is_finished {
            tracing::info!("Time spent {}", sim.elapsed().as_millis());
            return Ok(());
        }
    }
}

/// **Simulation**
///
/// Simulate 1 really slow node (with fixed random latencies)
pub fn simulation_slow_node(ctx: &mut TurmoilCtx, sim: &mut Sim<'_>) -> anyhow::Result<()> {
    let slow_node = ctx.random_id();

    for other_node in ctx.nodes.clone().iter().filter(|n| n.conf.id != slow_node) {
        let slowness = Duration::from_millis(ctx.random_between(150, 600));
        sim.set_link_latency(slow_node.clone(), other_node.conf.id.clone(), slowness);
    }

    loop {
        let is_finished = sim.step().map_err(|s| anyhow::anyhow!(s.to_string()))?;

        if is_finished {
            tracing::info!("Time spent {}", sim.elapsed().as_millis());
            return Ok(());
        }
    }
}

/// **Simulation**
///
/// Simulate 2 really slow nodes (with fixed random latencies)
pub fn simulation_two_slow_nodes(ctx: &mut TurmoilCtx, sim: &mut Sim<'_>) -> anyhow::Result<()> {
    let (slow_node, slow_node_2) = ctx.random_id_pair();

    for other_node in ctx.nodes.clone().iter().filter(|n| n.conf.id != slow_node) {
        let slowness = Duration::from_millis(ctx.random_between(150, 1500));
        sim.set_link_latency(slow_node.clone(), other_node.conf.id.clone(), slowness);
    }

    for other_node in ctx
        .nodes
        .clone()
        .iter()
        .filter(|n| n.conf.id != slow_node_2)
    {
        let slowness = Duration::from_millis(ctx.random_between(150, 1500));
        sim.set_link_latency(slow_node_2.clone(), other_node.conf.id.clone(), slowness);
    }

    loop {
        let is_finished = sim.step().map_err(|s| anyhow::anyhow!(s.to_string()))?;

        if is_finished {
            tracing::info!("Time spent {}", sim.elapsed().as_millis());
            return Ok(());
        }
    }
}

/// **Simulation**
///
/// Partition the cluster into two groups for a while, then heal and ensure the test can finish.
pub fn simulation_partition(ctx: &mut TurmoilCtx, sim: &mut Sim<'_>) -> anyhow::Result<()> {
    let mut nodes = ctx.nodes.clone();
    let mid = nodes.len() / 2;
    let group_a = nodes.drain(..mid).collect::<Vec<_>>();
    let group_b = nodes;

    // Give nodes a short warmup to reach the first height before cutting links.
    let warmup = Duration::from_secs(5);
    let partition_duration = Duration::from_secs(20);
    let mut partitioned = false;
    let mut healed = false;

    loop {
        let finished = sim.step().map_err(|s| anyhow::anyhow!(s.to_string()))?;
        let now = sim.elapsed();

        if !partitioned && now > warmup {
            partitioned = true;
            for a in group_a.iter() {
                for b in group_b.iter() {
                    sim.hold(a.conf.id.clone(), b.conf.id.clone());
                    sim.hold(b.conf.id.clone(), a.conf.id.clone());
                }
            }
        }

        if partitioned && !healed && now > warmup + partition_duration {
            healed = true;
            for a in group_a.iter() {
                for b in group_b.iter() {
                    sim.release(a.conf.id.clone(), b.conf.id.clone());
                    sim.release(b.conf.id.clone(), a.conf.id.clone());
                }
            }
        }

        if finished {
            tracing::info!("Time spent {}", sim.elapsed().as_millis());
            return Ok(());
        }
    }
}

/// **Simulation**
///
/// Cut only inbound traffic to a single node for a while to force asymmetric knowledge,
/// then heal and let it catch up.
pub fn simulation_asymmetric_partition(ctx: &mut TurmoilCtx, sim: &mut Sim<'_>) -> anyhow::Result<()> {
    let warmup = Duration::from_secs(5);
    let partition_duration = Duration::from_secs(15);
    let settle_time = Duration::from_secs(10);

    let target = ctx.random_id();
    let mut partitioned = false;
    let mut healed = false;

    loop {
        let _finished = sim.step().map_err(|s| anyhow::anyhow!(s.to_string()))?;
        let now = sim.elapsed();

        if !partitioned && now > warmup {
            partitioned = true;
            for other in ctx
                .nodes
                .clone()
                .iter()
                .filter(|n| n.conf.id != target)
            {
                // Block inbound messages so the target misses many blocks, but keep outbound traffic flowing (with jitter).
                sim.hold(other.conf.id.clone(), target.clone());
                sim.set_link_latency(
                    target.clone(),
                    other.conf.id.clone(),
                    Duration::from_millis(ctx.random_between(250, 800)),
                );
            }
        }

        if partitioned && !healed && now > warmup + partition_duration {
            healed = true;
            for other in ctx
                .nodes
                .clone()
                .iter()
                .filter(|n| n.conf.id != target)
            {
                sim.release(other.conf.id.clone(), target.clone());
                sim.set_link_latency(target.clone(), other.conf.id.clone(), Duration::from_millis(40));
            }
        }

        if healed && now > warmup + partition_duration + settle_time {
            tracing::info!(
                "Asymmetric partition healed after {} ms (target {})",
                sim.elapsed().as_millis(),
                target
            );
            return Ok(());
        }
    }
}

/// **Simulation**
///
/// Repeatedly cut random links for short bursts, then heal everything.
pub fn simulation_drop_storm(ctx: &mut TurmoilCtx, sim: &mut Sim<'_>) -> anyhow::Result<()> {
    let warmup = Duration::from_secs(5);
    let storm_duration = Duration::from_secs(20);
    let hold_interval = Duration::from_secs(3);
    let mut snapshot_done = false;
    let mut monitor_spawned = false;
    let settle_time = Duration::from_secs(5);

    #[derive(Clone)]
    struct ActiveHold {
        from: String,
        to: String,
        release_at: Duration,
    }

    let mut holds: Vec<ActiveHold> = Vec::new();
    let mut last_hold = Duration::from_secs(0);
    let mut healed = false;

    loop {
        let finished = sim.step().map_err(|s| anyhow::anyhow!(s.to_string()))?;
        let now = sim.elapsed();

        // schedule new hold
        if now > warmup
            && now < warmup + storm_duration
            && now.saturating_sub(last_hold) > hold_interval
        {
            let (from, to) = ctx.random_id_pair();
            let hold_len = Duration::from_secs(ctx.random_between(2, 4));
            sim.hold(from.clone(), to.clone());
            sim.hold(to.clone(), from.clone());
            holds.push(ActiveHold {
                from,
                to,
                release_at: now + hold_len,
            });
            last_hold = now;
        }

        // release expired holds
        let mut to_release = Vec::new();
        holds.retain(|h| {
            if now >= h.release_at {
                to_release.push((h.from.clone(), h.to.clone()));
                false
            } else {
                true
            }
        });
        for (from, to) in to_release {
            sim.release(from.clone(), to.clone());
            sim.release(to, from);
        }

        // heal all links after the storm window
        if !healed && now > warmup + storm_duration {
            healed = true;
            for node in ctx.nodes.clone().iter() {
                for other in ctx
                    .nodes
                    .clone()
                    .iter()
                    .filter(|n| n.conf.id != node.conf.id)
                {
                    sim.release(node.conf.id.clone(), other.conf.id.clone());
                }
            }

            // Capture a post-storm snapshot shortly after healing to help diagnose desyncs.
            if !snapshot_done {
                snapshot_done = true;
                let snapshot_ctx = ctx.clone();
                sim.client("post-storm-snapshot", async move {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    if let Ok(snaps) = snapshot_ctx.node_snapshots().await {
                        tracing::info!("post-storm snapshot: {:?}", snaps);
                    }
                    Ok(())
                });
            }

            if !monitor_spawned {
                monitor_spawned = true;
                let monitor_ctx = ctx.clone();
                sim.client("post-storm-monitor", async move {
                    for i in 0..15 {
                        if let Ok(snaps) = monitor_ctx.node_snapshots().await {
                            tracing::info!("post-storm monitor t={}s: {:?}", i, snaps);

                            let heights: Vec<u64> = snaps
                                .iter()
                                .filter_map(|s| s.split_whitespace().nth(1))
                                .filter_map(|h| h.strip_prefix("h="))
                                .filter_map(|h| h.parse::<u64>().ok())
                                .collect();

                            if heights.iter().min() == heights.iter().max() {
                                break;
                            }
                        }
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                    Ok(())
                });
            }
        }

        // Do not exit before the storm window + settle time is over, otherwise links might stay held.
        if finished && now > warmup + storm_duration + settle_time {
            tracing::info!(
                "Time spent {}ms; finished after storm window",
                sim.elapsed().as_millis()
            );
            return Ok(());
        }
    }
}

/// **Simulation**
///
/// Keep a node fully isolated long enough to build a backlog, then reconnect it and ensure it can catch up.
pub fn simulation_long_isolation(ctx: &mut TurmoilCtx, sim: &mut Sim<'_>) -> anyhow::Result<()> {
    let warmup = Duration::from_secs(6);
    let isolation_duration = Duration::from_secs(24);
    let settle_time = Duration::from_secs(20);
    let target = ctx.random_id();
    let mut isolated = false;
    let mut healed = false;

    loop {
        let _finished = sim.step().map_err(|s| anyhow::anyhow!(s.to_string()))?;
        let now = sim.elapsed();

        if !isolated && now > warmup {
            isolated = true;
            for other in ctx
                .nodes
                .clone()
                .iter()
                .filter(|n| n.conf.id != target)
            {
                sim.hold(target.clone(), other.conf.id.clone());
                sim.hold(other.conf.id.clone(), target.clone());
            }
        }

        if isolated && !healed && now > warmup + isolation_duration {
            healed = true;
            for other in ctx
                .nodes
                .clone()
                .iter()
                .filter(|n| n.conf.id != target)
            {
                sim.release(target.clone(), other.conf.id.clone());
                sim.release(other.conf.id.clone(), target.clone());
            }
        }

        if healed && now > warmup + isolation_duration + settle_time {
            tracing::info!(
                "Long isolation healed after {} ms (target {})",
                sim.elapsed().as_millis(),
                target
            );
            return Ok(());
        }
    }
}

/// **Simulation**
///
/// Periodically bounce a node to force reconnect/sync while the workload runs.
pub fn simulation_restart_node(ctx: &mut TurmoilCtx, sim: &mut Sim<'_>) -> anyhow::Result<()> {
    let warmup = Duration::from_secs(5);
    let downtime = Duration::from_secs(8);
    let settle_time = Duration::from_secs(20);

    let target = ctx.random_id();
    let mut last_cycle = Duration::from_secs(0);
    let mut offline_until: Option<Duration> = None;

    loop {
        let finished = sim.step().map_err(|s| anyhow::anyhow!(s.to_string()))?;
        let now = sim.elapsed();

        // Simulate a restart by isolating the node for a short downtime, then reconnecting.
        if offline_until.is_none() && now > warmup && now.saturating_sub(last_cycle) > downtime * 2 {
            for other in ctx
                .nodes
                .clone()
                .iter()
                .filter(|n| n.conf.id != target)
            {
                sim.hold(target.clone(), other.conf.id.clone());
                sim.hold(other.conf.id.clone(), target.clone());
            }
            offline_until = Some(now + downtime);
            last_cycle = now;
        }

        if let Some(until) = offline_until {
            if now >= until {
                for other in ctx
                    .nodes
                    .clone()
                    .iter()
                    .filter(|n| n.conf.id != target)
                {
                    sim.release(target.clone(), other.conf.id.clone());
                    sim.release(other.conf.id.clone(), target.clone());
                }
                offline_until = None;
            }
        }

        if finished && now > warmup + settle_time {
            tracing::info!(
                "Restart scenario finished after {} ms; target node {} isolated multiple times",
                sim.elapsed().as_millis(),
                target
            );
            return Ok(());
        }
    }
}

/// **Simulation**
///
/// Start holding message derivery between two peers at a random moment, for a random duration, and release them (no message loss).
pub fn simulation_hold(ctx: &mut TurmoilCtx, sim: &mut Sim<'_>) -> anyhow::Result<()> {
    let mut finished: bool;

    let mut hold_config = HoldConfiguration::random_from(ctx);

    tracing::info!(
        "Holding messages from {} to {} at {} for {} seconds",
        hold_config.from,
        hold_config.to,
        hold_config.when.as_secs(),
        hold_config.duration.as_secs()
    );

    loop {
        finished = sim.step().unwrap();

        _ = hold_config.execute(sim);

        if finished {
            tracing::info!("Time spent {}", sim.elapsed().as_millis());
            return Ok(());
        }
    }
}

/// **Simulation**
///
/// Add a new node to the network during simulation between 5 and 15 seconds after simulation starts
pub fn simulation_one_more_node(ctx: &mut TurmoilCtx, sim: &mut Sim<'_>) -> anyhow::Result<()> {
    let when = ctx.random_between(5, 15);

    let mut added_node = false;
    let mut finished: bool;

    loop {
        finished = sim.step().unwrap();

        let current_time = sim.elapsed();

        if current_time > Duration::from_secs(when) && !added_node {
            added_node = true;
            let client_with_retries = ctx.add_node_to_simulation(sim)?.retry_15times_1000ms();

            sim.client("client new-node", async move {
                _ = wait_height(&client_with_retries, 1).await;

                for i in 1..10 {
                    let contract = client_with_retries
                        .get_contract(format!("contract-{}", i).into())
                        .await?;
                    assert_eq!(contract.contract_name.0, format!("contract-{}", i).as_str());
                }
                Ok(())
            })
        }

        if finished {
            tracing::info!("Time spent {}", sim.elapsed().as_millis());
            return Ok(());
        }
    }
}

/// **Simulation**
///
/// Unroll tick steps until clients finish
pub fn simulation_basic(_ctx: &mut TurmoilCtx, sim: &mut Sim<'_>) -> anyhow::Result<()> {
    loop {
        let is_finished = sim.step().map_err(|s| anyhow::anyhow!(s.to_string()))?;

        if is_finished {
            tracing::info!("Time spent {}", sim.elapsed().as_millis());
            return Ok(());
        }
    }
}

/// **Test**
///
/// Inject 10 contracts on node-1.
/// Check on the node (all of them) that all 10 contracts are here.
pub async fn submit_10_contracts(node: TurmoilHost) -> anyhow::Result<()> {
    let client_with_retries = node.client.retry_15times_1000ms();

    _ = wait_height(&client_with_retries, 1).await;

    if node.conf.id == "node-1" {
        for i in 1..10 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let tx = make_register_contract_tx(format!("contract-{}", i).into());

            _ = log_error!(
                client_with_retries.send_tx_blob(tx).await,
                "Sending tx blob"
            );
        }
    } else {
        tokio::time::sleep(Duration::from_secs(10)).await;
    }

    for i in 1..10 {
        let name = format!("contract-{}", i);
        let mut attempts = 0;
        loop {
            match client_with_retries.get_contract(name.clone().into()).await {
                Ok(contract) => {
                    assert_eq!(contract.contract_name.0, name.as_str());
                    break;
                }
                Err(e) => {
                    attempts += 1;
                    if attempts > 40 {
                        return Err(e);
                    }
                    warn!("Retrying get_contract {} attempt {}: {}", name, attempts, e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    Ok(())
}

struct HoldConfiguration {
    pub from: String,
    pub to: String,
    pub when: Duration,
    pub duration: Duration,
    pub triggered: bool,
}

impl HoldConfiguration {
    pub fn random_from(ctx: &mut TurmoilCtx) -> Self {
        let (from, to) = ctx.random_id_pair();

        let when = Duration::from_secs(ctx.random_between(5, 15));
        let duration = Duration::from_secs(ctx.random_between(2, 10));

        tracing::info!(
            "Creating hold configuration from {} to {}",
            when.as_secs(),
            (when + duration).as_secs()
        );

        HoldConfiguration {
            from,
            to,
            when,
            duration,
            triggered: false,
        }
    }
    pub fn execute(&mut self, sim: &mut Sim<'_>) -> anyhow::Result<()> {
        let current_time = sim.elapsed();

        if current_time > self.when && current_time <= self.when + self.duration && !self.triggered
        {
            tracing::error!("HOLD TRIGGERED from {} to {}", self.from, self.to);
            sim.hold(self.from.clone(), self.to.clone());
            self.triggered = true;
        }

        if current_time > self.when + self.duration && self.triggered {
            tracing::error!("RELEASE TRIGGERED from {} to {}", self.from, self.to);
            sim.release(self.from.clone(), self.to.clone());
        }

        Ok(())
    }
}
