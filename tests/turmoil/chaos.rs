//! Chaos-style simulations using the Turmoil network harness.
//!
//! These scenarios mirror the E2E chaos tests with deterministic schedules
//! for reproducibility and fine-grained control.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use client_sdk::rest_client::NodeApiClient;
use hyli::bus::{BusClientSender, bus_client};
use hyli::rest::RestApi;
use hyli_modules::modules::signal::ShutdownModule;
use hyli_net::net::Sim;
use rand::seq::SliceRandom;
use tokio::sync::Mutex;

use crate::fixtures::turmoil::{
    TurmoilCtx, hold_all_links, hold_node, release_all_links, release_node,
};

bus_client! {
    struct ShutdownBusClient {
        sender(ShutdownModule),
    }
}

/// **Simulation**
///
/// Full network outage: after a warmup period, partition all nodes from each
/// other for a randomized outage window, then restore full connectivity.
pub fn simulation_chaos_full_outage(ctx: &mut TurmoilCtx, sim: &mut Sim<'_>) -> anyhow::Result<()> {
    let warmup = Duration::from_secs(5);
    let outage = Duration::from_secs(ctx.random_between(6, 12));
    let mut isolated = false;
    let mut healed = false;

    loop {
        let finished = sim.step().map_err(|s| anyhow::anyhow!(s.to_string()))?;
        let now = sim.elapsed();

        if !isolated && now > warmup {
            tracing::info!("Chaos: full outage for {}s", outage.as_secs());
            hold_all_links(ctx, sim);
            isolated = true;
        }

        if isolated && !healed && now > warmup + outage {
            tracing::info!("Chaos: healing full outage");
            release_all_links(ctx, sim);
            healed = true;
        }

        if finished {
            tracing::info!("Time spent {}", sim.elapsed().as_millis());
            return Ok(());
        }
    }
}

/// **Simulation**
///
/// Quorum loss: after warmup, isolate a majority of nodes for a randomized
/// outage window, then restore links to regain quorum.
pub fn simulation_chaos_quorum_loss(ctx: &mut TurmoilCtx, sim: &mut Sim<'_>) -> anyhow::Result<()> {
    let warmup = Duration::from_secs(5);
    let outage = Duration::from_secs(ctx.random_between(6, 12));
    let isolate_count = (ctx.nodes.len() / 2) + 1;
    let keep_count = ctx.nodes.len().saturating_sub(isolate_count);

    let isolated: Vec<String> = ctx
        .nodes
        .iter()
        .skip(keep_count)
        .map(|n| n.conf.id.clone())
        .collect();

    let mut isolated_now = false;
    let mut healed = false;

    loop {
        let finished = sim.step().map_err(|s| anyhow::anyhow!(s.to_string()))?;
        let now = sim.elapsed();

        if !isolated_now && now > warmup {
            tracing::info!(
                "Chaos: isolating {} nodes for {}s",
                isolated.len(),
                outage.as_secs()
            );
            for node_id in isolated.iter() {
                hold_node(ctx, sim, node_id);
            }
            isolated_now = true;
        }

        if isolated_now && !healed && now > warmup + outage {
            tracing::info!("Chaos: restoring quorum");
            for node_id in isolated.iter() {
                release_node(ctx, sim, node_id);
            }
            healed = true;
        }

        if finished {
            tracing::info!("Time spent {}", sim.elapsed().as_millis());
            return Ok(());
        }
    }
}

/// **Simulation**
///
/// Crash a single node after warmup, keep it down for a short window, then restart it.
pub fn simulation_chaos_crash_one_node_restart(
    ctx: &mut TurmoilCtx,
    sim: &mut Sim<'_>,
) -> anyhow::Result<()> {
    let warmup = Duration::from_secs(5);
    let downtime = Duration::from_secs(ctx.random_between(4, 8));
    let target = ctx.random_id();
    let mut crashed = false;
    let mut restarted = false;
    let mut restart_at: Option<Duration> = None;

    loop {
        let finished = sim.step().map_err(|s| anyhow::anyhow!(s.to_string()))?;
        let now = sim.elapsed();

        if !crashed && now > warmup {
            tracing::info!("Chaos: crashing {}", target);
            sim.crash(target.clone());
            crashed = true;
            restart_at = Some(now + downtime);
            tracing::info!("Chaos: waiting {}s before restart", downtime.as_secs());
        }

        if !restarted {
            if let Some(when) = restart_at {
                if now >= when {
                    tracing::info!("Chaos: restarting {}", target);
                    sim.bounce(target.clone());
                    restarted = true;
                }
            }
        }

        if finished {
            if !restarted {
                anyhow::bail!("simulation finished before crash/restart sequence completed");
            }
            tracing::info!("Time spent {}", sim.elapsed().as_millis());
            return Ok(());
        }
    }
}

/// **Simulation**
///
/// Crash three nodes at the same time after warmup, then restart them.
pub fn simulation_chaos_crash_three_nodes_same_time(
    ctx: &mut TurmoilCtx,
    sim: &mut Sim<'_>,
) -> anyhow::Result<()> {
    let warmup = Duration::from_secs(5);
    let downtime = Duration::from_secs(ctx.random_between(4, 8));
    let mut targets: Vec<String> = ctx.nodes.iter().map(|n| n.conf.id.clone()).collect();
    targets.shuffle(&mut ctx.rng);
    targets.truncate(3.min(targets.len()));
    let mut crashed = false;
    let mut restarted = false;
    let mut restart_at: Option<Duration> = None;

    loop {
        let finished = sim.step().map_err(|s| anyhow::anyhow!(s.to_string()))?;
        let now = sim.elapsed();

        if !crashed && now > warmup {
            tracing::info!("Chaos: crashing nodes {:?}", targets);
            for node_id in targets.iter() {
                sim.crash(node_id.clone());
            }
            crashed = true;
            restart_at = Some(now + downtime);
            tracing::info!("Chaos: waiting {}s before restart", downtime.as_secs());
        }

        if crashed && !restarted {
            if let Some(when) = restart_at {
                if now >= when {
                    tracing::info!("Chaos: restarting nodes {:?}", targets);
                    for node_id in targets.iter() {
                        sim.bounce(node_id.clone());
                    }
                    restarted = true;
                }
            }
        }

        if finished {
            if !crashed || !restarted {
                anyhow::bail!("simulation finished before crash/restart sequence completed");
            }
            tracing::info!("Time spent {}", sim.elapsed().as_millis());
            return Ok(());
        }
    }
}

/// **Simulation**
///
/// Gracefully shut down all nodes once they reach a seed-derived target height,
/// in a deterministic shuffled order (one per tick), then restart them and
/// assert consensus resumes at >= target_height + 1.
pub fn simulation_chaos_graceful_shutdown_all_nodes(
    ctx: &mut TurmoilCtx,
    sim: &mut Sim<'_>,
) -> anyhow::Result<()> {
    let target_height = ctx.random_between(5, 15);
    let downtime = Duration::from_secs(ctx.random_between(4, 8));
    tracing::info!("Chaos: target shutdown height {}", target_height);
    let shutdown_ready = Arc::new(AtomicBool::new(false));
    let verified = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));
    let mut shutdown_task_started = false;
    let mut keepalive_started = false;
    let mut shutdown_order: Option<Vec<String>> = None;
    let mut shutdown_index = 0;
    let mut restart_triggered = false;
    let mut restart_at: Option<Duration> = None;
    let mut verification_started = false;

    loop {
        let finished = sim.step().map_err(|s| anyhow::anyhow!(s.to_string()))?;

        if !keepalive_started {
            let done = Arc::clone(&done);
            sim.client("chaos-keepalive", async move {
                while !done.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Ok(())
            });
            keepalive_started = true;
        }

        if !shutdown_task_started {
            let ctx_clone = ctx.clone();
            let shutdown_ready = Arc::clone(&shutdown_ready);
            sim.client("await-shutdown-height", async move {
                ctx_clone
                    .assert_cluster_converged(target_height)
                    .await
                    .expect("cluster reached target height");
                shutdown_ready.store(true, Ordering::SeqCst);
                Ok(())
            });
            shutdown_task_started = true;
        }

        if shutdown_ready.load(Ordering::SeqCst) {
            if shutdown_order.is_none() {
                let mut order: Vec<String> = ctx.nodes.iter().map(|n| n.conf.id.clone()).collect();
                order.shuffle(&mut ctx.rng);
                tracing::info!("Chaos: shutdown order {:?}", order);
                shutdown_order = Some(order);
            }

            if let Some(order) = shutdown_order.as_ref() {
                if shutdown_index < order.len() {
                    let node_id = order[shutdown_index].clone();
                    if let Some(bus) = ctx.bus_handle(&node_id) {
                        tracing::info!("Chaos: scheduling shutdown for {}", node_id);
                        sim.client(format!("shutdown-{}", node_id), async move {
                            let mut client = ShutdownBusClient::new_from_bus(bus).await;
                            client.send(ShutdownModule {
                                module: std::any::type_name::<RestApi>().to_string(),
                            })?;
                            Ok(())
                        });
                    } else {
                        tracing::warn!("Chaos: no bus handle for {}, skipping shutdown", node_id);
                    }
                    shutdown_index += 1;
                }
            }

            if shutdown_order.is_some() && !restart_triggered {
                let all_stopped = ctx
                    .nodes
                    .iter()
                    .all(|node| !sim.is_host_running(node.conf.id.as_str()));
                if all_stopped {
                    if restart_at.is_none() {
                        tracing::info!(
                            "Chaos: all nodes stopped, waiting {}s before restart",
                            downtime.as_secs()
                        );
                        restart_at = Some(sim.elapsed() + downtime);
                    }
                }
            }
        }

        if !restart_triggered {
            if let Some(when) = restart_at {
                if sim.elapsed() >= when {
                    tracing::info!("Chaos: restarting cluster");
                    for node in ctx.nodes.iter() {
                        sim.bounce(node.conf.id.clone());
                    }
                    restart_triggered = true;
                }
            }
        }

        if restart_triggered && !verification_started {
            let ctx_clone = ctx.clone();
            let verified = Arc::clone(&verified);
            sim.client("post-restart-convergence", async move {
                ctx_clone
                    .assert_cluster_converged(target_height + 1)
                    .await
                    .expect("cluster resumed past target height");
                verified.store(true, Ordering::SeqCst);
                Ok(())
            });
            verification_started = true;
        }

        if verified.load(Ordering::SeqCst) && !done.load(Ordering::SeqCst) {
            tracing::info!("Chaos: verification complete");
            done.store(true, Ordering::SeqCst);
        }

        if finished {
            if done.load(Ordering::SeqCst) {
                tracing::info!("Time spent {}", sim.elapsed().as_millis());
                return Ok(());
            }
            return Err(anyhow::anyhow!(
                "simulation finished before shutdown/restart sequence completed"
            ));
        }
    }
}

/// **Simulation**
///
/// Cascading failure: Start with one node crash, then trigger additional node crashes
/// over time to simulate failure propagation. Eventually restart all crashed nodes.
pub fn simulation_chaos_cascading_failure(
    ctx: &mut TurmoilCtx,
    sim: &mut Sim<'_>,
) -> anyhow::Result<()> {
    let warmup = Duration::from_secs(5);
    let cascade_interval = Duration::from_secs(3);
    let max_failures = 3.min(ctx.nodes.len());
    let recovery_delay = Duration::from_secs(ctx.random_between(5, 10));

    let mut targets: Vec<String> = ctx.nodes.iter().map(|n| n.conf.id.clone()).collect();
    targets.shuffle(&mut ctx.rng);
    targets.truncate(max_failures);

    let mut failure_count = 0;
    let mut last_failure: Option<Duration> = None;
    let mut recovery_started = false;
    let mut recovery_at: Option<Duration> = None;

    tracing::info!("Chaos: cascading failure will affect {:?}", targets);

    loop {
        let finished = sim.step().map_err(|s| anyhow::anyhow!(s.to_string()))?;
        let now = sim.elapsed();

        // Trigger cascading failures
        if failure_count < targets.len() && now > warmup {
            let should_fail = match last_failure {
                None => true,
                Some(last) => now.saturating_sub(last) >= cascade_interval,
            };

            if should_fail {
                let target = &targets[failure_count];
                tracing::info!(
                    "Chaos: cascading failure {} of {} - crashing {}",
                    failure_count + 1,
                    targets.len(),
                    target
                );
                sim.crash(target.clone());
                last_failure = Some(now);
                failure_count += 1;

                if failure_count == targets.len() {
                    recovery_at = Some(now + recovery_delay);
                    tracing::info!(
                        "Chaos: all failures complete, waiting {}s before recovery",
                        recovery_delay.as_secs()
                    );
                }
            }
        }

        // Recover all nodes after delay
        if !recovery_started {
            if let Some(when) = recovery_at {
                if now >= when {
                    tracing::info!("Chaos: recovering all failed nodes");
                    for target in targets.iter() {
                        sim.bounce(target.clone());
                    }
                    recovery_started = true;
                }
            }
        }

        if finished {
            if !recovery_started {
                anyhow::bail!("simulation finished before recovery");
            }
            tracing::info!("Time spent {}", sim.elapsed().as_millis());
            return Ok(());
        }
    }
}

/// **Simulation**
///
/// Flapping network: Rapid connect/disconnect cycles on a random node pair.
/// Tests reconnection logic and state synchronization under unstable network.
pub fn simulation_chaos_flapping_network(
    ctx: &mut TurmoilCtx,
    sim: &mut Sim<'_>,
) -> anyhow::Result<()> {
    let warmup = Duration::from_secs(3);
    let flapping_duration = Duration::from_secs(20);
    let flap_interval = Duration::from_secs(2);

    let (node_a, node_b) = ctx.random_id_pair();
    let mut last_flap = Duration::from_secs(0);
    let mut is_disconnected = false;
    let mut flap_count = 0;
    let mut finished_flapping = false;

    tracing::info!("Chaos: flapping network between {} and {}", node_a, node_b);

    loop {
        let finished = sim.step().map_err(|s| anyhow::anyhow!(s.to_string()))?;
        let now = sim.elapsed();

        if !finished_flapping && now > warmup && now < warmup + flapping_duration {
            if now.saturating_sub(last_flap) >= flap_interval {
                if is_disconnected {
                    tracing::info!("Chaos: flap {} - reconnecting", flap_count);
                    sim.release(node_a.clone(), node_b.clone());
                    sim.release(node_b.clone(), node_a.clone());
                    is_disconnected = false;
                } else {
                    tracing::info!("Chaos: flap {} - disconnecting", flap_count);
                    sim.hold(node_a.clone(), node_b.clone());
                    sim.hold(node_b.clone(), node_a.clone());
                    is_disconnected = true;
                }
                last_flap = now;
                flap_count += 1;
            }
        }

        if !finished_flapping && now >= warmup + flapping_duration {
            if is_disconnected {
                tracing::info!("Chaos: final reconnection");
                sim.release(node_a.clone(), node_b.clone());
                sim.release(node_b.clone(), node_a.clone());
            }
            finished_flapping = true;
            tracing::info!("Chaos: completed {} flaps", flap_count);
        }

        if finished {
            tracing::info!("Time spent {}", sim.elapsed().as_millis());
            return Ok(());
        }
    }
}

/// **Simulation**
///
/// Asymmetric partition: Create a unidirectional communication failure where
/// node A can send to node B, but B cannot reply to A.
pub fn simulation_chaos_asymmetric_partition(
    ctx: &mut TurmoilCtx,
    sim: &mut Sim<'_>,
) -> anyhow::Result<()> {
    let warmup = Duration::from_secs(5);
    let partition_duration = Duration::from_secs(ctx.random_between(8, 15));

    let (node_a, node_b) = ctx.random_id_pair();
    let mut partitioned = false;
    let mut healed = false;

    tracing::info!(
        "Chaos: asymmetric partition - {} can send to {}, but {} cannot reply",
        node_a,
        node_b,
        node_b
    );

    loop {
        let finished = sim.step().map_err(|s| anyhow::anyhow!(s.to_string()))?;
        let now = sim.elapsed();

        if !partitioned && now > warmup {
            tracing::info!(
                "Chaos: starting asymmetric partition for {}s",
                partition_duration.as_secs()
            );
            // Only hold B -> A, not A -> B
            sim.hold(node_b.clone(), node_a.clone());
            partitioned = true;
        }

        if partitioned && !healed && now > warmup + partition_duration {
            tracing::info!("Chaos: healing asymmetric partition");
            sim.release(node_b.clone(), node_a.clone());
            healed = true;
        }

        if finished {
            tracing::info!("Time spent {}", sim.elapsed().as_millis());
            return Ok(());
        }
    }
}

/// **Simulation**
///
/// Slow leader: Inject high latency specifically on a node, particularly affecting
/// the leader. Tests whether the system handles slow leader performance gracefully.
pub fn simulation_chaos_slow_leader(ctx: &mut TurmoilCtx, sim: &mut Sim<'_>) -> anyhow::Result<()> {
    let warmup = Duration::from_secs(5);
    let slow_duration = Duration::from_secs(ctx.random_between(10, 20));
    let leader_id = Arc::new(Mutex::new(None::<String>));
    let leader_ready = Arc::new(AtomicBool::new(false));
    let mut leader_lookup_started = false;

    let mut slowdown_applied = false;
    let mut restored = false;

    tracing::info!("Chaos: will slow down the current leader");

    loop {
        let finished = sim.step().map_err(|s| anyhow::anyhow!(s.to_string()))?;
        let now = sim.elapsed();

        if !leader_lookup_started {
            let nodes = ctx.nodes.clone();
            let leader_id = Arc::clone(&leader_id);
            let leader_ready = Arc::clone(&leader_ready);
            sim.client("resolve-leader", async move {
                let client = nodes
                    .first()
                    .expect("at least one node")
                    .client
                    .retry_15times_1000ms();

                let mut resolved = None::<String>;
                for _ in 0..30 {
                    if let Ok(info) = client.get_consensus_info().await {
                        let leader_pk = info.round_leader;
                        for node in nodes.iter() {
                            if let Ok(node_info) = node.client.get_node_info().await {
                                if node_info.pubkey.as_ref() == Some(&leader_pk) {
                                    resolved = Some(node_info.id);
                                    break;
                                }
                            }
                        }
                        if resolved.is_some() {
                            break;
                        }
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }

                if resolved.is_none() {
                    tracing::warn!("Chaos: failed to resolve leader from consensus info");
                }

                *leader_id.lock().await = resolved;
                leader_ready.store(true, Ordering::SeqCst);
                Ok(())
            });
            leader_lookup_started = true;
        }

        if !slowdown_applied && now > warmup {
            if leader_ready.load(Ordering::SeqCst) {
                let target_node = leader_id.try_lock().ok().and_then(|guard| guard.clone());
                let Some(target_node) = target_node else {
                    return Err(anyhow::anyhow!(
                        "failed to resolve leader before applying slowdown"
                    ));
                };

                tracing::info!(
                    "Chaos: applying high latency to leader {} for {}s",
                    target_node,
                    slow_duration.as_secs()
                );
                for other_node in ctx
                    .nodes
                    .clone()
                    .iter()
                    .filter(|n| n.conf.id != target_node)
                {
                    let slowness = Duration::from_millis(ctx.random_between(500, 1500));
                    sim.set_link_latency(target_node.clone(), other_node.conf.id.clone(), slowness);
                    sim.set_link_latency(other_node.conf.id.clone(), target_node.clone(), slowness);
                }
                slowdown_applied = true;
            }
        }

        if slowdown_applied && !restored && now > warmup + slow_duration {
            let target_node = leader_id
                .try_lock()
                .ok()
                .and_then(|guard| guard.clone())
                .unwrap_or_else(|| "unknown-leader".to_string());
            tracing::info!("Chaos: restoring normal latency to {}", target_node);
            for other_node in ctx
                .nodes
                .clone()
                .iter()
                .filter(|n| n.conf.id != target_node)
            {
                sim.set_link_latency(
                    target_node.clone(),
                    other_node.conf.id.clone(),
                    Duration::from_millis(0),
                );
                sim.set_link_latency(
                    other_node.conf.id.clone(),
                    target_node.clone(),
                    Duration::from_millis(0),
                );
            }
            restored = true;
        }

        if finished {
            tracing::info!("Time spent {}", sim.elapsed().as_millis());
            return Ok(());
        }
    }
}

/// **Simulation**
///
/// Clock skew: Introduce time drift between nodes to test timeout handling
/// and time-based logic. Note: Turmoil simulates deterministic time, so this
/// tests logical clock differences via message delays.
pub fn simulation_chaos_clock_skew(ctx: &mut TurmoilCtx, sim: &mut Sim<'_>) -> anyhow::Result<()> {
    let warmup = Duration::from_secs(3);
    let skew_duration = Duration::from_secs(15);

    // Pick two nodes to have skewed communication
    let (fast_node, slow_node) = ctx.random_id_pair();
    let mut skew_applied = false;
    let mut restored = false;

    tracing::info!(
        "Chaos: simulating clock skew between {} and {}",
        fast_node,
        slow_node
    );

    loop {
        let finished = sim.step().map_err(|s| anyhow::anyhow!(s.to_string()))?;
        let now = sim.elapsed();

        if !skew_applied && now > warmup {
            tracing::info!("Chaos: applying clock skew via asymmetric latencies");
            // Simulate clock skew by making one direction much slower
            let skew_delay = Duration::from_millis(ctx.random_between(200, 800));
            sim.set_link_latency(slow_node.clone(), fast_node.clone(), skew_delay);
            skew_applied = true;
        }

        if skew_applied && !restored && now > warmup + skew_duration {
            tracing::info!("Chaos: removing clock skew");
            sim.set_link_latency(
                slow_node.clone(),
                fast_node.clone(),
                Duration::from_millis(0),
            );
            restored = true;
        }

        if finished {
            tracing::info!("Time spent {}", sim.elapsed().as_millis());
            return Ok(());
        }
    }
}

/// **Simulation**
///
/// Heavy load during chaos: Combine high transaction volume with network partition
/// to test backpressure and recovery under load. This requires a workload that
/// submits many transactions.
pub fn simulation_chaos_heavy_load_during_partition(
    ctx: &mut TurmoilCtx,
    sim: &mut Sim<'_>,
) -> anyhow::Result<()> {
    let warmup = Duration::from_secs(5);
    let partition_duration = Duration::from_secs(ctx.random_between(8, 12));
    let mut partitioned = false;
    let mut healed = false;

    let mut nodes = ctx.nodes.clone();
    let mid = nodes.len() / 2;
    let group_a = nodes.drain(..mid).collect::<Vec<_>>();
    let group_b = nodes;

    tracing::info!("Chaos: heavy load with partition - workload should submit many transactions");

    loop {
        let finished = sim.step().map_err(|s| anyhow::anyhow!(s.to_string()))?;
        let now = sim.elapsed();

        if !partitioned && now > warmup {
            tracing::info!("Chaos: partitioning network during heavy load");
            for a in group_a.iter() {
                for b in group_b.iter() {
                    sim.hold(a.conf.id.clone(), b.conf.id.clone());
                    sim.hold(b.conf.id.clone(), a.conf.id.clone());
                }
            }
            partitioned = true;
        }

        if partitioned && !healed && now > warmup + partition_duration {
            tracing::info!("Chaos: healing partition, load should continue");
            for a in group_a.iter() {
                for b in group_b.iter() {
                    sim.release(a.conf.id.clone(), b.conf.id.clone());
                    sim.release(b.conf.id.clone(), a.conf.id.clone());
                }
            }
            healed = true;
        }

        if finished {
            tracing::info!("Time spent {}", sim.elapsed().as_millis());
            return Ok(());
        }
    }
}

/// **Simulation**
///
/// Memory pressure simulation: Hold and release messages repeatedly to simulate
/// message queue buildup and backpressure scenarios.
pub fn simulation_chaos_memory_pressure(
    ctx: &mut TurmoilCtx,
    sim: &mut Sim<'_>,
) -> anyhow::Result<()> {
    let warmup = Duration::from_secs(5);
    let pressure_cycles = 3;
    let hold_duration = Duration::from_secs(4);
    let release_duration = Duration::from_secs(2);

    let target_node = ctx.random_id();
    let mut cycle = 0;
    let mut is_holding = false;
    let mut last_action = Duration::from_secs(0);
    let mut started = false;

    tracing::info!(
        "Chaos: memory pressure on {} with {} cycles",
        target_node,
        pressure_cycles
    );

    loop {
        let finished = sim.step().map_err(|s| anyhow::anyhow!(s.to_string()))?;
        let now = sim.elapsed();

        if !started && now > warmup {
            started = true;
            last_action = now;
        }

        if started && cycle < pressure_cycles {
            if !is_holding {
                // Start holding messages to build up queue
                if now.saturating_sub(last_action) >= release_duration {
                    tracing::info!(
                        "Chaos: pressure cycle {} - holding messages to {}",
                        cycle + 1,
                        target_node
                    );
                    for other in ctx
                        .nodes
                        .clone()
                        .iter()
                        .filter(|n| n.conf.id != target_node)
                    {
                        sim.hold(other.conf.id.clone(), target_node.clone());
                    }
                    is_holding = true;
                    last_action = now;
                }
            } else {
                // Release messages to simulate burst processing
                if now.saturating_sub(last_action) >= hold_duration {
                    tracing::info!(
                        "Chaos: pressure cycle {} - releasing messages to {}",
                        cycle + 1,
                        target_node
                    );
                    for other in ctx
                        .nodes
                        .clone()
                        .iter()
                        .filter(|n| n.conf.id != target_node)
                    {
                        sim.release(other.conf.id.clone(), target_node.clone());
                    }
                    is_holding = false;
                    last_action = now;
                    cycle += 1;
                }
            }
        }

        // Ensure final release if still holding
        if started && cycle >= pressure_cycles && is_holding {
            tracing::info!("Chaos: final release of held messages");
            for other in ctx
                .nodes
                .clone()
                .iter()
                .filter(|n| n.conf.id != target_node)
            {
                sim.release(other.conf.id.clone(), target_node.clone());
            }
            is_holding = false;
        }

        if finished {
            tracing::info!("Time spent {}", sim.elapsed().as_millis());
            return Ok(());
        }
    }
}

/// **Simulation**
///
/// Multiple sequential failures: Chain different failure types to test recovery
/// from complex, evolving failure scenarios. Combines partition, slow network, and crash.
pub fn simulation_chaos_sequential_failures(
    ctx: &mut TurmoilCtx,
    sim: &mut Sim<'_>,
) -> anyhow::Result<()> {
    let warmup = Duration::from_secs(3);
    let phase1_duration = Duration::from_secs(6); // Network partition
    let phase2_duration = Duration::from_secs(6); // Slow network
    let phase3_duration = Duration::from_secs(5); // Node crash

    let (node_a, node_b) = ctx.random_id_pair();
    let crash_target = ctx.random_id();

    let phase1_end = warmup + phase1_duration;
    let phase2_end = phase1_end + phase2_duration;
    let phase3_end = phase2_end + phase3_duration;

    let mut phase = 0;

    tracing::info!("Chaos: sequential failures - partition, slowdown, crash");

    loop {
        let finished = sim.step().map_err(|s| anyhow::anyhow!(s.to_string()))?;
        let now = sim.elapsed();

        // Phase 1: Network partition
        if phase == 0 && now > warmup {
            tracing::info!(
                "Chaos: phase 1 - network partition between {} and {}",
                node_a,
                node_b
            );
            sim.hold(node_a.clone(), node_b.clone());
            sim.hold(node_b.clone(), node_a.clone());
            phase = 1;
        }

        // Phase 2: Heal partition, apply slow network
        if phase == 1 && now > phase1_end {
            tracing::info!("Chaos: phase 2 - healing partition, applying slow network");
            sim.release(node_a.clone(), node_b.clone());
            sim.release(node_b.clone(), node_a.clone());

            for node in ctx.nodes.clone().iter() {
                for other in ctx
                    .nodes
                    .clone()
                    .iter()
                    .filter(|n| n.conf.id != node.conf.id)
                {
                    let slowness = Duration::from_millis(ctx.random_between(200, 500));
                    sim.set_link_latency(node.conf.id.clone(), other.conf.id.clone(), slowness);
                }
            }
            phase = 2;
        }

        // Phase 3: Restore speed, crash a node
        if phase == 2 && now > phase2_end {
            tracing::info!(
                "Chaos: phase 3 - restoring speed, crashing {}",
                crash_target
            );
            for node in ctx.nodes.clone().iter() {
                for other in ctx
                    .nodes
                    .clone()
                    .iter()
                    .filter(|n| n.conf.id != node.conf.id)
                {
                    sim.set_link_latency(
                        node.conf.id.clone(),
                        other.conf.id.clone(),
                        Duration::from_millis(0),
                    );
                }
            }
            sim.crash(crash_target.clone());
            phase = 3;
        }

        // Phase 4: Restart crashed node
        if phase == 3 && now > phase3_end {
            tracing::info!("Chaos: phase 4 - restarting {}", crash_target);
            sim.bounce(crash_target.clone());
            phase = 4;
        }

        if finished {
            if phase < 4 {
                anyhow::bail!("simulation finished before all phases completed");
            }
            tracing::info!("Time spent {}", sim.elapsed().as_millis());
            return Ok(());
        }
    }
}
