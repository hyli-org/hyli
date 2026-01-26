//! Chaos-style simulations using the Turmoil network harness.
//!
//! These scenarios mirror the E2E chaos tests with deterministic schedules
//! for reproducibility and fine-grained control.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

use hyli::bus::{bus_client, BusClientSender};
use hyli::rest::RestApi;
use hyli_modules::modules::signal::ShutdownModule;
use hyli_net::net::Sim;
use rand::seq::SliceRandom;

use crate::fixtures::turmoil::{
    hold_all_links, hold_node, release_all_links, release_node, TurmoilCtx,
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
/// Gracefully shut down all nodes once they reach a seed-derived target height,
/// in a deterministic shuffled order (one per tick), then restart them and
/// assert consensus resumes at >= target_height + 1.
pub fn simulation_chaos_graceful_shutdown_all_nodes(
    ctx: &mut TurmoilCtx,
    sim: &mut Sim<'_>,
) -> anyhow::Result<()> {
    let target_height = ctx.random_between(5, 15);
    tracing::info!("Chaos: target shutdown height {}", target_height);
    let shutdown_ready = Arc::new(AtomicBool::new(false));
    let verified = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));
    let mut shutdown_task_started = false;
    let mut keepalive_started = false;
    let mut shutdown_order: Option<Vec<String>> = None;
    let mut shutdown_index = 0;
    let mut restart_triggered = false;
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
                    tracing::info!("Chaos: all nodes stopped, restarting cluster");
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
