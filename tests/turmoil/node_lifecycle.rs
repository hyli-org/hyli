//! Node lifecycle simulation tests.
//!
//! These simulations test the system's behavior when nodes restart,
//! join, or leave the network.

use std::time::Duration;

use client_sdk::rest_client::NodeApiClient;
use hyli_net::net::Sim;

use crate::fixtures::{test_helpers::wait_height, turmoil::TurmoilCtx};

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
        if offline_until.is_none() && now > warmup && now.saturating_sub(last_cycle) > downtime * 2
        {
            for other in ctx.nodes.clone().iter().filter(|n| n.conf.id != target) {
                sim.hold(target.clone(), other.conf.id.clone());
                sim.hold(other.conf.id.clone(), target.clone());
            }
            offline_until = Some(now + downtime);
            last_cycle = now;
        }

        if let Some(until) = offline_until {
            if now >= until {
                for other in ctx.nodes.clone().iter().filter(|n| n.conf.id != target) {
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
/// Add a new node to the network during simulation between 5 and 15 seconds after simulation starts.
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
