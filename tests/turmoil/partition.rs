//! Network partition and message hold simulation tests.
//!
//! These simulations test the system's behavior when network partitions occur
//! or when messages are temporarily held between nodes.

use std::time::Duration;

use hyli_net::net::Sim;

use crate::fixtures::turmoil::TurmoilCtx;

use super::common::HoldConfiguration;

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
/// Start holding message delivery between two peers at a random moment,
/// for a random duration, and release them (no message loss).
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
/// Create a view split by holding messages from two nodes to the other two,
/// then heal the links so the cluster can continue.
pub fn simulation_timeout_split_view(
    ctx: &mut TurmoilCtx,
    sim: &mut Sim<'_>,
) -> anyhow::Result<()> {
    let nodes = ctx.nodes.clone();
    let tc_nodes = nodes[..2].to_vec();
    let non_tc_nodes = nodes[2..].to_vec();

    let warmup = Duration::from_secs(5);
    let split_duration = Duration::from_secs(12);
    let mut split = false;
    let mut healed = false;

    loop {
        let finished = sim.step().map_err(|s| anyhow::anyhow!(s.to_string()))?;
        let now = sim.elapsed();

        if !split && now > warmup {
            split = true;
            for from in tc_nodes.iter() {
                for to in non_tc_nodes.iter() {
                    sim.hold(from.conf.id.clone(), to.conf.id.clone());
                }
            }
        }

        if split && !healed && now > warmup + split_duration {
            healed = true;
            for from in tc_nodes.iter() {
                for to in non_tc_nodes.iter() {
                    sim.release(from.conf.id.clone(), to.conf.id.clone());
                }
            }
        }

        if finished {
            tracing::info!("Time spent {}", sim.elapsed().as_millis());
            return Ok(());
        }
    }
}
