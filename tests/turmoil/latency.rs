//! Network latency simulation tests.
//!
//! These simulations test the system's behavior under various network latency conditions.

use std::time::Duration;

use hyli_net::net::Sim;

use crate::fixtures::turmoil::TurmoilCtx;

/// **Simulation**
///
/// Unroll tick steps until clients finish (baseline simulation with no modifications).
pub fn simulation_basic(_ctx: &mut TurmoilCtx, sim: &mut Sim<'_>) -> anyhow::Result<()> {
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
/// Simulate a slow network (with fixed random latencies).
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
/// Simulate 1 really slow node (with fixed random latencies).
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
/// Simulate 2 really slow nodes (with fixed random latencies).
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
