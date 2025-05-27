#![allow(clippy::all)]
#![cfg(feature = "turmoil")]
#![cfg(test)]

mod fixtures;

use std::time::Duration;

use client_sdk::rest_client::NodeApiClient;
use fixtures::turmoil::TurmoilHost;
use hyle_model::{
    BlobTransaction, ContractAction, ContractName, ProgramId, RegisterContractAction,
    StateCommitment,
};
use hyle_modules::log_error;
use hyle_net::net::Sim;
use rand::{rngs::StdRng, SeedableRng};

use crate::fixtures::{test_helpers::wait_height, turmoil::TurmoilCtx};

pub fn make_register_contract_tx(name: ContractName) -> BlobTransaction {
    BlobTransaction::new(
        "hyle@hyle",
        vec![RegisterContractAction {
            verifier: "test".into(),
            program_id: ProgramId(vec![]),
            state_commitment: StateCommitment(vec![0, 1, 2, 3]),
            contract_name: name,
            ..Default::default()
        }
        .as_blob("hyle".into(), None, None)],
    )
}

macro_rules! turmoil_simple {
    ($seed:literal, $simulation:ident, $test:ident) => {
        paste::paste! {
        #[test_log::test]
            fn [<turmoil_ $simulation _ $seed _ $test>]() -> anyhow::Result<()> {
                tracing::info!("Starting test {} with seed {}", stringify!([<turmoil_ $simulation _ $seed _ $test>]), $seed);
                let rng = StdRng::seed_from_u64($seed);
                let mut sim = hyle_net::turmoil::Builder::new()
                    .simulation_duration(Duration::from_secs(120))
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
                    assert_eq!(contract.name.0, format!("contract-{}", i).as_str());
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
        let contract = client_with_retries
            .get_contract(format!("contract-{}", i).into())
            .await?;
        assert_eq!(contract.name.0, format!("contract-{}", i).as_str());
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
