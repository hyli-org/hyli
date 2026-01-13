//! Turmoil-based network simulation tests.
//!
//! This module contains tests that use the turmoil framework to simulate
//! various network conditions and verify the system's behavior.
//!
//! ## Test Categories
//!
//! - **latency**: Tests for various network latency conditions
//! - **partition**: Tests for network partitions and message holds
//! - **message_drop**: Tests for dropped messages (targeted and random)
//! - **corruption**: Tests for corrupted messages (bit flips, payload modification)
//! - **node_lifecycle**: Tests for node restarts and dynamic membership
//! - **workloads**: Test workloads that run during simulations

#![allow(clippy::all)]
#![cfg(feature = "turmoil")]
#![cfg(test)]

mod fixtures;

#[path = "turmoil/common.rs"]
mod common;
#[path = "turmoil/corruption.rs"]
mod corruption;
#[path = "turmoil/latency.rs"]
mod latency;
#[path = "turmoil/message_drop.rs"]
mod message_drop;
#[path = "turmoil/node_lifecycle.rs"]
mod node_lifecycle;
#[path = "turmoil/partition.rs"]
mod partition;
#[path = "turmoil/workloads.rs"]
mod workloads;

use std::time::Duration;

use crate::fixtures::turmoil::TurmoilCtx;

// Re-export simulations for use in test macros
use corruption::simulation_corrupt_random_messages;
use latency::{
    simulation_basic, simulation_realistic_network, simulation_slow_network, simulation_slow_node,
    simulation_two_slow_nodes,
};
use message_drop::{
    simulation_drop_all_messages, simulation_drop_data_proposals, simulation_drop_data_votes,
    simulation_drop_storm,
};
use node_lifecycle::{simulation_one_more_node, simulation_restart_node};
use partition::{simulation_hold, simulation_partition, simulation_timeout_split_view};
use workloads::{submit_10_contracts, timeout_split_view_recovery};

use common::{assert_converged, assert_converged_with_one_block_height_tolerance};

macro_rules! turmoil_simple {
    ($seed:literal, $simulation:ident, $test:ident) => {
        paste::paste! {
        #[test_log::test]
            fn [<turmoil_ $simulation _ $seed _ $test>]() -> anyhow::Result<()> {
                tracing::info!("Starting test {} with seed {}", stringify!([<turmoil_ $simulation _ $seed _ $test>]), $seed);
                let mut sim = hyli_net::turmoil::Builder::new()
                    .simulation_duration(Duration::from_secs(120))
                    .tick_duration(Duration::from_millis(20))
                    .min_message_latency(Duration::from_millis(20))
                .tcp_capacity(256)
                .enable_tokio_io()
                    .rng_seed($seed)
                    .build();

                let mut ctx = TurmoilCtx::new_multi(4, 500, $seed, &mut sim)?;

                for node in ctx.nodes.iter() {
                    let cloned_node = node.clone();
                    sim.client(format!("client {}", node.conf.id.clone()), async move {
                        _ = $test(cloned_node).await?;
                        Ok(())
                    });
                }

                $simulation(&mut ctx, &mut sim)?;
                match stringify!($simulation) {
                    "simulation_corrupt_random_messages" => {
                        assert_converged_with_one_block_height_tolerance(&ctx, &mut sim, 1)?;
                    }
                    "simulation_restart_node" => {}
                    _ => {
                        assert_converged(&ctx, &mut sim, 1)?;
                    }
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

macro_rules! turmoil_simple_flaky {
    ($seed:literal, $simulation:ident, $test:ident) => {
        paste::paste! {
        #[test_log::test]
        #[ignore = "flaky"]
            fn [<turmoil_ $simulation _ $seed _ $test>]() -> anyhow::Result<()> {
                tracing::info!("Starting test {} with seed {}", stringify!([<turmoil_ $simulation _ $seed _ $test>]), $seed);
                let mut sim = hyli_net::turmoil::Builder::new()
                    .simulation_duration(Duration::from_secs(120))
                    .tick_duration(Duration::from_millis(20))
                    .min_message_latency(Duration::from_millis(20))
                .tcp_capacity(256)
                .enable_tokio_io()
                    .rng_seed($seed)
                    .build();

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
                    match stringify!($simulation) {
                        "simulation_drop_storm"
                        | "simulation_drop_data_proposals"
                        | "simulation_drop_data_votes"
                        | "simulation_drop_all_messages"
                        | "simulation_corrupt_random_messages" => {
                            assert_converged_with_one_block_height_tolerance(&ctx, &mut sim, 1)?;
                        }
                        _ => {
                            assert_converged(&ctx, &mut sim, 1)?;
                        }
                    }
                }

                Ok(())
            }
        }
    };

    ($seed_from:literal..=$seed_to:literal, $simulation:ident, $test:ident) => {
        seq_macro::seq!(SEED in $seed_from..=$seed_to {
            turmoil_simple_flaky!(SEED, $simulation, $test);
        });
    };
}

// =============================================================================
// Latency Tests
// =============================================================================

turmoil_simple!(411..=420, simulation_basic, submit_10_contracts);
turmoil_simple!(511..=520, simulation_slow_node, submit_10_contracts);
turmoil_simple!(511..=520, simulation_two_slow_nodes, submit_10_contracts);
turmoil_simple!(511..=520, simulation_slow_network, submit_10_contracts);
turmoil_simple!(651..=660, simulation_realistic_network, submit_10_contracts);

// =============================================================================
// Partition Tests
// =============================================================================

turmoil_simple!(511..=520, simulation_hold, submit_10_contracts);
turmoil_simple!(621..=630, simulation_partition, submit_10_contracts);
turmoil_simple!(
    661..=670,
    simulation_timeout_split_view,
    timeout_split_view_recovery
);

// =============================================================================
// Message Drop Tests
// =============================================================================

turmoil_simple_flaky!(631..=640, simulation_drop_storm, submit_10_contracts);
turmoil_simple!(
    671..=680,
    simulation_drop_data_proposals,
    submit_10_contracts
);
turmoil_simple!(681..=690, simulation_drop_data_votes, submit_10_contracts);
turmoil_simple!(691..=700, simulation_drop_all_messages, submit_10_contracts);

// =============================================================================
// Node Lifecycle Tests
// =============================================================================

turmoil_simple!(611..=620, simulation_one_more_node, submit_10_contracts);
turmoil_simple!(641..=650, simulation_restart_node, submit_10_contracts);

// =============================================================================
// Message Corruption Tests
// =============================================================================

turmoil_simple!(
    701..=710,
    simulation_corrupt_random_messages,
    submit_10_contracts
);
