//! Message corruption simulation tests.
//!
//! These simulations test the system's resilience when messages are corrupted
//! (modified) before being sent over the network.

use bytes::Bytes;
use hyli_net::net::Sim;
use rand::{rngs::StdRng, Rng, SeedableRng};

use crate::fixtures::turmoil::{
    install_consensus_message_corrupter, install_mempool_message_corrupter,
    install_net_message_corrupter, TurmoilCtx,
};

fn corrupt_random_bytes(
    rng: &mut StdRng,
    corruption_probability: f64,
    bytes: &[u8],
) -> Option<Bytes> {
    // Corrupt a percentage of messages based on the provided probability.
    if !rng.random_bool(corruption_probability) {
        return None;
    }

    // Flip some bits in the message
    let mut corrupted = bytes.to_vec();
    if corrupted.len() > 10 {
        // Corrupt a byte somewhere in the middle of the message
        let pos = rng.random_range(5..corrupted.len() - 5);
        corrupted[pos] ^= 0xFF;
    }

    Some(Bytes::from(corrupted))
}

/// **Simulation**
///
/// Corrupt random messages by flipping bits in the payload.
/// The system should detect these corrupted messages and handle them gracefully.
pub fn simulation_corrupt_random_messages(
    _ctx: &mut TurmoilCtx,
    sim: &mut Sim<'_>,
) -> anyhow::Result<()> {
    let seed = std::env::var("HYLI_TURMOIL_SEED")
        .ok()
        .and_then(|seed| seed.parse::<u64>().ok())
        .unwrap_or(0);
    let rng = std::sync::Arc::new(std::sync::Mutex::new(StdRng::seed_from_u64(seed)));

    let _corrupter = install_net_message_corrupter(move |_message, bytes| {
        let mut rng = rng.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        corrupt_random_bytes(&mut rng, 0.07, bytes)
    });

    let result = loop {
        match sim.step() {
            Ok(true) => {
                tracing::info!("Time spent {}", sim.elapsed().as_millis());
                break Ok(());
            }
            Ok(false) => {}
            Err(e) => break Err(anyhow::anyhow!(e.to_string())),
        }
    };

    result
}

/// **Simulation**
///
/// Corrupt random consensus messages by flipping bits in the payload.
/// The system should detect these corrupted messages and handle them gracefully.
pub fn simulation_corrupt_consensus_messages(
    _ctx: &mut TurmoilCtx,
    sim: &mut Sim<'_>,
) -> anyhow::Result<()> {
    let seed = std::env::var("HYLI_TURMOIL_SEED")
        .ok()
        .and_then(|seed| seed.parse::<u64>().ok())
        .unwrap_or(0);
    let rng = std::sync::Arc::new(std::sync::Mutex::new(StdRng::seed_from_u64(seed)));

    let _corrupter = install_consensus_message_corrupter(move |_message, bytes| {
        let mut rng = rng.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        // Corrupt 7% of mempool messages
        let corruption_probability = 0.07;
        corrupt_random_bytes(&mut rng, corruption_probability, bytes)
    });

    let result = loop {
        match sim.step() {
            Ok(true) => {
                tracing::info!("Time spent {}", sim.elapsed().as_millis());
                break Ok(());
            }
            Ok(false) => {}
            Err(e) => break Err(anyhow::anyhow!(e.to_string())),
        }
    };

    result
}

/// **Simulation**
///
/// Corrupt random mempool messages by flipping bits in the payload.
/// The system should detect these corrupted messages and handle them gracefully.
pub fn simulation_corrupt_mempool_messages(
    _ctx: &mut TurmoilCtx,
    sim: &mut Sim<'_>,
) -> anyhow::Result<()> {
    let seed = std::env::var("HYLI_TURMOIL_SEED")
        .ok()
        .and_then(|seed| seed.parse::<u64>().ok())
        .unwrap_or(0);
    let rng = std::sync::Arc::new(std::sync::Mutex::new(StdRng::seed_from_u64(seed)));

    let _corrupter = install_mempool_message_corrupter(move |_message, bytes| {
        let mut rng = rng.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        // Corrupt 7% of mempool messages
        let corruption_probability = 0.07;
        corrupt_random_bytes(&mut rng, corruption_probability, bytes)
    });

    let result = loop {
        match sim.step() {
            Ok(true) => {
                tracing::info!("Time spent {}", sim.elapsed().as_millis());
                break Ok(());
            }
            Ok(false) => {}
            Err(e) => break Err(anyhow::anyhow!(e.to_string())),
        }
    };

    result
}
