//! Message corruption simulation tests.
//!
//! These simulations test the system's resilience when messages are corrupted
//! (modified) before being sent over the network.

use bytes::Bytes;
use hyli_net::net::Sim;
use rand::{rngs::StdRng, Rng, SeedableRng};

use crate::fixtures::turmoil::{install_net_message_corrupter, TurmoilCtx};

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

        // Corrupt 1% of messages
        if !rng.random_bool(0.1) {
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
