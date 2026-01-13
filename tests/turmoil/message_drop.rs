//! Message dropping simulation tests.
//!
//! These simulations test the system's resilience when messages are dropped
//! either randomly or for specific message types.

use std::time::Duration;

use hyli::mempool::MempoolNetMessage;
use hyli_net::net::Sim;
use rand::{rngs::StdRng, Rng, SeedableRng};

use crate::fixtures::turmoil::{install_net_message_dropper, TurmoilCtx};

/// **Simulation**
///
/// Repeatedly cut random links for short bursts, then heal everything.
pub fn simulation_drop_storm(ctx: &mut TurmoilCtx, sim: &mut Sim<'_>) -> anyhow::Result<()> {
    let warmup = Duration::from_secs(5);
    let storm_duration = Duration::from_secs(20);
    let hold_interval = Duration::from_secs(3);

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
        }

        if finished {
            tracing::info!("Time spent {}", sim.elapsed().as_millis());
            return Ok(());
        }
    }
}

/// **Simulation**
///
/// Drop a handful of data proposal messages and ensure the network still converges.
pub fn simulation_drop_data_proposals(
    _ctx: &mut TurmoilCtx,
    sim: &mut Sim<'_>,
) -> anyhow::Result<()> {
    let seed = std::env::var("HYLI_TURMOIL_SEED")
        .ok()
        .and_then(|seed| seed.parse::<u64>().ok())
        .unwrap_or(0);
    let rng = std::sync::Arc::new(std::sync::Mutex::new(StdRng::seed_from_u64(seed)));

    let _dropper = install_net_message_dropper(move |message| {
        let is_data_proposal = matches!(
            message,
            hyli::p2p::network::NetMessage::MempoolMessage(msg)
                if matches!(msg.msg, MempoolNetMessage::DataProposal(..))
        );

        if !is_data_proposal {
            return false;
        }

        let mut rng = rng.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        rng.gen_bool(0.80)
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
/// Drop a handful of data vote messages and ensure the network still converges.
pub fn simulation_drop_data_votes(_ctx: &mut TurmoilCtx, sim: &mut Sim<'_>) -> anyhow::Result<()> {
    let seed = std::env::var("HYLI_TURMOIL_SEED")
        .ok()
        .and_then(|seed| seed.parse::<u64>().ok())
        .unwrap_or(0);
    let rng = std::sync::Arc::new(std::sync::Mutex::new(StdRng::seed_from_u64(seed)));

    let _dropper = install_net_message_dropper(move |message| {
        let is_data_vote = matches!(
            message,
            hyli::p2p::network::NetMessage::MempoolMessage(msg)
                if matches!(msg.msg, MempoolNetMessage::DataVote(..))
        );

        if !is_data_vote {
            return false;
        }

        let mut rng = rng.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        rng.gen_bool(0.9)
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
/// Drop any message at a high rate and ensure the network still converges.
pub fn simulation_drop_all_messages(
    _ctx: &mut TurmoilCtx,
    sim: &mut Sim<'_>,
) -> anyhow::Result<()> {
    let seed = std::env::var("HYLI_TURMOIL_SEED")
        .ok()
        .and_then(|seed| seed.parse::<u64>().ok())
        .unwrap_or(0);
    let rng = std::sync::Arc::new(std::sync::Mutex::new(StdRng::seed_from_u64(seed)));

    let _dropper = install_net_message_dropper(move |_message| {
        let mut rng = rng.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        rng.gen_bool(0.10)
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
