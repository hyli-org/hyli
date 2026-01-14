//! Message corruption simulation tests.
//!
//! These simulations test the system's resilience when messages are corrupted
//! (modified) before being sent over the network.
//!
//! ## Smart Corruption Strategies
//!
//! Each consensus message type has critical fields that, when corrupted, cause
//! specific failure modes:
//!
//! | Message Type      | Critical Fields                | Corruption Effect                    |
//! |-------------------|--------------------------------|--------------------------------------|
//! | Prepare           | slot, parent_hash, timestamp   | Proposal validation failure          |
//! | PrepareVote       | signature, proposal_hash       | Vote invalidation, wrong proposal    |
//! | Confirm           | PrepareQC signature, validators| Quorum validation failure            |
//! | ConfirmAck        | signature, proposal_hash       | Ack invalidation                     |
//! | Commit            | CommitQC signature, validators | Commit validation failure            |
//! | Timeout           | slot, view, signature          | Timeout aggregation failure          |
//! | TimeoutCertificate| TimeoutQC, slot, view          | View change failure                  |

use bytes::Bytes;
use hyli::consensus::ConsensusNetMessage;
use hyli_net::net::Sim;
use rand::{rngs::StdRng, Rng, SeedableRng};

use crate::fixtures::turmoil::{
    install_consensus_message_corrupter, install_mempool_message_corrupter,
    install_net_message_corrupter, TurmoilCtx,
};

/// Corruption strategy for smart message corruption.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorruptionStrategy {
    /// Corrupt signature bytes (causes cryptographic validation failure)
    Signature,
    /// Corrupt hash field bytes (causes hash mismatch)
    Hash,
    /// Corrupt slot/view numbers (causes wrong round handling)
    SlotView,
    /// Corrupt timestamp field (causes time validation failure)
    Timestamp,
    /// Corrupt type marker bytes (causes type confusion/deserialization failure)
    Marker,
    /// Corrupt random bytes in the middle (original behavior)
    Random,
}

/// Field location information for targeted corruption.
/// Based on Borsh serialization layout after the message envelope.
struct FieldLocation {
    /// Starting byte offset from the beginning of the ConsensusNetMessage
    start: usize,
    /// Length of the field in bytes
    len: usize,
}

impl FieldLocation {
    const fn new(start: usize, len: usize) -> Self {
        Self { start, len }
    }
}

/// Get the byte ranges for critical fields based on message type.
/// These are approximate locations based on Borsh serialization format.
///
/// Borsh format notes:
/// - Enum variant tag: 1 byte at the start
/// - u64: 8 bytes little-endian
/// - String: 4 bytes length (u32 LE) + UTF-8 data
/// - Vec<T>: 4 bytes length + serialized items
/// - Signature (BLS): ~96 bytes
/// - ValidatorPublicKey: ~48 bytes
fn get_corruption_targets(
    msg: &ConsensusNetMessage,
    strategy: CorruptionStrategy,
) -> Vec<FieldLocation> {
    // Byte 0 is the enum variant tag
    const VARIANT_TAG: usize = 0;
    const AFTER_TAG: usize = 1;

    // BLS signature size
    const SIGNATURE_SIZE: usize = 96;
    // Public key size
    const PUBKEY_SIZE: usize = 48;
    // u64 size
    const U64_SIZE: usize = 8;
    // String length prefix
    const STRING_LEN_PREFIX: usize = 4;

    match (msg, strategy) {
        // ============================================================
        // Prepare(ConsensusProposal, Ticket, View)
        // ConsensusProposal layout: slot(u64) + parent_hash(String) + cut(Vec) + staking_actions(Vec) + timestamp(u64)
        // ============================================================
        (ConsensusNetMessage::Prepare(proposal, ..), CorruptionStrategy::SlotView) => {
            // Slot is at byte 1 (after variant tag), 8 bytes
            vec![FieldLocation::new(AFTER_TAG, U64_SIZE)]
        }
        (ConsensusNetMessage::Prepare(proposal, ..), CorruptionStrategy::Hash) => {
            // parent_hash starts after slot (byte 9), has 4-byte length prefix + hash string
            let hash_len = proposal.parent_hash.0.len();
            vec![FieldLocation::new(
                AFTER_TAG + U64_SIZE + STRING_LEN_PREFIX,
                hash_len.min(32),
            )]
        }
        (ConsensusNetMessage::Prepare(..), CorruptionStrategy::Timestamp) => {
            // Timestamp is harder to locate precisely due to variable-length cut/staking_actions
            // Use random corruption as fallback for timestamp
            vec![FieldLocation::new(50, 8)] // Approximate location
        }

        // ============================================================
        // PrepareVote(SignedByValidator<(ConsensusProposalHash, PrepareVoteMarker)>)
        // Layout: msg(hash + marker) + signature(ValidatorSignature)
        // ============================================================
        (ConsensusNetMessage::PrepareVote(vote), CorruptionStrategy::Signature) => {
            // Signature is in ValidatorSignature at the end
            // ValidatorSignature = signature(Signature) + validator(ValidatorPublicKey)
            let hash_len = vote.msg.0 .0.len();
            let sig_offset = AFTER_TAG + STRING_LEN_PREFIX + hash_len + 1; // +1 for marker
            vec![FieldLocation::new(sig_offset, SIGNATURE_SIZE)]
        }
        (ConsensusNetMessage::PrepareVote(vote), CorruptionStrategy::Hash) => {
            // ConsensusProposalHash is at start of msg
            let hash_len = vote.msg.0 .0.len();
            vec![FieldLocation::new(
                AFTER_TAG + STRING_LEN_PREFIX,
                hash_len.min(32),
            )]
        }
        (ConsensusNetMessage::PrepareVote(..), CorruptionStrategy::Marker) => {
            // Marker is 1 byte after the hash
            // This would cause type confusion (PrepareVote vs ConfirmAck)
            vec![FieldLocation::new(AFTER_TAG + STRING_LEN_PREFIX + 32, 1)]
        }

        // ============================================================
        // Confirm(PrepareQC, ConsensusProposalHash)
        // PrepareQC = QuorumCertificate<PrepareVoteMarker> = AggregateSignature + marker
        // AggregateSignature = signature + validators(Vec<ValidatorPublicKey>)
        // ============================================================
        (ConsensusNetMessage::Confirm(qc, hash), CorruptionStrategy::Signature) => {
            // Aggregate signature is at start of PrepareQC
            vec![FieldLocation::new(
                AFTER_TAG + STRING_LEN_PREFIX,
                SIGNATURE_SIZE,
            )]
        }
        (ConsensusNetMessage::Confirm(_, hash), CorruptionStrategy::Hash) => {
            // Hash is at end of message, but variable position
            // Corrupt the hash hint
            vec![FieldLocation::new(200, hash.0.len().min(32))] // Approximate
        }
        (ConsensusNetMessage::Confirm(..), CorruptionStrategy::Marker) => {
            // Marker byte distinguishes PrepareQC from CommitQC
            vec![FieldLocation::new(
                AFTER_TAG + STRING_LEN_PREFIX + SIGNATURE_SIZE + 4,
                1,
            )]
        }

        // ============================================================
        // ConfirmAck(SignedByValidator<(ConsensusProposalHash, ConfirmAckMarker)>)
        // Same structure as PrepareVote but with different marker
        // ============================================================
        (ConsensusNetMessage::ConfirmAck(ack), CorruptionStrategy::Signature) => {
            let hash_len = ack.msg.0 .0.len();
            let sig_offset = AFTER_TAG + STRING_LEN_PREFIX + hash_len + 1;
            vec![FieldLocation::new(sig_offset, SIGNATURE_SIZE)]
        }
        (ConsensusNetMessage::ConfirmAck(ack), CorruptionStrategy::Hash) => {
            let hash_len = ack.msg.0 .0.len();
            vec![FieldLocation::new(
                AFTER_TAG + STRING_LEN_PREFIX,
                hash_len.min(32),
            )]
        }
        (ConsensusNetMessage::ConfirmAck(..), CorruptionStrategy::Marker) => {
            vec![FieldLocation::new(AFTER_TAG + STRING_LEN_PREFIX + 32, 1)]
        }

        // ============================================================
        // Commit(CommitQC, ConsensusProposalHash)
        // CommitQC = QuorumCertificate<ConfirmAckMarker>
        // ============================================================
        (ConsensusNetMessage::Commit(..), CorruptionStrategy::Signature) => {
            // CommitQC aggregate signature
            vec![FieldLocation::new(
                AFTER_TAG + STRING_LEN_PREFIX,
                SIGNATURE_SIZE,
            )]
        }
        (ConsensusNetMessage::Commit(_, hash), CorruptionStrategy::Hash) => {
            vec![FieldLocation::new(200, hash.0.len().min(32))]
        }
        (ConsensusNetMessage::Commit(..), CorruptionStrategy::Marker) => {
            vec![FieldLocation::new(
                AFTER_TAG + STRING_LEN_PREFIX + SIGNATURE_SIZE + 4,
                1,
            )]
        }

        // ============================================================
        // Timeout((SignedByValidator<(Slot, View, ParentHash, TimeoutMarker)>, TimeoutKind))
        // ============================================================
        (ConsensusNetMessage::Timeout(..), CorruptionStrategy::SlotView) => {
            // Slot and View are at start of signed message
            vec![
                FieldLocation::new(AFTER_TAG, U64_SIZE),            // Slot
                FieldLocation::new(AFTER_TAG + U64_SIZE, U64_SIZE), // View
            ]
        }
        (ConsensusNetMessage::Timeout(..), CorruptionStrategy::Signature) => {
            // Signature follows the signed message content
            vec![FieldLocation::new(
                AFTER_TAG + U64_SIZE * 2 + STRING_LEN_PREFIX + 64,
                SIGNATURE_SIZE,
            )]
        }
        (ConsensusNetMessage::Timeout(..), CorruptionStrategy::Hash) => {
            // Parent hash in timeout
            vec![FieldLocation::new(
                AFTER_TAG + U64_SIZE * 2 + STRING_LEN_PREFIX,
                32,
            )]
        }

        // ============================================================
        // TimeoutCertificate(TimeoutQC, TCKind, Slot, View)
        // ============================================================
        (ConsensusNetMessage::TimeoutCertificate(..), CorruptionStrategy::Signature) => {
            // TimeoutQC aggregate signature
            vec![FieldLocation::new(
                AFTER_TAG + STRING_LEN_PREFIX,
                SIGNATURE_SIZE,
            )]
        }
        (ConsensusNetMessage::TimeoutCertificate(..), CorruptionStrategy::SlotView) => {
            // Slot and View at end
            vec![FieldLocation::new(300, U64_SIZE * 2)] // Approximate
        }

        // ============================================================
        // Default: Random corruption for unmatched combinations
        // ============================================================
        _ => vec![],
    }
}

/// Smart corruption function that targets specific critical fields.
fn corrupt_smart_bytes(
    rng: &mut StdRng,
    corruption_probability: f64,
    msg: &ConsensusNetMessage,
    bytes: &[u8],
    strategy: CorruptionStrategy,
) -> Option<Bytes> {
    if !rng.random_bool(corruption_probability) {
        return None;
    }

    let targets = get_corruption_targets(msg, strategy);

    if targets.is_empty() || strategy == CorruptionStrategy::Random {
        // Fall back to random corruption
        return corrupt_random_bytes(rng, 1.0, bytes);
    }

    let mut corrupted = bytes.to_vec();

    // Choose one of the target fields to corrupt
    let target = &targets[rng.random_range(0..targets.len())];

    // Ensure we don't go out of bounds
    if target.start >= corrupted.len() {
        // Fall back to random corruption if offset is invalid
        return corrupt_random_bytes(rng, 1.0, bytes);
    }

    let end = (target.start + target.len).min(corrupted.len());
    let actual_len = end - target.start;

    if actual_len == 0 {
        return corrupt_random_bytes(rng, 1.0, bytes);
    }

    // Corrupt 1-3 bytes within the target field
    let num_corruptions = rng.random_range(1..=actual_len.min(3));
    for _ in 0..num_corruptions {
        let pos = target.start + rng.random_range(0..actual_len);
        corrupted[pos] ^= rng.random_range(1u8..=255u8);
    }

    let msg_type: &'static str = msg.into();
    tracing::debug!(
        "🔴 Smart corruption: {} - {:?} strategy, corrupted {} bytes at offset {}",
        msg_type,
        strategy,
        num_corruptions,
        target.start
    );

    Some(Bytes::from(corrupted))
}

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

/// **Simulation - Smart Corruption**
///
/// Corrupt consensus messages using smart field-targeted corruption.
/// This targets critical fields based on message type to cause specific failure modes.
pub fn simulation_corrupt_consensus_messages(
    _ctx: &mut TurmoilCtx,
    sim: &mut Sim<'_>,
) -> anyhow::Result<()> {
    let seed = std::env::var("HYLI_TURMOIL_SEED")
        .ok()
        .and_then(|seed| seed.parse::<u64>().ok())
        .unwrap_or(0);
    let rng = std::sync::Arc::new(std::sync::Mutex::new(StdRng::seed_from_u64(seed)));

    let _corrupter = install_consensus_message_corrupter(move |message, bytes| {
        let mut rng = rng.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

        // Choose a corruption strategy based on the message type
        let strategy = match &message.msg {
            ConsensusNetMessage::Prepare(..) => {
                // For Prepare, corrupt slot/view or hash
                if rng.random_bool(0.5) {
                    CorruptionStrategy::SlotView
                } else {
                    CorruptionStrategy::Hash
                }
            }
            ConsensusNetMessage::PrepareVote(..) | ConsensusNetMessage::ConfirmAck(..) => {
                // For votes, corrupt signature or hash
                if rng.random_bool(0.7) {
                    CorruptionStrategy::Signature
                } else {
                    CorruptionStrategy::Hash
                }
            }
            ConsensusNetMessage::Confirm(..) | ConsensusNetMessage::Commit(..) => {
                // For QC messages, corrupt signature or marker
                if rng.random_bool(0.8) {
                    CorruptionStrategy::Signature
                } else {
                    CorruptionStrategy::Marker
                }
            }
            ConsensusNetMessage::Timeout(..) => {
                // For timeout, corrupt slot/view or signature
                if rng.random_bool(0.5) {
                    CorruptionStrategy::SlotView
                } else {
                    CorruptionStrategy::Signature
                }
            }
            ConsensusNetMessage::TimeoutCertificate(..) => CorruptionStrategy::Signature,
            _ => CorruptionStrategy::Random,
        };

        // Corrupt 7% of consensus messages
        corrupt_smart_bytes(&mut rng, 0.07, &message.msg, bytes, strategy)
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

/// **Simulation - Smart Corruption**
///
/// Corrupt only Commit messages sent by the leader.
///
/// This tests a critical edge case in the consensus protocol:
/// - The leader successfully forms a CommitQC and broadcasts the Commit message
/// - However, followers don't receive the valid Commit message (corrupted in transit)
/// - The leader advances to the next slot thinking the block is committed
/// - Followers are stuck waiting for the Commit message
///
/// Expected behavior:
/// 1. Followers timeout waiting for the next round's Prepare
/// 2. View change occurs, a new leader is elected
/// 3. The new leader re-proposes (potentially with the highest seen PrepareQC)
/// 4. System recovers and continues consensus
///
/// This test verifies:
/// - Safety: No chain divergence between nodes
/// - Liveness: System recovers through timeout mechanism
/// - Consistency: All nodes eventually converge to the same state
pub fn simulation_corrupt_commit_messages(
    _ctx: &mut TurmoilCtx,
    sim: &mut Sim<'_>,
) -> anyhow::Result<()> {
    let seed = std::env::var("HYLI_TURMOIL_SEED")
        .ok()
        .and_then(|seed| seed.parse::<u64>().ok())
        .unwrap_or(0);
    let rng = std::sync::Arc::new(std::sync::Mutex::new(StdRng::seed_from_u64(seed)));

    let _corrupter = install_consensus_message_corrupter(move |message, bytes| {
        // Only corrupt Commit messages
        if matches!(message.msg, ConsensusNetMessage::Commit(..)) {
            let mut rng = rng.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            // Corrupt 10% of Commit messages using smart signature corruption
            if rng.random_bool(0.1) {
                tracing::warn!("🔴 Smart corrupting Commit message (targeting signature)");
                return corrupt_smart_bytes(
                    &mut rng,
                    1.0,
                    &message.msg,
                    bytes,
                    CorruptionStrategy::Signature,
                );
            }
        }
        None
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

/// **Simulation - Smart Corruption**
///
/// Corrupt only PrepareVote messages to prevent quorum formation.
///
/// This tests the system's ability to handle vote loss/corruption:
/// - Followers send PrepareVote messages to the leader
/// - Some votes are corrupted, causing signature validation failures
/// - Leader cannot form PrepareQC (doesn't reach 2f+1 threshold)
///
/// Expected behavior:
/// 1. Leader fails to reach quorum for PrepareQC
/// 2. All nodes timeout
/// 3. View change occurs, new leader is elected
/// 4. System recovers with new proposal
pub fn simulation_corrupt_prepare_votes(
    _ctx: &mut TurmoilCtx,
    sim: &mut Sim<'_>,
) -> anyhow::Result<()> {
    let seed = std::env::var("HYLI_TURMOIL_SEED")
        .ok()
        .and_then(|seed| seed.parse::<u64>().ok())
        .unwrap_or(0);
    let rng = std::sync::Arc::new(std::sync::Mutex::new(StdRng::seed_from_u64(seed)));

    let _corrupter = install_consensus_message_corrupter(move |message, bytes| {
        if matches!(message.msg, ConsensusNetMessage::PrepareVote(..)) {
            let mut rng = rng.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            // Corrupt 30% of PrepareVote messages
            if rng.random_bool(0.3) {
                tracing::warn!("🔴 Smart corrupting PrepareVote message");
                return corrupt_smart_bytes(
                    &mut rng,
                    1.0,
                    &message.msg,
                    bytes,
                    CorruptionStrategy::Signature,
                );
            }
        }
        None
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

/// **Simulation - Smart Corruption**
///
/// Corrupt Timeout messages to stress the view change mechanism.
///
/// This tests resilience when timeout aggregation is disrupted:
/// - Validators send Timeout messages when they detect leader failure
/// - Some timeout messages are corrupted
/// - TimeoutQC formation may be delayed
///
/// Expected behavior:
/// 1. Corrupted timeouts are rejected
/// 2. System still reaches timeout quorum from valid messages
/// 3. View change eventually succeeds
/// 4. New leader continues consensus
pub fn simulation_corrupt_timeout_messages(
    _ctx: &mut TurmoilCtx,
    sim: &mut Sim<'_>,
) -> anyhow::Result<()> {
    let seed = std::env::var("HYLI_TURMOIL_SEED")
        .ok()
        .and_then(|seed| seed.parse::<u64>().ok())
        .unwrap_or(0);
    let rng = std::sync::Arc::new(std::sync::Mutex::new(StdRng::seed_from_u64(seed)));

    let _corrupter = install_consensus_message_corrupter(move |message, bytes| {
        if matches!(
            message.msg,
            ConsensusNetMessage::Timeout(..) | ConsensusNetMessage::TimeoutCertificate(..)
        ) {
            let mut rng = rng.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            // Corrupt 20% of timeout-related messages
            if rng.random_bool(0.2) {
                let strategy = if rng.random_bool(0.5) {
                    CorruptionStrategy::SlotView
                } else {
                    CorruptionStrategy::Signature
                };
                tracing::warn!("🔴 Smart corrupting Timeout message ({:?})", strategy);
                return corrupt_smart_bytes(&mut rng, 1.0, &message.msg, bytes, strategy);
            }
        }
        None
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
