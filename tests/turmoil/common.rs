//! Common utilities and helpers for turmoil tests.

use std::time::Duration;

use hyli_model::{
    BlobTransaction, ContractName, ProgramId, RegisterContractAction, StateCommitment,
};
use hyli_net::net::Sim;

use crate::fixtures::turmoil::TurmoilCtx;

/// Create a register contract transaction for testing.
pub fn make_register_contract_tx(name: ContractName) -> BlobTransaction {
    let register_contract_action = RegisterContractAction {
        verifier: "test".into(),
        program_id: ProgramId(vec![]),
        state_commitment: StateCommitment(vec![0, 1, 2, 3]),
        contract_name: name.clone(),
        constructor_metadata: Some(vec![1]),
        ..Default::default()
    };
    BlobTransaction::new(
        "hyli@hyli",
        vec![
            register_contract_action.as_blob("hyli".into()),
            register_contract_action.as_blob(name),
        ],
    )
}

/// Assert that all nodes in the cluster have converged to at least `min_height`.
pub fn assert_converged(
    ctx: &TurmoilCtx,
    sim: &mut Sim<'_>,
    min_height: u64,
) -> anyhow::Result<()> {
    let ctx_clone = ctx.clone();

    sim.client("convergence", async move {
        ctx_clone
            .assert_cluster_converged(min_height)
            .await
            .expect("cluster converged");
        Ok(())
    });

    loop {
        let finished = sim.step().map_err(|s| anyhow::anyhow!(s.to_string()))?;
        if finished {
            break;
        }
    }

    Ok(())
}

/// Assert that all nodes converge to at least `min_height` within 1 block.
pub fn assert_converged_with_one_block_height_tolerance(
    ctx: &TurmoilCtx,
    sim: &mut Sim<'_>,
    min_height: u64,
) -> anyhow::Result<()> {
    let ctx_clone = ctx.clone();

    sim.client("convergence", async move {
        ctx_clone
            .assert_cluster_converged_with_max_delta(min_height, 1)
            .await
            .expect("cluster converged with 1-block tolerance");
        Ok(())
    });

    loop {
        let finished = sim.step().map_err(|s| anyhow::anyhow!(s.to_string()))?;
        if finished {
            break;
        }
    }

    Ok(())
}

/// Configuration for holding messages between two nodes.
pub struct HoldConfiguration {
    pub from: String,
    pub to: String,
    pub when: Duration,
    pub duration: Duration,
    pub triggered: bool,
    pub released: bool,
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
            released: false,
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

        if current_time > self.when + self.duration && self.triggered && !self.released {
            tracing::error!("RELEASE TRIGGERED from {} to {}", self.from, self.to);
            sim.release(self.from.clone(), self.to.clone());
            self.released = true;
        }

        Ok(())
    }
}
