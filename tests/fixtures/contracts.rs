use hyle_contract_sdk::ProgramId;
use hyle_contract_sdk::StateCommitment;
use hyle_contract_sdk::Verifier;
use hyle_contract_sdk::ZkContract;
use hyllar::Hyllar;

use super::ctx::E2EContract;

#[allow(dead_code)]
pub struct HyllarTestContract {}

impl HyllarTestContract {
    #[allow(dead_code)]
    pub fn init_state() -> Hyllar {
        hyllar::Hyllar::default()
    }
}

impl E2EContract for HyllarTestContract {
    fn verifier() -> Verifier {
        hyle_model::verifiers::RISC0_1.into()
    }

    fn program_id() -> ProgramId {
        hyle_contracts::HYLLAR_ID.to_vec().into()
    }

    fn state_commitment() -> StateCommitment {
        HyllarTestContract::init_state().commit()
    }
}

#[allow(dead_code)]
pub struct AmmTestContract {}

impl E2EContract for AmmTestContract {
    fn verifier() -> Verifier {
        hyle_model::verifiers::RISC0_1.into()
    }

    fn program_id() -> ProgramId {
        hyle_contracts::AMM_ID.to_vec().into()
    }

    fn state_commitment() -> StateCommitment {
        amm::Amm::default().commit()
    }
}
