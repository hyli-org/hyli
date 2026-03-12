use alloc::{string::String, vec::Vec};
use borsh::{BorshDeserialize, BorshSerialize};

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct VerifyRequest {
    pub verifier: String,
    pub proof: Vec<u8>,
    pub program_id: Vec<u8>,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct VerifyResponse {
    pub ok: bool,
    pub outputs: Vec<u8>,
    pub error: String,
}
