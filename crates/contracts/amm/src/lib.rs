use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

use borsh::{BorshDeserialize, BorshSerialize};
use hyllar::HyllarAction;
use sdk::utils::parse_calldata;
use sdk::{Blob, BlobIndex, Calldata, ContractAction, RunResult, StateCommitment, ZkContract};
use sdk::{BlobData, ContractName, StructuredBlobData};
use serde::{Deserialize, Serialize};

#[cfg(feature = "client")]
pub mod client;

type TokenPair = (String, String);
type TokenPairAmount = (u128, u128);

#[derive(
    Debug, Serialize, Deserialize, Clone, BorshSerialize, BorshDeserialize, Ord, PartialOrd,
)]
pub struct UnorderedTokenPair {
    a: String,
    b: String,
}

impl UnorderedTokenPair {
    pub fn new(x: String, y: String) -> Self {
        if x <= y {
            UnorderedTokenPair { a: x, b: y }
        } else {
            UnorderedTokenPair { a: y, b: x }
        }
    }
}

impl PartialEq for UnorderedTokenPair {
    fn eq(&self, other: &Self) -> bool {
        self.a == other.a && self.b == other.b
    }
}

impl Eq for UnorderedTokenPair {}

impl Hash for UnorderedTokenPair {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.a.hash(state);
        self.b.hash(state);
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, BorshSerialize, BorshDeserialize, Default)]
pub struct Amm {
    pairs: BTreeMap<UnorderedTokenPair, TokenPairAmount>,
}

impl ZkContract for Amm {
    fn execute(&mut self, calldata: &Calldata) -> RunResult {
        let (action, mut execution_ctx) = parse_calldata::<AmmAction>(calldata)?;
        let output = match action {
            AmmAction::Swap {
                pair,
                amounts: (from_amount, to_amount),
            } => {
                // Check that a blob for the transfer exists for first token in swap
                execution_ctx.is_in_callee_blobs(
                    &ContractName(pair.0.clone()),
                    HyllarAction::TransferFrom {
                        owner: execution_ctx.caller.0.clone(),
                        recipient: execution_ctx.contract_name.0.clone(),
                        amount: from_amount,
                    },
                )?;
                // Check that a blob for the transfer exists for second token in swap
                execution_ctx.is_in_callee_blobs(
                    &ContractName(pair.1.clone()),
                    HyllarAction::Transfer {
                        recipient: execution_ctx.caller.0.clone(),
                        amount: to_amount,
                    },
                )?;
                self.verify_swap(pair, from_amount, to_amount)
            }
            AmmAction::NewPair { pair, amounts } => {
                // Check that a blob for the transfer exists for first token in pair
                execution_ctx.is_in_callee_blobs(
                    &ContractName(pair.0.clone()),
                    HyllarAction::TransferFrom {
                        owner: execution_ctx.caller.0.clone(),
                        recipient: execution_ctx.contract_name.0.clone(),
                        amount: amounts.0,
                    },
                )?;
                // Check that a blob for the transfer exists for second token in pair
                execution_ctx.is_in_callee_blobs(
                    &ContractName(pair.1.clone()),
                    HyllarAction::TransferFrom {
                        owner: execution_ctx.caller.0.clone(),
                        recipient: execution_ctx.contract_name.0.clone(),
                        amount: amounts.1,
                    },
                )?;
                self.create_new_pair(pair, amounts)
            }
        };
        match output {
            Err(e) => Err(e),
            Ok(output) => Ok((output.into_bytes(), execution_ctx, vec![])),
        }
    }

    fn commit(&self) -> sdk::StateCommitment {
        sdk::StateCommitment(self.as_bytes())
    }
}

impl Amm {
    pub fn new(pairs: BTreeMap<UnorderedTokenPair, TokenPairAmount>) -> Self {
        Amm { pairs }
    }

    pub fn get_paired_amount(
        &self,
        token_a: String,
        token_b: String,
        amount_a: u128,
    ) -> Option<u128> {
        let pair = UnorderedTokenPair::new(token_a, token_b);
        if let Some((k, x, y)) = self.pairs.get(&pair).map(|(x, y)| (x * y, *x, *y)) {
            let amount_b = y - (k / (x + amount_a));
            return Some(amount_b);
        }
        None
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        borsh::to_vec(self).expect("Failed to encode AmmState")
    }

    pub fn create_new_pair(
        &mut self,
        pair: (String, String),
        amounts: TokenPairAmount,
    ) -> Result<String, String> {
        // Check that new pair is about two different tokens and that there is one blob for each
        if pair.0 == pair.1 {
            return Err("Swap can only happen between two different tokens".to_string());
        }

        let normalized_pair = UnorderedTokenPair::new(pair.0, pair.1);

        if self.pairs.contains_key(&normalized_pair) {
            return Err(format!("Pair {:?} already exists", normalized_pair));
        }

        let program_outputs = format!("Pair {:?} created", normalized_pair);

        self.pairs.insert(normalized_pair, amounts);

        Ok(program_outputs)
    }

    pub fn verify_swap(
        &mut self,
        pair: TokenPair,
        from_amount: u128,
        to_amount: u128,
    ) -> Result<String, String> {
        // Check that swap is only about two different tokens
        if pair.0 == pair.1 {
            return Err("Swap can only happen between two different tokens".to_string());
        }

        // Compute x,y and check swap is legit (x*y=k)
        let normalized_pair = UnorderedTokenPair::new(pair.0.clone(), pair.1.clone());
        let is_normalized_order = pair.0 <= pair.1;
        let Some((prev_x, prev_y)) = self.pairs.get_mut(&normalized_pair) else {
            return Err(format!("Pair {:?} not found in AMM state", pair));
        };
        let expected_to_amount = if is_normalized_order {
            let amount = *prev_y - (*prev_x * *prev_y / (*prev_x + from_amount));
            *prev_x += from_amount;
            *prev_y -= amount; // we need to remove the full amount to avoid slipping
            amount
        } else {
            let amount = *prev_x - (*prev_y * *prev_x / (*prev_y + from_amount));
            *prev_y += from_amount;
            *prev_x -= amount; // we need to remove the full amount to avoid slipping
            amount
        };

        // Assert that we transferred less than that, within 2%
        if to_amount > expected_to_amount || to_amount < expected_to_amount * 98 / 100 {
            return Err(format!(
                "Invalid swap: expected to receive between {} and {} {}",
                expected_to_amount * 100 / 102,
                expected_to_amount,
                pair.1
            ));
        }

        // At this point the contract has effectively taken some fees but we don't actually count them.
        Ok(format!(
            "Swap of {} {} for {} {} is valid",
            from_amount, pair.0, to_amount, pair.1
        ))
    }
}

impl TryFrom<StateCommitment> for Amm {
    type Error = anyhow::Error;

    fn try_from(state: StateCommitment) -> Result<Self, Self::Error> {
        borsh::from_slice(&state.0).map_err(|_| anyhow::anyhow!("Could not decode amm state"))
    }
}

/// Enum representing the actions that can be performed by the Amm state.
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
pub enum AmmAction {
    Swap {
        pair: TokenPair, // User swaps the first token of the pair for the second token
        amounts: TokenPairAmount,
    },
    NewPair {
        pair: TokenPair,
        amounts: TokenPairAmount,
    },
}

impl ContractAction for AmmAction {
    fn as_blob(
        &self,
        contract_name: ContractName,
        caller: Option<BlobIndex>,
        callees: Option<Vec<BlobIndex>>,
    ) -> Blob {
        Blob {
            contract_name,
            data: BlobData::from(StructuredBlobData {
                caller,
                callees,
                parameters: self.clone(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn test_verify_swap_success() {
        let normalized_token_pair =
            UnorderedTokenPair::new("token1".to_string(), "token2".to_string());
        let mut state = Amm {
            pairs: BTreeMap::from([(normalized_token_pair.clone(), (20, 50))]),
        };
        println!(
            "default state: {:?}",
            Amm {
                pairs: BTreeMap::default(),
            }
            .commit()
        );

        let result = state.verify_swap(("token1".to_string(), "token2".to_string()), 5, 10);
        assert!(result.is_ok());
        // Assert that the amounts for the pair token1/token2 have been updated
        assert_eq!(state.pairs.get(&normalized_token_pair), Some(&(25, 40)));
    }

    #[test]
    fn test_verify_opposite_swap_success() {
        let normalized_token_pair =
            UnorderedTokenPair::new("token1".to_string(), "token2".to_string());
        let mut state = Amm {
            pairs: BTreeMap::from([(normalized_token_pair.clone(), (20, 50))]),
        };

        let result = state.verify_swap(("token2".to_string(), "token1".to_string()), 50, 10);

        assert!(result.is_ok());
        // Assert that the amounts for the pair token1/token2 have been updated
        println!("{:?}", state.pairs.get(&normalized_token_pair));
        assert!(state.pairs.get(&normalized_token_pair) == Some(&(10, 100)));
    }

    #[test]
    fn test_verify_swap_success_with_slippage() {
        let normalized_token_pair =
            UnorderedTokenPair::new("token1".to_string(), "token2".to_string());
        let mut state = Amm {
            pairs: BTreeMap::from([(normalized_token_pair.clone(), (2000, 5000))]),
        };
        println!(
            "default state: {:?}",
            Amm {
                pairs: BTreeMap::default(),
            }
            .commit()
        );

        let result = state.verify_swap(("token1".to_string(), "token2".to_string()), 500, 980);
        assert!(result.is_ok());
        assert_eq!(
            state.pairs.get(&normalized_token_pair),
            Some(&(2500, 4000)) // 20 of token2 taken as 'fees'
        );
    }

    #[test]
    fn test_verify_swap_invalid_pair() {
        let normalized_token_pair =
            UnorderedTokenPair::new("token1".to_string(), "token2".to_string());
        let mut state = Amm {
            pairs: BTreeMap::from([(normalized_token_pair.clone(), (20, 50))]),
        };

        let result = state.verify_swap(
            ("token1".to_string(), "rubbish".to_string()), // Invalid pair
            5,
            10,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_verify_swap_invalid_swap_formula() {
        let normalized_token_pair =
            UnorderedTokenPair::new("token1".to_string(), "token2".to_string());
        let mut state = Amm {
            pairs: BTreeMap::from([(normalized_token_pair.clone(), (20, 50))]),
        };

        let result = state.verify_swap(("token1".to_string(), "token2".to_string()), 0, 50);
        assert!(result.is_err());
        assert!(result
            .err()
            .unwrap()
            .contains("Invalid swap: expected to receive"));
    }

    #[test]
    fn test_create_new_pair_success() {
        let mut state = Amm {
            pairs: BTreeMap::new(),
        };

        let result = state.create_new_pair(("token1".to_string(), "token2".to_string()), (20, 50));

        println!("result: {:?}", result);
        assert!(result.is_ok());
        let normalized_token_pair =
            UnorderedTokenPair::new("token1".to_string(), "token2".to_string());
        assert!(state.pairs.get(&normalized_token_pair) == Some(&(20, 50)));
    }

    #[test]
    fn test_create_new_pair_already_exists() {
        let normalized_token_pair =
            UnorderedTokenPair::new("token1".to_string(), "token2".to_string());
        let mut state = Amm {
            pairs: BTreeMap::from([(
                UnorderedTokenPair::new("token1".to_string(), "token2".to_string()),
                (100, 200),
            )]),
        };

        let result = state.create_new_pair(("token1".to_string(), "token2".to_string()), (20, 50));

        assert!(result.is_err());
        assert!(state.pairs.get(&normalized_token_pair) == Some(&(100, 200)));
    }

    #[test]
    fn test_create_new_pair_same_tokens() {
        let mut state = Amm {
            pairs: BTreeMap::new(),
        };

        let result = state.create_new_pair(
            ("token1".to_string(), "token1".to_string()), // same tokens
            (20, 50),
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_get_paired_amount_existing_pair() {
        let pair = UnorderedTokenPair::new("token1".into(), "token2".into());
        let mut pairs = BTreeMap::new();
        pairs.insert(pair.clone(), (10, 20));
        let state = Amm { pairs };

        let result = state.get_paired_amount("token1".to_string(), "token2".to_string(), 5);

        assert!(result.is_some());
        let amount_b = result.unwrap();
        assert_eq!(amount_b, 20 - (10 * 20 / (10 + 5)));
    }

    #[test]
    fn test_get_paired_amount_non_existing_pair() {
        let state = Amm {
            pairs: BTreeMap::new(),
        };

        let result = state.get_paired_amount("token1".to_string(), "token2".to_string(), 5);

        assert!(result.is_none());
    }

    #[test]
    fn test_get_paired_amount_zero_amount_a() {
        let pair = UnorderedTokenPair::new("token1".into(), "token2".into());
        let mut pairs = BTreeMap::new();
        pairs.insert(pair.clone(), (10, 20));
        let state = Amm { pairs };

        let result = state.get_paired_amount("token1".to_string(), "token2".to_string(), 0);

        assert!(result.is_some());
        let amount_b = result.unwrap();
        assert_eq!(amount_b, 20 - (10 * 20 / 10));
    }

    #[test]
    fn test_get_paired_amount_division_by_zero() {
        let pair = UnorderedTokenPair::new("token1".into(), "token2".into());
        let mut pairs = BTreeMap::new();
        pairs.insert(pair.clone(), (0, 20));
        let state = Amm { pairs };

        let result = state.get_paired_amount("token1".to_string(), "token2".to_string(), 5);

        assert!(result.is_some());
        let amount_b = result.unwrap();
        assert_eq!(amount_b, 20);
    }
}
