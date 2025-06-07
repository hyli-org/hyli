use std::{
    fmt::Display,
    ops::{Add, Deref, DerefMut, Sub},
};

use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

use crate::{utils::TimestampMs, LaneId};

#[derive(
    Debug,
    Serialize,
    Deserialize,
    Clone,
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Default,
    Ord,
    PartialOrd,
)]
#[cfg_attr(feature = "full", derive(utoipa::ToSchema))]
pub struct ConsensusProposalHash(pub String);
pub type BlockHash = ConsensusProposalHash;

impl std::hash::Hash for ConsensusProposalHash {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write(self.0.as_bytes());
    }
}

pub trait Hashed<T> {
    fn hashed(&self) -> T;
}

pub trait DataSized {
    fn estimate_size(&self) -> usize;
}

/// Blob of the transactions the contract uses to validate its transition
#[derive(
    Default,
    Serialize,
    Deserialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    BorshSerialize,
    BorshDeserialize,
)]
#[cfg_attr(feature = "full", derive(utoipa::ToSchema))]
pub struct IndexedBlobs(pub Vec<(BlobIndex, Blob)>);

impl Deref for IndexedBlobs {
    type Target = Vec<(BlobIndex, Blob)>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for IndexedBlobs {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl IndexedBlobs {
    pub fn get(&self, index: &BlobIndex) -> Option<&Blob> {
        self.0.iter().find(|(i, _)| i == index).map(|(_, b)| b)
    }
}

impl<T> From<T> for IndexedBlobs
where
    T: IntoIterator<Item = Blob>,
{
    fn from(vec: T) -> Self {
        let mut blobs = IndexedBlobs::default();
        for (i, blob) in vec.into_iter().enumerate() {
            blobs.push((BlobIndex(i), blob));
        }
        blobs
    }
}

impl<'a> IntoIterator for &'a IndexedBlobs {
    type Item = &'a (BlobIndex, Blob);
    type IntoIter = std::slice::Iter<'a, (BlobIndex, Blob)>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

/// This struct is passed from the application backend to the contract as an input.
/// It contains the data that the contract will use to run the blob's action on its state.
#[derive(Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct Calldata {
    /// TxHash of the BlobTransaction being proved
    pub tx_hash: TxHash,
    /// User's identity used for the BlobTransaction
    pub identity: Identity,
    /// Subset of [Blob]s of the BlobTransaction
    pub blobs: IndexedBlobs,
    /// Number of ALL blobs in the transaction. tx_blob_count >= blobs.len()
    pub tx_blob_count: usize,
    /// Index of the blob corresponding to the contract.
    /// The [Blob] referenced by this index has to be parsed by the contract
    pub index: BlobIndex,
    /// Optional additional context of the BlobTransaction
    pub tx_ctx: Option<TxContext>,
    /// Additional input for the contract that is not written on-chain in the BlobTransaction
    pub private_input: Vec<u8>,
}

/// State commitment of the contract.
#[derive(
    Default, Serialize, Deserialize, Clone, PartialEq, Eq, Hash, BorshSerialize, BorshDeserialize,
)]
#[cfg_attr(feature = "full", derive(utoipa::ToSchema))]
pub struct StateCommitment(pub Vec<u8>);

impl std::fmt::Debug for StateCommitment {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "StateCommitment({})", hex::encode(&self.0))
    }
}

#[derive(
    Default,
    Serialize,
    Deserialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    BorshSerialize,
    BorshDeserialize,
    Ord,
    PartialOrd,
)]
#[cfg_attr(feature = "full", derive(utoipa::ToSchema))]
/// An identity is a string that identifies the person that sent
/// the BlobTransaction
pub struct Identity(pub String);

#[derive(
    Default,
    Serialize,
    Deserialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    BorshSerialize,
    BorshDeserialize,
    Ord,
    PartialOrd,
)]
#[cfg_attr(feature = "full", derive(utoipa::ToSchema))]
pub struct TxHash(pub String);

#[derive(
    Default,
    Serialize,
    Deserialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    BorshDeserialize,
    BorshSerialize,
    Copy,
    PartialOrd,
    Ord,
)]
#[cfg_attr(feature = "full", derive(utoipa::ToSchema))]
pub struct BlobIndex(pub usize);

impl Add<usize> for BlobIndex {
    type Output = BlobIndex;
    fn add(self, other: usize) -> BlobIndex {
        BlobIndex(self.0 + other)
    }
}

#[derive(
    Default, Serialize, Deserialize, Clone, PartialEq, Eq, Hash, BorshSerialize, BorshDeserialize,
)]
#[cfg_attr(feature = "full", derive(utoipa::ToSchema))]
pub struct BlobData(pub Vec<u8>);

impl std::fmt::Debug for BlobData {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.0.len() > 20 {
            write!(f, "BlobData({}...)", hex::encode(&self.0[..20]))
        } else {
            write!(f, "BlobData({})", hex::encode(&self.0))
        }
    }
}

/**
This struct allows to define cross-contract calls (aka contract composition).
A contract `A` can "call" an other contract `B` by being it's "caller":

Blob for contract `A` has to be a `StructuredBlobData` with callees vec including the blob index of
contract `B`.

Blob for contract `B` has to be a `StructuredBlobData` with caller = the blob index of contract
`A`.

## When to use cross-contract calls ?

When a contract needs to do an operation on an other one. Like transferring funds from
contract's wallet to the user doing the transaction.

### Example: Bob Swap 2 USDC to 2 USDT

A swap contract can use transactions with 4 blobs:

```text
┌─ Blob 0
│  Identity verification for user Bob
└─────
┌─ Blob 1 - Contract = "amm"
│  Swap action
│  callees = vec![2]
└─────
┌─ Blob 2 - Contract = "usdt"
│  Transfer action of 2 USDT to "Bob"
│  caller = 1
└─────
┌─ Blob 3 - Contract = "usdc"
│  Transfer action of 2 USDC to "amm"
└─────
```

Blob 2 will do various checks on the swap to ensure its validity (correct transfer amounts...)

As Blob 2 has a "caller", the identity used by the contract will be "amm", thus the
transfer of USDT will be done FROM "amm" TO Bob

And as Blob 3 has no "caller", the identity used by the contract will be the same as the
transaction identity, i.e: Bob.


An alternative way that is more evm-like with an token approve would look like:
```text
┌─ Blob 0
│  Identity verification for user Bob
└─────
┌─ Blob 1 - Contract = "usdc"
│  Approve action of 2 USDC for "amm"
└─────
┌─ Blob 2 - Contract = "amm"
│  Swap action
│  callees = vec![3, 4]
└─────
┌─ Blob 3 - Contract = "usdt"
│  Transfer action of 2 USDT to "Bob"
│  caller = 2
└─────
┌─ Blob 4 - Contract = "usdc"
│  TransferFrom action from "Bob" of 2 USDC to "amm"
│  caller = 2
└─────
```

As Blob 4 now has a "caller", the identity used by the contract will be "amm" and not "Bob".
Note that here we are using a TransferFrom in blob 4, contract "amm" got the approval from Bob
to initate a transfer on its behalf with blob 1.

You can find an example of this implementation in our [amm contract](https://github.com/hyli-org/hyli/tree/main/crates/contracts/amm/src/lib.rs)
*/
#[derive(Debug, BorshSerialize)]
pub struct StructuredBlobData<Action> {
    pub caller: Option<BlobIndex>,
    pub callees: Option<Vec<BlobIndex>>,
    pub parameters: Action,
}

/// Struct used to be able to deserialize a StructuredBlobData
/// without knowing the concrete type of `Action`
/// warning: this will drop the end of the reader, thus, you can't
/// deserialize a structure that contains a `StructuredBlobData<DropEndOfReader>`
/// Unless this struct is at the end of your data structure.
/// It's not meant to be used outside the sdk internal logic.
pub struct DropEndOfReader;

impl<Action: BorshDeserialize> BorshDeserialize for StructuredBlobData<Action> {
    fn deserialize_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        let caller = Option::<BlobIndex>::deserialize_reader(reader)?;
        let callees = Option::<Vec<BlobIndex>>::deserialize_reader(reader)?;
        let parameters = Action::deserialize_reader(reader)?;
        Ok(StructuredBlobData {
            caller,
            callees,
            parameters,
        })
    }
}

impl BorshDeserialize for StructuredBlobData<DropEndOfReader> {
    fn deserialize_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        let caller = Option::<BlobIndex>::deserialize_reader(reader)?;
        let callees = Option::<Vec<BlobIndex>>::deserialize_reader(reader)?;
        reader.read_to_end(&mut vec![])?;
        let parameters = DropEndOfReader;
        Ok(StructuredBlobData {
            caller,
            callees,
            parameters,
        })
    }
}

impl<Action: BorshSerialize> From<StructuredBlobData<Action>> for BlobData {
    fn from(val: StructuredBlobData<Action>) -> Self {
        BlobData(borsh::to_vec(&val).expect("failed to encode BlobData"))
    }
}
impl<Action: BorshDeserialize> TryFrom<BlobData> for StructuredBlobData<Action> {
    type Error = std::io::Error;

    fn try_from(val: BlobData) -> Result<StructuredBlobData<Action>, Self::Error> {
        borsh::from_slice(&val.0)
    }
}
impl TryFrom<BlobData> for StructuredBlobData<DropEndOfReader> {
    type Error = std::io::Error;

    fn try_from(val: BlobData) -> Result<StructuredBlobData<DropEndOfReader>, Self::Error> {
        borsh::from_slice(&val.0)
    }
}

#[derive(
    Debug,
    Serialize,
    Deserialize,
    Default,
    Clone,
    PartialEq,
    Eq,
    BorshSerialize,
    BorshDeserialize,
    Hash,
)]
#[cfg_attr(feature = "full", derive(utoipa::ToSchema))]
/// A Blob is a binary-serialized action that the contract has to parse
/// An action is often written as an enum representing the call of a specific
/// contract function.
pub struct Blob {
    pub contract_name: ContractName,
    pub data: BlobData,
}

#[derive(
    Default,
    Debug,
    Clone,
    Serialize,
    Deserialize,
    Eq,
    PartialEq,
    Hash,
    BorshSerialize,
    BorshDeserialize,
)]
#[cfg_attr(feature = "full", derive(utoipa::ToSchema))]
pub struct BlobHash(pub String);

#[cfg(feature = "full")]
impl Hashed<BlobHash> for Blob {
    fn hashed(&self) -> BlobHash {
        use sha3::{Digest, Sha3_256};

        let mut hasher = Sha3_256::new();
        hasher.update(self.contract_name.0.clone());
        hasher.update(self.data.0.clone());
        let hash_bytes = hasher.finalize();
        BlobHash(hex::encode(hash_bytes))
    }
}

#[derive(Debug, BorshSerialize, BorshDeserialize)]
pub struct StructuredBlob<Action> {
    pub contract_name: ContractName,
    pub data: StructuredBlobData<Action>,
}

impl<Action: BorshSerialize> From<StructuredBlob<Action>> for Blob {
    fn from(val: StructuredBlob<Action>) -> Self {
        Blob {
            contract_name: val.contract_name,
            data: BlobData::from(val.data),
        }
    }
}

impl<Action: BorshDeserialize> TryFrom<Blob> for StructuredBlob<Action> {
    type Error = std::io::Error;

    fn try_from(val: Blob) -> Result<StructuredBlob<Action>, Self::Error> {
        let data = borsh::from_slice(&val.data.0)?;
        Ok(StructuredBlob {
            contract_name: val.contract_name,
            data,
        })
    }
}

pub trait ContractAction: Send {
    fn as_blob(
        &self,
        contract_name: ContractName,
        caller: Option<BlobIndex>,
        callees: Option<Vec<BlobIndex>>,
    ) -> Blob;
}

#[derive(
    Default,
    Debug,
    Clone,
    Serialize,
    Deserialize,
    Eq,
    PartialEq,
    Hash,
    BorshSerialize,
    BorshDeserialize,
    Ord,
    PartialOrd,
)]
#[cfg_attr(feature = "full", derive(utoipa::ToSchema))]
pub struct ContractName(pub String);

#[derive(
    Default,
    Debug,
    Clone,
    Serialize,
    Deserialize,
    Eq,
    PartialEq,
    Hash,
    BorshSerialize,
    BorshDeserialize,
)]
#[cfg_attr(feature = "full", derive(utoipa::ToSchema))]
pub struct Verifier(pub String);

#[derive(
    Default,
    Debug,
    Clone,
    Serialize,
    Deserialize,
    Eq,
    PartialEq,
    Hash,
    BorshSerialize,
    BorshDeserialize,
)]
#[cfg_attr(feature = "full", derive(utoipa::ToSchema))]
pub struct ProgramId(pub Vec<u8>);

#[derive(Debug, Default, PartialEq, Eq, Clone, BorshSerialize, BorshDeserialize)]
#[cfg_attr(feature = "full", derive(Serialize, utoipa::ToSchema))]
pub struct ProofData(#[cfg_attr(feature = "full", serde(with = "base64_field"))] pub Vec<u8>);

#[cfg(feature = "full")]
impl<'de> Deserialize<'de> for ProofData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ProofDataVisitor;

        impl<'de> serde::de::Visitor<'de> for ProofDataVisitor {
            type Value = ProofData;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a Base64 string or a Vec<u8>")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use base64::prelude::*;
                let decoded = BASE64_STANDARD
                    .decode(value)
                    .map_err(serde::de::Error::custom)?;
                Ok(ProofData(decoded))
            }

            fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let vec_u8: Vec<u8> = serde::de::Deserialize::deserialize(
                    serde::de::value::SeqAccessDeserializer::new(seq),
                )?;
                Ok(ProofData(vec_u8))
            }
        }

        deserializer.deserialize_any(ProofDataVisitor)
    }
}

#[derive(
    Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize,
)]
pub struct ProofDataHash(pub String);

#[cfg(feature = "full")]
impl Hashed<ProofDataHash> for ProofData {
    fn hashed(&self) -> ProofDataHash {
        use sha3::Digest;
        let mut hasher = sha3::Sha3_256::new();
        hasher.update(self.0.as_slice());
        let hash_bytes = hasher.finalize();
        ProofDataHash(hex::encode(hash_bytes))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
#[cfg_attr(feature = "full", derive(utoipa::ToSchema))]
/// Enum for various side-effects blobs can have on the chain.
/// This is implemented as an enum for easier forward compatibility.
pub enum OnchainEffect {
    RegisterContract(RegisterContractEffect),
    DeleteContract(ContractName),
}

/// This struct has to be the zkvm committed output. It will be used by
/// hyle node to verify & settle the blob transaction.
#[derive(
    Default, Serialize, Deserialize, Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize,
)]
#[cfg_attr(feature = "full", derive(utoipa::ToSchema))]
pub struct HyleOutput {
    /// The version of the HyleOutput. This is unchecked for now.
    pub version: u32,
    /// The initial state of the contract. This is the state before the transaction is executed.
    pub initial_state: StateCommitment,
    /// The state of the contract after the transaction is executed.
    pub next_state: StateCommitment,
    /// The identity used to execute the transaction.
    /// This must match the one used in the BlobTransaction.
    pub identity: Identity,

    /// The index of the blob being proven.
    pub index: BlobIndex,
    /// The blobs that were used by the contract. It has to be a subset of the transactions blobs
    /// It can be the complete list of blobs if the contract used all of them.
    pub blobs: IndexedBlobs,
    /// Number of blobs in the transaction. tx_blob_count >= blobs.len()
    pub tx_blob_count: usize,

    /// TxHash of the BlobTransaction.
    pub tx_hash: TxHash, // Technically redundant with identity + blobs hash

    /// Whether the execution was successful or not. If false, the BlobTransaction will be
    /// settled as failed.
    pub success: bool,

    /// List of other contracts used by the proof. Each state commitment will be checked
    /// against the contract's state when settling.
    pub state_reads: Vec<(ContractName, StateCommitment)>,

    /// Optional - if empty, these won't be checked, but also can't be used inside the program.
    pub tx_ctx: Option<TxContext>,

    pub onchain_effects: Vec<OnchainEffect>,

    /// Arbitrary data that could be used by indexers or other tools. Some contracts write a utf-8
    /// string here, but it can be anything.
    /// Note: As this data is generated by the contract in the zkvm, this can be trusted.
    pub program_outputs: Vec<u8>,
}

#[derive(
    Default,
    Serialize,
    Deserialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    BorshSerialize,
    BorshDeserialize,
)]
#[cfg_attr(feature = "full", derive(utoipa::ToSchema))]
pub struct TxContext {
    pub lane_id: LaneId,
    pub block_hash: BlockHash,
    pub block_height: BlockHeight,
    pub timestamp: TimestampMs,
    pub chain_id: u128,
}

impl Identity {
    pub fn new<S: Into<Self>>(s: S) -> Self {
        s.into()
    }
}
impl<S: Into<String>> From<S> for Identity {
    fn from(s: S) -> Self {
        Identity(s.into())
    }
}

impl TxHash {
    pub fn new<S: Into<Self>>(s: S) -> Self {
        s.into()
    }
}
impl<S: Into<String>> From<S> for TxHash {
    fn from(s: S) -> Self {
        TxHash(s.into())
    }
}

impl ContractName {
    pub fn new<S: Into<Self>>(s: S) -> Self {
        s.into()
    }
}
impl<S: Into<String>> From<S> for ContractName {
    fn from(s: S) -> Self {
        ContractName(s.into())
    }
}

impl Verifier {
    pub fn new<S: Into<Self>>(s: S) -> Self {
        s.into()
    }
}
impl<S: Into<String>> From<S> for Verifier {
    fn from(s: S) -> Self {
        Verifier(s.into())
    }
}
impl From<Vec<u8>> for ProgramId {
    fn from(v: Vec<u8>) -> Self {
        ProgramId(v.clone())
    }
}
impl From<&Vec<u8>> for ProgramId {
    fn from(v: &Vec<u8>) -> Self {
        ProgramId(v.clone())
    }
}
impl From<&[u8]> for ProgramId {
    fn from(v: &[u8]) -> Self {
        ProgramId(v.to_vec().clone())
    }
}

impl Display for TxHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0)
    }
}
impl Display for BlobIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0)
    }
}
impl Display for ContractName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0)
    }
}
impl Display for Identity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0)
    }
}
impl Display for Verifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0)
    }
}
impl From<usize> for BlobIndex {
    fn from(i: usize) -> Self {
        BlobIndex(i)
    }
}

#[cfg_attr(feature = "full", derive(derive_more::derive::Display))]
#[derive(
    Default,
    Debug,
    Clone,
    Serialize,
    Deserialize,
    Eq,
    PartialEq,
    Hash,
    Copy,
    BorshSerialize,
    BorshDeserialize,
    PartialOrd,
    Ord,
)]
#[cfg_attr(feature = "full", derive(utoipa::ToSchema))]
pub struct BlockHeight(pub u64);

impl Add<BlockHeight> for u64 {
    type Output = BlockHeight;
    fn add(self, other: BlockHeight) -> BlockHeight {
        BlockHeight(self + other.0)
    }
}

impl Add<u64> for BlockHeight {
    type Output = BlockHeight;
    fn add(self, other: u64) -> BlockHeight {
        BlockHeight(self.0 + other)
    }
}

impl Sub<u64> for BlockHeight {
    type Output = BlockHeight;
    fn sub(self, other: u64) -> BlockHeight {
        BlockHeight(self.0 - other)
    }
}

impl Add<BlockHeight> for BlockHeight {
    type Output = BlockHeight;
    fn add(self, other: BlockHeight) -> BlockHeight {
        BlockHeight(self.0 + other.0)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
#[cfg_attr(feature = "full", derive(utoipa::ToSchema))]
pub enum TimeoutWindow {
    NoTimeout,
    Timeout(BlockHeight),
}

impl Default for TimeoutWindow {
    fn default() -> Self {
        TimeoutWindow::Timeout(BlockHeight(100))
    }
}

#[derive(
    Debug, Serialize, Deserialize, Default, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize,
)]
#[cfg_attr(feature = "full", derive(utoipa::ToSchema))]
/// Used as a blob action to register a contract.
pub struct RegisterContractAction {
    /// Verifier to use for transactions sent to this new contract.
    pub verifier: Verifier,
    /// Other verifier data, such as a Risc0 program ID or noir public key.
    /// Transactions sent to this contract will have to match this program ID.
    pub program_id: ProgramId,
    /// Initial state commitment of the contract to register.
    pub state_commitment: StateCommitment,
    /// Name of the contract to register.
    pub contract_name: ContractName,
    /// Optionally set the timeout window for the contract.
    /// If the contract exists, the timeout window will be unchanged, otherwise, the default value will be used.
    pub timeout_window: Option<TimeoutWindow>,
    /// Optional data for indexers to construct the initial state of the contract.
    /// Importantly, this is *never* checked by the node. You should verify manually it matches the state commitment.
    pub constructor_metadata: Option<Vec<u8>>,
}

#[cfg(feature = "full")]
impl Hashed<TxHash> for RegisterContractAction {
    fn hashed(&self) -> TxHash {
        use sha3::{Digest, Sha3_256};

        let mut hasher = Sha3_256::new();
        hasher.update(self.verifier.0.clone());
        hasher.update(self.program_id.0.clone());
        hasher.update(self.state_commitment.0.clone());
        hasher.update(self.contract_name.0.clone());
        if let Some(timeout_window) = &self.timeout_window {
            match timeout_window {
                TimeoutWindow::NoTimeout => hasher.update(0u8.to_le_bytes()),
                TimeoutWindow::Timeout(bh) => hasher.update(bh.0.to_le_bytes()),
            }
        }
        // We don't hash the constructor metadata.
        let hash_bytes = hasher.finalize();
        TxHash(hex::encode(hash_bytes))
    }
}

impl ContractAction for RegisterContractAction {
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

#[derive(
    Debug, Serialize, Deserialize, Default, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize,
)]
/// Used as a blob action to delete a contract.
pub struct DeleteContractAction {
    pub contract_name: ContractName,
}

impl ContractAction for DeleteContractAction {
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

/// Used by the Hyli node to recognize contract registration.
/// Simply output this struct in your HyleOutput registered_contracts.
/// See uuid-tld for examples.
#[derive(
    Debug, Serialize, Deserialize, Default, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize,
)]
#[cfg_attr(feature = "full", derive(utoipa::ToSchema))]
pub struct RegisterContractEffect {
    /// Verifier to use for transactions sent to this new contract.
    pub verifier: Verifier,
    /// Other verifier data, such as a Risc0 program ID or noir public key.
    /// Transactions sent to this contract will have to match this program ID.
    pub program_id: ProgramId,
    /// Initial state commitment of the contract to register.
    pub state_commitment: StateCommitment,
    /// Name of the contract to register.
    pub contract_name: ContractName,
    /// Optionally set the timeout window for the contract.
    /// If the contract exists, the timeout window will be unchanged, otherwise, the default value will be used.
    pub timeout_window: Option<TimeoutWindow>,
}

impl From<RegisterContractAction> for RegisterContractEffect {
    fn from(action: RegisterContractAction) -> Self {
        RegisterContractEffect {
            verifier: action.verifier,
            program_id: action.program_id,
            state_commitment: action.state_commitment,
            contract_name: action.contract_name,
            timeout_window: action.timeout_window,
        }
    }
}

#[cfg(feature = "full")]
impl Hashed<TxHash> for RegisterContractEffect {
    fn hashed(&self) -> TxHash {
        use sha3::{Digest, Sha3_256};

        let mut hasher = Sha3_256::new();
        hasher.update(self.verifier.0.clone());
        hasher.update(self.program_id.0.clone());
        hasher.update(self.state_commitment.0.clone());
        hasher.update(self.contract_name.0.clone());
        if let Some(timeout_window) = &self.timeout_window {
            match timeout_window {
                TimeoutWindow::NoTimeout => hasher.update(0u8.to_le_bytes()),
                TimeoutWindow::Timeout(bh) => hasher.update(bh.0.to_le_bytes()),
            }
        }
        let hash_bytes = hasher.finalize();
        TxHash(hex::encode(hash_bytes))
    }
}

#[cfg(feature = "full")]
pub mod base64_field {
    use base64::prelude::*;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let encoded = BASE64_STANDARD.encode(bytes);
        serializer.serialize_str(&encoded)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        BASE64_STANDARD.decode(&s).map_err(serde::de::Error::custom)
    }
}
