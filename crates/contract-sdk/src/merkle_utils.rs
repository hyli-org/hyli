extern crate alloc;

use alloc::vec::Vec;

use borsh::{BorshDeserialize, BorshSerialize};
use sha2::{Digest, Sha256};
use sparse_merkle_tree::{merge::MergeValue, traits::Hasher, MerkleProof, H256};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BorshableMerkleProof(pub MerkleProof);

impl From<BorshableMerkleProof> for MerkleProof {
    fn from(proof: BorshableMerkleProof) -> Self {
        proof.0
    }
}
impl From<MerkleProof> for BorshableMerkleProof {
    fn from(proof: MerkleProof) -> Self {
        BorshableMerkleProof(proof)
    }
}

impl BorshSerialize for BorshableMerkleProof {
    fn serialize<W: borsh::io::Write>(&self, writer: &mut W) -> borsh::io::Result<()> {
        BorshSerialize::serialize(&(self.0.leaves_count() as u32), writer)?;
        for leaf in self.0.leaves_bitmap() {
            writer.write_all(leaf.as_slice())?;
        }
        BorshSerialize::serialize(&(self.0.merkle_path().len() as u32), writer)?;
        for item in self.0.merkle_path() {
            match item {
                MergeValue::Value(h) => {
                    BorshSerialize::serialize(&0u8, writer)?;
                    writer.write_all(h.as_slice())?;
                }
                MergeValue::MergeWithZero {
                    base_node,
                    zero_bits,
                    zero_count,
                } => {
                    BorshSerialize::serialize(&1u8, writer)?;
                    writer.write_all(base_node.as_slice())?;
                    writer.write_all(zero_bits.as_slice())?;
                    BorshSerialize::serialize(&zero_count, writer)?;
                }
            }
        }
        Ok(())
    }
}

impl BorshDeserialize for BorshableMerkleProof {
    fn deserialize_reader<R: borsh::io::Read>(reader: &mut R) -> borsh::io::Result<Self> {
        let leaves_count = u32::deserialize_reader(reader)?;
        let mut leaves_bitmap = Vec::with_capacity(leaves_count as usize);
        for _ in 0..leaves_count {
            let mut leaf_buf = [0u8; 32];
            reader.read_exact(&mut leaf_buf)?;
            leaves_bitmap.push(H256::from(leaf_buf));
        }

        let path_len = u32::deserialize_reader(reader)?;
        let mut merkle_path = Vec::with_capacity(path_len as usize);
        for _ in 0..path_len {
            let marker = u8::deserialize_reader(reader)?;
            match marker {
                0 => {
                    let mut h_buf = [0u8; 32];
                    reader.read_exact(&mut h_buf)?;
                    merkle_path.push(MergeValue::Value(H256::from(h_buf)));
                }
                1 => {
                    let mut base_node_buf = [0u8; 32];
                    reader.read_exact(&mut base_node_buf)?;

                    let mut zero_bits_buf = [0u8; 32];
                    reader.read_exact(&mut zero_bits_buf)?;

                    let zero_count = u8::deserialize_reader(reader)?;
                    merkle_path.push(MergeValue::MergeWithZero {
                        base_node: H256::from(base_node_buf),
                        zero_bits: H256::from(zero_bits_buf),
                        zero_count,
                    });
                }
                _ => {
                    return Err(borsh::io::Error::new(
                        borsh::io::ErrorKind::InvalidData,
                        "Invalid marker for MergeValue",
                    ));
                }
            }
        }

        Ok(Self(MerkleProof::new(leaves_bitmap, merkle_path)))
    }
}

// Custom SHA256Hasher implementation
#[derive(Default, Debug)]
pub struct SHA256Hasher(Sha256);

impl Hasher for SHA256Hasher {
    fn write_h256(&mut self, h: &H256) {
        self.0.update(h.as_slice());
    }

    fn write_byte(&mut self, b: u8) {
        self.0.update([b]);
    }

    fn finish(self) -> H256 {
        let result = self.0.finalize();
        let mut h = [0u8; 32];
        h.copy_from_slice(&result);
        H256::from(h)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sparse_merkle_tree::merge::MergeValue;

    #[test]
    fn test_borshable_merkle_proof_serialization() {
        let leaves_bitmap = vec![H256::from([1u8; 32]), H256::from([2u8; 32])];
        let merkle_path = vec![
            MergeValue::Value(H256::from([3u8; 32])),
            MergeValue::MergeWithZero {
                base_node: H256::from([4u8; 32]),
                zero_bits: H256::from([5u8; 32]),
                zero_count: 2,
            },
        ];
        let original_proof = MerkleProof::new(leaves_bitmap.clone(), merkle_path.clone());
        let original = BorshableMerkleProof(original_proof);

        let serialized = borsh::to_vec(&original).expect("Failed to serialize");
        let deserialized: BorshableMerkleProof =
            borsh::from_slice(&serialized).expect("Failed to deserialize");

        assert_eq!(deserialized, original);
    }
}
