use alloc::string::String;
use hyle_model::{verifiers::Secp256k1Blob, BlobIndex, Calldata, ContractName, Identity};
use sha2::{Digest, Sha256};

/// This struct allows to check the existence of a secp256k1 blob in the calldata.
/// It will check:
/// - the identity and the data of the blob.
/// - the contract name of the blob.
/// - the data of the blob.
///
/// secp256k1 blobs are used to verify the signature of a transaction, the signature is
/// natively verified by the node (aka not in a zkvm).
/// Example usage:
/// ```rust,no_run,compile_fail
/// let calldata = Calldata::default();
/// let expected_data = b"expected data";
///
/// let check = CheckSecp256k1::new(&calldata, expected_data);
/// check.expect().unwrap();
/// ```
pub struct CheckSecp256k1<'a> {
    calldata: &'a Calldata,
    expected_data: &'a [u8],
    blob_index: Option<BlobIndex>,
}

impl<'a> CheckSecp256k1<'a> {
    pub fn new(calldata: &'a Calldata, expected_data: &'a [u8]) -> Self {
        Self {
            calldata,
            expected_data,
            blob_index: None,
        }
    }

    #[allow(dead_code)]
    pub fn with_blob_index(mut self, blob_index: BlobIndex) -> Self {
        self.blob_index = Some(blob_index);
        self
    }

    pub fn validate_blob(
        blob: &Secp256k1Blob,
        identity: &Identity,
        expected_data: &'_ [u8],
    ) -> Result<(), &'static str> {
        // Verify that the identity matches the user
        if &blob.identity != identity {
            tracing::error!("Secp256k1Blob identity does not match");
            return Err("Secp256k1Blob identity does not match");
        }

        let mut hasher = Sha256::new();
        hasher.update(expected_data);
        let message_hash: [u8; 32] = hasher.finalize().into();

        if blob.data != message_hash {
            tracing::error!("Secp256k1Blob data does not match");
            return Err("Secp256k1Blob data does not match");
        }

        Ok(())
    }

    pub fn expect(self) -> Result<Secp256k1Blob, &'static str> {
        // Verify Secp256k1Blob
        let secp_blob = match self.blob_index {
            Some(idx) => {
                let blob = self
                    .calldata
                    .blobs
                    .get(&idx)
                    .ok_or("Invalid blob index for secp256k1")?;
                if blob.contract_name != ContractName(String::from("secp256k1")) {
                    return Err("Invalid contract name for Secp256k1Blob");
                }
                blob
            }
            None => self
                .calldata
                .blobs
                .iter()
                .map(|(_, b)| b)
                .find(|b| b.contract_name == ContractName(String::from("secp256k1")))
                .ok_or("Missing Secp256k1Blob")?,
        };

        let secp_data: Secp256k1Blob =
            borsh::from_slice(&secp_blob.data.0).map_err(|_| "Failed to decode Secp256k1Blob")?;

        Self::validate_blob(&secp_data, &self.calldata.identity, self.expected_data)?;

        Ok(secp_data)
    }
}
