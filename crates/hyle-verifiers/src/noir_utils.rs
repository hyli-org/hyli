use anyhow::{Context, Error};
use hyle_model::{Blob, BlobIndex, HyleOutput, IndexedBlobs, StateCommitment, TxHash};
use tracing::debug;

/// Extracts the public inputs from the output of `reconstruct_honk_proof`.
pub fn extract_public_inputs<'a>(
    proof_with_public_inputs: &'a [u8],
    vkey: &[u8],
) -> Option<&'a [u8]> {
    // We need to know the number of public inputs, and that's in the vkey.
    // See barretenberg/plonk/proof_system/verification_key/verification_key.hpp
    // This is msgpack encoded, but we can safely just parse the third u64 (for now anyways).
    let num_public_inputs = vkey
        .get(16..24)
        .map(|x| u64::from_be_bytes(x.try_into().unwrap()))?;

    proof_with_public_inputs.get(4..(4 + num_public_inputs * 32) as usize)
}

/// Reverses the flattening process by splitting a `Vec<u8>` into a vector of sanitized hex-encoded strings
pub fn deflatten_fields(flattened_fields: &[u8]) -> Vec<String> {
    const PUBLIC_INPUT_SIZE: usize = 32; // Each field is 32 bytes
    let mut result = Vec::new();

    for chunk in flattened_fields.chunks(PUBLIC_INPUT_SIZE) {
        let hex_string = hex::encode(chunk);
        let sanitised_hex = format!("{:0>64}", hex_string); // Pad to 64 characters
        result.push(sanitised_hex);
    }

    result
}

pub fn parse_noir_output(output: &[u8], vkey: &[u8]) -> Result<HyleOutput, Error> {
    // let mut public_outputs: Vec<String> = serde_json::from_str(&output_json)?;
    let Some(public_inputs) = extract_public_inputs(output, vkey) else {
        return Err(anyhow::anyhow!("Failed to extract public inputs"));
    };
    let mut vector = deflatten_fields(public_inputs);

    let version = u32::from_str_radix(&vector.remove(0), 16)?;
    debug!("Parsed version: {}", version);
    let initial_state = parse_array(&mut vector)?;
    let next_state = parse_array(&mut vector)?;
    let identity = parse_string_with_len(&mut vector)?;
    let tx_hash = parse_sized_string(&mut vector, 64)?;
    let index = u32::from_str_radix(&vector.remove(0), 16)?;
    debug!("Parsed index: {}", index);
    let blobs = parse_blobs(&mut vector)?;
    let tx_blob_count = usize::from_str_radix(&vector.remove(0), 16)?;
    debug!("Parsed tx_blob_count: {}", tx_blob_count);
    let success = u32::from_str_radix(&vector.remove(0), 16)? == 1;
    debug!("Parsed success: {}", success);

    Ok(HyleOutput {
        version,
        initial_state: StateCommitment(initial_state),
        next_state: StateCommitment(next_state),
        identity: identity.into(),
        tx_hash: TxHash(tx_hash),
        tx_ctx: None,
        index: BlobIndex(index as usize),
        blobs,
        tx_blob_count,
        success,
        state_reads: vec![],
        onchain_effects: vec![],
        // Parse the remained as an array, if any
        program_outputs: match vector.len() {
            0 => vec![],
            _ => parse_array(&mut vector).unwrap_or_default(),
        },
    })
}

fn parse_sized_string(vector: &mut Vec<String>, length: usize) -> Result<String, Error> {
    let mut resp = String::with_capacity(length);
    for _ in 0..length {
        let code = u32::from_str_radix(&vector.remove(0), 16)?;
        let ch = std::char::from_u32(code)
            .ok_or_else(|| anyhow::anyhow!("Invalid char code: {}", code))?;
        resp.push(ch);
    }
    debug!("Parsed string: {}", resp);
    Ok(resp)
}

/// Parse a string of variable length, up to a maximum size of 256 bytes.
/// Returns the string without trailing zeros.
fn parse_string_with_len(vector: &mut Vec<String>) -> Result<String, Error> {
    let length = usize::from_str_radix(&vector.remove(0), 16)?;
    if length > 256 {
        return Err(anyhow::anyhow!(
            "Invalid contract name length {length}. Max is 256."
        ));
    }
    let mut field = parse_sized_string(vector, 256)?;
    field.truncate(length);
    debug!("Parsed string: {}", field);
    Ok(field)
}

fn parse_array(vector: &mut Vec<String>) -> Result<Vec<u8>, Error> {
    let length = usize::from_str_radix(&vector.remove(0), 16)?;
    let mut resp = Vec::with_capacity(length);
    for _ in 0..length {
        let num = u8::from_str_radix(&vector.remove(0), 16)?;
        resp.push(num);
    }
    debug!("Parsed array of len: {}", length);
    Ok(resp)
}

fn parse_blobs(blob_data: &mut Vec<String>) -> Result<IndexedBlobs, Error> {
    let blob_number = usize::from_str_radix(&blob_data.remove(0), 16)?;
    let mut blobs = IndexedBlobs::default();

    debug!("blob_number: {}", blob_number);

    for _ in 0..blob_number {
        let index = usize::from_str_radix(&blob_data.remove(0), 16)?;
        debug!("blob index: {}", index);

        let contract_name = parse_string_with_len(blob_data)?;

        let blob_capacity = usize::from_str_radix(&blob_data.remove(0), 16)?;
        let blob_len = usize::from_str_radix(&blob_data.remove(0), 16)?;
        debug!("blob len: {} (capacity: {})", blob_len, blob_capacity);

        let mut blob = Vec::with_capacity(blob_capacity);

        for i in 0..blob_capacity {
            let v = &blob_data.remove(0);
            blob.push(
                u8::from_str_radix(v, 16)
                    .context(format!("Failed to parse blob data at {i}/{blob_capacity}"))?,
            );
        }
        blob.truncate(blob_len);

        debug!("blob data: {:?}", blob);
        blobs.push((
            BlobIndex(index),
            Blob {
                contract_name: contract_name.into(),
                data: hyle_model::BlobData(blob),
            },
        ));
    }

    debug!("Parsed blobs: {:?}", blobs);

    Ok(blobs)
}
