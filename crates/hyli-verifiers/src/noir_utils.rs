use std::collections::VecDeque;

use anyhow::{anyhow, bail, Context, Error};
use hyli_model::{Blob, BlobData, BlobIndex, HyliOutput, IndexedBlobs, StateCommitment, TxHash};
use tracing::debug;

/// Extracts the public inputs from the output of `reconstruct_honk_proof`.
pub fn split_public_inputs<'a>(
    proof_with_public_inputs: &'a [u8],
    vkey: &[u8],
) -> Option<(&'a [u8], &'a [u8])> {
    // The vk is encoded as field elements; element 1 is the public-input count.
    // Each field element is 32 bytes, so index 1 lives in bytes 32..64.
    let num_public_inputs = {
        let field_one = vkey.get(32..64)?;
        // Count fits in u64; take the least-significant 8 bytes.
        let mut buf = [0u8; 8];
        buf.copy_from_slice(field_one.get(24..32)?);
        u64::from_be_bytes(buf)
    };

    let split_at = num_public_inputs.saturating_mul(32) as usize;
    if split_at == 0 || proof_with_public_inputs.len() < split_at {
        return None;
    }

    proof_with_public_inputs.split_at_checked(split_at)
}

/// Reverses the flattening process by splitting a `Vec<u8>` into a vector of sanitized hex-encoded strings
pub fn deflatten_fields(flattened_fields: &[u8]) -> Vec<String> {
    const PUBLIC_INPUT_SIZE: usize = 32; // Each field is 32 bytes
    let mut result = Vec::new();

    for chunk in flattened_fields.chunks(PUBLIC_INPUT_SIZE) {
        let hex_string = hex::encode(chunk);
        let sanitised_hex = format!("{hex_string:0>64}"); // Pad to 64 characters
        result.push(sanitised_hex);
    }

    result
}

pub fn parse_noir_output(public_inputs: &[u8]) -> Result<HyliOutput, Error> {
    let mut fields: VecDeque<String> = deflatten_fields(public_inputs).into();
    let version = parse_u32(&mut fields)?;
    debug!("Parsed version: {}", version);
    match version {
        1 => parse_noir_output_v1(version, &mut fields),
        2 => parse_noir_output_v2(version, &mut fields),
        _ => Err(anyhow::anyhow!("Unsupported version: {version}")),
    }
}

fn parse_noir_output_v1(version: u32, fields: &mut VecDeque<String>) -> Result<HyliOutput, Error> {
    let initial_state = parse_array_v1(fields)?;
    let next_state = parse_array_v1(fields)?;
    let identity = parse_string_with_len_v1(fields)?;
    let tx_hash = parse_sized_string_v1(fields, 64)?;
    let index = parse_u32(fields)?;
    debug!("Parsed index: {}", index);
    let blobs = parse_blobs_v1(fields)?;
    let tx_blob_count = parse_u32(fields)? as usize;
    debug!("Parsed tx_blob_count: {}", tx_blob_count);
    let success = parse_u32(fields)? == 1;
    debug!("Parsed success: {}", success);

    Ok(HyliOutput {
        version,
        initial_state: StateCommitment(initial_state),
        next_state: StateCommitment(next_state),
        identity: identity.into(),
        tx_hash: TxHash::from_hex(&tx_hash)
            .map_err(|e| anyhow::anyhow!("invalid tx_hash hex: {e}"))?,
        tx_ctx: None,
        index: BlobIndex(index as usize),
        blobs,
        tx_blob_count,
        success,
        state_reads: vec![],
        onchain_effects: vec![],
        // Keep v1 behaviour: if extra fields exist, try parsing once as program_outputs.
        program_outputs: match fields.is_empty() {
            true => vec![],
            false => parse_array_v1(fields).unwrap_or_default(),
        },
    })
}

fn parse_noir_output_v2(version: u32, fields: &mut VecDeque<String>) -> Result<HyliOutput, Error> {
    let initial_state = parse_bounded_bytes(fields, "initial_state")?;
    let next_state = parse_bounded_bytes(fields, "next_state")?;
    let identity = parse_bounded_utf8(fields, "identity")?;
    let index = parse_u32(fields)? as usize;

    let blob_count = parse_u32(fields)? as usize;
    let blob_slots = parse_u32(fields)? as usize;
    let blob_name_max = parse_u32(fields)? as usize;
    let blob_data_max = parse_u32(fields)? as usize;
    if blob_count > blob_slots {
        bail!("blob_count ({blob_count}) exceeds blob_slots ({blob_slots})");
    }
    let blobs = parse_blob_slots_v2(fields, blob_count, blob_slots, blob_name_max, blob_data_max)?;

    let tx_blob_count = parse_u32(fields)? as usize;
    if blob_count > tx_blob_count {
        bail!("blob_count ({blob_count}) exceeds tx_blob_count ({tx_blob_count})");
    }

    let tx_hash = parse_fixed_bytes(fields, "tx_hash", 32)?;

    let success = parse_bool(fields)?;
    let program_outputs_max = parse_u32(fields)? as usize;
    let program_outputs_len = parse_u32(fields)? as usize;
    let mut program_outputs = parse_fixed_bytes(fields, "program_outputs", program_outputs_max)?;
    if program_outputs_len > program_outputs.len() {
        bail!(
            "program_outputs_len ({program_outputs_len}) exceeds program_outputs_max ({})",
            program_outputs.len()
        );
    }
    program_outputs.truncate(program_outputs_len);

    if !fields.is_empty() {
        debug!("Ignoring trailing v2 fields: {}", fields.len());
    }

    Ok(HyliOutput {
        version,
        initial_state: StateCommitment(initial_state),
        next_state: StateCommitment(next_state),
        identity: identity.into(),
        index: BlobIndex(index),
        blobs,
        tx_blob_count,
        tx_hash: TxHash(tx_hash),
        success,
        state_reads: vec![],
        tx_ctx: None,
        onchain_effects: vec![],
        program_outputs,
    })
}

fn parse_sized_string_v1(vector: &mut VecDeque<String>, length: usize) -> Result<String, Error> {
    let mut resp = String::with_capacity(length);
    for _ in 0..length {
        let code = parse_u32(vector)?;
        let ch = std::char::from_u32(code)
            .ok_or_else(|| anyhow::anyhow!("Invalid char code: {}", code))?;
        resp.push(ch);
    }
    debug!("Parsed string: {}", resp);
    Ok(resp)
}

/// Parse a string of variable length, up to a maximum size of 256 bytes.
/// Returns the string without trailing zeros.
fn parse_string_with_len_v1(vector: &mut VecDeque<String>) -> Result<String, Error> {
    let length = parse_u32(vector)? as usize;
    if length > 256 {
        return Err(anyhow::anyhow!(
            "Invalid contract name length {length}. Max is 256."
        ));
    }
    let mut field = parse_sized_string_v1(vector, 256)?;
    field.truncate(length);
    debug!("Parsed string: {}", field);
    Ok(field)
}

fn parse_array_v1(vector: &mut VecDeque<String>) -> Result<Vec<u8>, Error> {
    let length = parse_u32(vector)? as usize;
    let mut resp = Vec::with_capacity(length);
    for _ in 0..length {
        let num = parse_u8(vector)?;
        resp.push(num);
    }
    debug!("Parsed array of len: {}", length);
    Ok(resp)
}

fn parse_blobs_v1(blob_data: &mut VecDeque<String>) -> Result<IndexedBlobs, Error> {
    let blob_number = parse_u32(blob_data)? as usize;
    let mut blobs = IndexedBlobs::default();

    debug!("blob_number: {}", blob_number);

    for _ in 0..blob_number {
        let index = parse_u32(blob_data)? as usize;
        debug!("blob index: {}", index);

        let contract_name = parse_string_with_len_v1(blob_data)?;

        let blob_capacity = parse_u32(blob_data)? as usize;
        let blob_len = parse_u32(blob_data)? as usize;
        debug!("blob len: {} (capacity: {})", blob_len, blob_capacity);

        let mut blob = Vec::with_capacity(blob_capacity);

        for i in 0..blob_capacity {
            let v = pop_field(blob_data)?;
            blob.push(
                u8::from_str_radix(&v, 16)
                    .context(format!("Failed to parse blob data at {i}/{blob_capacity}"))?,
            );
        }
        blob.truncate(blob_len);

        debug!("blob data: {:?}", blob);
        blobs.push((
            BlobIndex(index),
            Blob {
                contract_name: contract_name.into(),
                data: hyli_model::BlobData(blob),
            },
        ));
    }

    debug!("Parsed blobs: {:?}", blobs);

    Ok(blobs)
}

fn parse_blob_slots_v2(
    fields: &mut VecDeque<String>,
    blob_count: usize,
    blob_slots: usize,
    blob_name_max: usize,
    blob_data_max: usize,
) -> Result<IndexedBlobs, Error> {
    let mut blobs = IndexedBlobs::default();
    for slot_index in 0..blob_slots {
        let index = parse_u32(fields)? as usize;
        let contract_name_len = parse_u32(fields)? as usize;
        let mut contract_name = parse_string(fields, "blob.contract_name", blob_name_max)?;
        if contract_name_len > contract_name.len() {
            bail!(
                "blob.contract_name_len ({contract_name_len}) exceeds blob_name_max ({})",
                contract_name.len()
            );
        }
        contract_name.truncate(contract_name_len);

        let data_len = parse_u32(fields)? as usize;
        let mut data = parse_fixed_bytes(fields, "blob.data", blob_data_max)?;
        if data_len > data.len() {
            bail!(
                "blob.data_len ({data_len}) exceeds blob_data_max ({})",
                data.len()
            );
        }
        data.truncate(data_len);
        if slot_index < blob_count {
            blobs.push((
                BlobIndex(index),
                Blob {
                    contract_name: contract_name.into(),
                    data: BlobData(data),
                },
            ));
        }
    }
    Ok(blobs)
}

fn parse_string(
    fields: &mut VecDeque<String>,
    label: &str,
    fixed_len: usize,
) -> Result<String, Error> {
    let bytes = parse_fixed_bytes(fields, label, fixed_len)?;
    String::from_utf8(bytes).context(format!("Invalid UTF-8 for {label}"))
}

fn parse_bounded_bytes(fields: &mut VecDeque<String>, label: &str) -> Result<Vec<u8>, Error> {
    let length = parse_u32(fields)? as usize;
    let max = parse_u32(fields)? as usize;
    let mut out = parse_fixed_bytes(fields, label, max)?;
    if length > out.len() {
        bail!("{label}_len ({length}) exceeds {label}_max ({})", out.len());
    }
    out.truncate(length);
    debug!("Parsed {label} bytes len: {} (max: {})", length, max);
    Ok(out)
}

fn parse_bounded_utf8(fields: &mut VecDeque<String>, label: &str) -> Result<String, Error> {
    let bytes = parse_bounded_bytes(fields, label)?;
    String::from_utf8(bytes).context(format!("Invalid UTF-8 for {label}"))
}

fn parse_fixed_bytes(
    fields: &mut VecDeque<String>,
    label: &str,
    fixed_len: usize,
) -> Result<Vec<u8>, Error> {
    let mut out = Vec::with_capacity(fixed_len);
    for _ in 0..fixed_len {
        out.push(parse_u8(fields)?);
    }
    debug!("Parsed fixed {label} bytes len: {}", fixed_len);
    Ok(out)
}

fn parse_bool(fields: &mut VecDeque<String>) -> Result<bool, Error> {
    let raw = parse_u8(fields)?;
    match raw {
        0 => Ok(false),
        1 => Ok(true),
        _ => bail!("Invalid bool value: {}", raw),
    }
}

fn parse_u8(fields: &mut VecDeque<String>) -> Result<u8, Error> {
    let raw = pop_field(fields)?;
    u8::from_str_radix(&raw, 16).context("Failed to parse u8 from field")
}

fn parse_u32(fields: &mut VecDeque<String>) -> Result<u32, Error> {
    let raw = pop_field(fields)?;
    u32::from_str_radix(&raw, 16).context("Failed to parse u32 from field")
}

fn pop_field(fields: &mut VecDeque<String>) -> Result<String, Error> {
    fields
        .pop_front()
        .ok_or_else(|| anyhow!("Unexpected end of Noir public inputs"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn push_field(fields: &mut Vec<u8>, value: u128) {
        let mut slot = [0u8; 32];
        let bytes = value.to_be_bytes();
        slot[16..].copy_from_slice(&bytes);
        fields.extend_from_slice(&slot);
    }

    fn push_fixed_string(fields: &mut Vec<u8>, fixed_len: usize, s: &str) {
        let bytes = s.as_bytes();
        for &b in bytes {
            push_field(fields, b as u128);
        }
        for _ in bytes.len()..fixed_len {
            push_field(fields, 0);
        }
    }

    fn push_fixed_bytes(fields: &mut Vec<u8>, fixed_len: usize, bytes: &[u8]) {
        for &b in bytes {
            push_field(fields, b as u128);
        }
        for _ in bytes.len()..fixed_len {
            push_field(fields, 0);
        }
    }

    fn build_minimal_v2_public_inputs() -> Vec<u8> {
        let initial_state_max = 4usize;
        let next_state_max = 4usize;
        let identity_max = 56usize;
        let blob_name_max = 8usize;
        let blob_data_max = 4usize;
        let program_outputs_max = 4usize;
        let mut out = Vec::new();
        push_field(&mut out, 2); // version
        push_field(&mut out, 4); // initial_state_len
        push_field(&mut out, initial_state_max as u128); // initial_state_max
        push_fixed_bytes(&mut out, initial_state_max, &[0, 0, 0, 0]); // initial_state
        push_field(&mut out, 4); // next_state_len
        push_field(&mut out, next_state_max as u128); // next_state_max
        push_fixed_bytes(&mut out, next_state_max, &[1, 2, 3, 4]); // next_state
        let identity = "id@ecdsa_secp256r1";
        push_field(&mut out, identity.len() as u128); // identity_len
        push_field(&mut out, identity_max as u128); // identity_max
        push_fixed_string(&mut out, identity_max, identity);
        push_field(&mut out, 0); // index
        push_field(&mut out, 0); // blob_count
        push_field(&mut out, 1); // blob_slots
        push_field(&mut out, blob_name_max as u128); // blob_name_max
        push_field(&mut out, blob_data_max as u128); // blob_data_max
        push_field(&mut out, 0); // blob[0].index
        push_field(&mut out, 0); // blob[0].contract_name_len
        push_fixed_bytes(&mut out, blob_name_max, b""); // blob[0].contract_name
        push_field(&mut out, 0); // blob[0].data_len
        push_fixed_bytes(&mut out, blob_data_max, b""); // blob[0].data
        push_field(&mut out, 0); // tx_blob_count
        push_fixed_bytes(&mut out, 32, &[0u8; 32]); // tx_hash [u8;32]
        push_field(&mut out, 1); // success
        push_field(&mut out, program_outputs_max as u128); // program_outputs_max
        push_field(&mut out, 2); // program_outputs_len
        push_fixed_bytes(&mut out, program_outputs_max, b"ok"); // program_outputs
        out
    }

    #[test]
    fn parses_v2_minimal_output() {
        let inputs = build_minimal_v2_public_inputs();
        let parsed = parse_noir_output(&inputs).expect("parse v2 minimal");
        assert_eq!(parsed.version, 2);
        assert_eq!(parsed.initial_state.0, vec![0, 0, 0, 0]);
        assert_eq!(parsed.next_state.0, vec![1, 2, 3, 4]);
        assert_eq!(parsed.identity.0, "id@ecdsa_secp256r1");
        assert_eq!(parsed.tx_hash.0.len(), 32);
        assert!(parsed.success);
        assert_eq!(parsed.program_outputs, b"ok");
    }

    #[test]
    fn decodes_v2_tx_hash_as_fixed_32_bytes() {
        let mut inputs = Vec::new();
        push_field(&mut inputs, 2); // version
        push_field(&mut inputs, 0); // initial_state_len
        push_field(&mut inputs, 4); // initial_state_max
        push_fixed_bytes(&mut inputs, 4, &[]); // initial_state
        push_field(&mut inputs, 0); // next_state_len
        push_field(&mut inputs, 4); // next_state_max
        push_fixed_bytes(&mut inputs, 4, &[]); // next_state
        push_field(&mut inputs, 1); // identity_len
        push_field(&mut inputs, 56); // identity_max
        push_fixed_string(&mut inputs, 56, "x");
        push_field(&mut inputs, 0); // index
        push_field(&mut inputs, 0); // blob_count
        push_field(&mut inputs, 1); // blob_slots
        push_field(&mut inputs, 8); // blob_name_max
        push_field(&mut inputs, 4); // blob_data_max
        push_field(&mut inputs, 0); // blob[0].index
        push_field(&mut inputs, 0); // blob[0].contract_name_len
        push_fixed_bytes(&mut inputs, 8, b""); // blob[0].contract_name
        push_field(&mut inputs, 0); // blob[0].data_len
        push_fixed_bytes(&mut inputs, 4, b""); // blob[0].data
        push_field(&mut inputs, 0); // tx_blob_count
        let mut tx_hash = [0u8; 32];
        tx_hash[0] = 0xAB;
        tx_hash[31] = 0xCD;
        push_fixed_bytes(&mut inputs, 32, &tx_hash); // tx_hash payload
        push_field(&mut inputs, 0); // success
        push_field(&mut inputs, 4); // outputs_max
        push_field(&mut inputs, 0); // outputs_len
        push_fixed_bytes(&mut inputs, 4, &[]); // outputs

        let parsed = parse_noir_output(&inputs).expect("must parse");
        assert_eq!(parsed.tx_hash.0.len(), 32);
        assert_eq!(parsed.tx_hash.0.first().copied(), Some(0xAB));
        assert_eq!(parsed.tx_hash.0.last().copied(), Some(0xCD));
    }

    #[test]
    fn tolerates_v2_trailing_fields() {
        let mut inputs = build_minimal_v2_public_inputs();
        push_field(&mut inputs, 99); // trailing unexpected field
        let parsed = parse_noir_output(&inputs).expect("must parse with trailing fields");
        assert_eq!(parsed.version, 2);
    }
}
