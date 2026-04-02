-- fixtures/test_data.sql

-- Inserting test data for the blocks table
INSERT INTO blocks (hash, parent_hash, height, timestamp, total_txs)
VALUES
    ('0101010101010101010101010101010101010101010101010101010101010101', '0000000000000000000000000000000000000000000000000000000000000000', 1, to_timestamp(1632938400), 0),  -- Block 1
    ('0202020202020202020202020202020202020202020202020202020202020202', '0101010101010101010101010101010101010101010101010101010101010101', 2, to_timestamp(1632938460), 4),  -- Block 2
    ('0303030303030303030303030303030303030303030303030303030303030303', '0202020202020202020202020202020202020202020202020202020202020202', 3, to_timestamp(1632938460), 2);  -- Block 2

-- Inserting test data for the transactions table
INSERT INTO transactions (tx_hash, parent_dp_hash, block_hash, block_height, index, version, transaction_type, transaction_status, identity)
VALUES
    ('a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0', 'b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0', NULL, NULL, NULL, 1, 'blob_transaction', 'waiting_dissemination', NULL),  -- Transaction 1 (contract_registration)
    ('a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1', 'b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0', '0202020202020202020202020202020202020202020202020202020202020202', 2, 0, 1, 'blob_transaction', 'success', 'bob@contract_1'),  -- Transaction 1 (contract_registration)
    ('a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2', 'b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0', '0202020202020202020202020202020202020202020202020202020202020202', 2, 1, 1, 'blob_transaction', 'success', 'bob@contract_1'),               -- Transaction 2 (blob)
    ('a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2', 'b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1', '0303030303030303030303030303030303030303030303030303030303030303', 3, 0, 1, 'blob_transaction', 'success', 'bob@contract_1'),               -- Transaction 2 bis (blob)
    ('a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3', 'b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0', '0202020202020202020202020202020202020202020202020202020202020202', 2, 2, 1, 'proof_transaction', 'success', NULL),              -- Transaction 3 (proof)
    ('a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3', 'b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1', NULL, NULL, NULL, 1, 'proof_transaction', 'success', NULL),              -- Transaction 3 bis (proof)
    ('a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4', 'b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0', '0202020202020202020202020202020202020202020202020202020202020202', 2, 3, 1, 'blob_transaction', 'sequenced', 'bob@contract_1');             -- Transaction 4 (blob)

-- Inserting test data for the blob_transactions table
INSERT INTO blobs (tx_hash, parent_dp_hash, blob_index, identity, contract_name, data)
VALUES
    ('a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2', 'b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0', 0, 'identity_1', 'contract_1', '{"data": "blob_data_2"}'),  -- Blob Transaction 2
    ('a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2', 'b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1', 0, 'identity_1', 'contract_1', '{"data": "blob_data_2_bis"}'),  -- Blob Transaction 2 bis
    ('a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4', 'b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0', 0, 'identity_1', 'contract_1', '{"data": "blob_data_4"}');  -- Blob Transaction 4

-- Inserting test data for the blob_proof_outputs table
INSERT INTO blob_proof_outputs (proof_tx_hash, proof_parent_dp_hash, blob_tx_hash, blob_parent_dp_hash, blob_index, blob_proof_output_index, contract_name, hyli_output, settled)
VALUES
    ('a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3', 'b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0', 'a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2', 'b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0', 0, 0, 'contract_1', '{}', true),  -- Proof Transaction 3
    ('a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3', 'b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1', 'a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2', 'b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1', 0, 0, 'contract_1', '{}', true);  -- Proof Transaction 3 bis

-- Inserting test data for the contracts table
INSERT INTO contracts (tx_hash, parent_dp_hash, verifier, program_id, state_commitment, contract_name)
VALUES
    ('a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1', 'b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0', 'verifier_1', convert_to('program_id_1', 'UTF-8'), convert_to('state_commitment_1', 'UTF-8'), 'contract_1');  -- Contract 1

-- Inserting test data for the contract_state table
INSERT INTO contract_state (contract_name, block_hash, state_commitment)
VALUES
    ('contract_1', '0101010101010101010101010101010101010101010101010101010101010101', convert_to('state_commitment_1', 'UTF-8')),     -- State for Contract 1
    ('contract_1', '0202020202020202020202020202020202020202020202020202020202020202', convert_to('state_commitment_1Bis', 'UTF-8'));  -- State for Contract 2

INSERT INTO transaction_state_events (block_hash, block_height, index, tx_hash, parent_dp_hash, event)
VALUES
	('0202020202020202020202020202020202020202020202020202020202020202', 2, 1, 'a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2', 'b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0', '{"name": "Sequenced"}'::jsonb),
	('0303030303030303030303030303030303030303030303030303030303030303', 3, 1, 'a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2', 'b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1', '{"name": "Success"}'::jsonb);
