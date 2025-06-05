-- Add data_proposals table to store metadata for all data proposals
CREATE TABLE data_proposals (
    hash TEXT PRIMARY KEY,                                        -- The data proposal hash
    parent_hash TEXT,                                             -- Parent data proposal hash (nullable)
    lane_id TEXT NOT NULL,                                        -- Lane that created this proposal
    tx_count INT NOT NULL,                                        -- Number of transactions
    estimated_size BIGINT NOT NULL,                               -- Estimated size in bytes
    block_hash TEXT NOT NULL REFERENCES blocks(hash) ON DELETE CASCADE,  -- Block containing this DP
    block_height BIGINT NOT NULL,                                 -- Height of the block
    created_at TIMESTAMP(3) NOT NULL,                             -- Block timestamp as creation time
    
    CHECK (length(hash) = 64),
    CHECK (tx_count >= 0),
    CHECK (estimated_size >= 0),
    CHECK (block_height >= 0)
);

-- Add indexes for efficient querying
CREATE INDEX idx_data_proposals_lane_id ON data_proposals(lane_id);
CREATE INDEX idx_data_proposals_block_height ON data_proposals(block_height DESC);
CREATE INDEX idx_data_proposals_parent_hash ON data_proposals(parent_hash);
CREATE INDEX idx_data_proposals_created_at ON data_proposals(created_at DESC);

-- Add foreign key from transactions to data_proposals
-- This ensures referential integrity
ALTER TABLE transactions 
ADD CONSTRAINT fk_transactions_data_proposal 
FOREIGN KEY (parent_dp_hash) 
REFERENCES data_proposals(hash) 
ON DELETE CASCADE
DEFERRABLE INITIALLY DEFERRED;