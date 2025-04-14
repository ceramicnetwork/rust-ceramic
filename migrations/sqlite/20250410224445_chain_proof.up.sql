-- Add up migration script here

CREATE TABLE IF NOT EXISTS ceramic_one_chain_proof (
    chain_id TEXT NOT NULL, -- caip-2 chain ID as a string
    transaction_hash TEXT NOT NULL, -- '0x' prefixed hex string
    transaction_input TEXT NOT NULL, -- '0x' prefixed hex string
    block_hash TEXT NULL, -- '0x' prefixed hex string
    PRIMARY KEY (chain_id, transaction_hash)
);

CREATE INDEX IF NOT EXISTS idx_ceramic_one_chain_proof_block_hash ON ceramic_one_chain_proof (block_hash);
CREATE INDEX IF NOT EXISTS idx_ceramic_one_chain_proof_chain_id_block_hash ON ceramic_one_chain_proof (chain_id, block_hash);