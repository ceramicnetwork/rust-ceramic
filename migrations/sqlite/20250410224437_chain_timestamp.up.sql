-- Add up migration script here

CREATE TABLE IF NOT EXISTS ceramic_one_chain_timestamp (
    chain_id TEXT NOT NULL, -- caip-2 chain ID as a string
    block_hash TEXT NOT NULL, -- '0x' prefixed hex string
    "timestamp" INTEGER NOT NULL, -- unix timestamp
    PRIMARY KEY (chain_id, block_hash)
);

CREATE INDEX IF NOT EXISTS idx_ceramic_one_chain_timestamp_block_hash ON ceramic_one_chain_timestamp (block_hash);
