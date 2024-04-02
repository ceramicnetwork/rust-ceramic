-- Add up migration script here
CREATE TABLE IF NOT EXISTS "root" (
    tx_hash BYTEA NOT NULL, 
    "root" BYTEA NOT NULL, 
    block_hash TEXT NOT NULL,
    "timestamp" BIGINT NOT NULL, 
    PRIMARY KEY(tx_hash)
);

SELECT tx_hash, "root", block_hash, "timestamp" FROM "root" WHERE false;
