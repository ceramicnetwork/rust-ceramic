-- Add up migration script here
CREATE TABLE IF NOT EXISTS "root" (
    tx_hash BLOB NOT NULL, 
    "root" BLOB NOT NULL, 
    block_hash TEXT NOT NULL,
    "timestamp" INTEGER NOT NULL, 
    PRIMARY KEY(tx_hash)
);