-- Add up migration script here

CREATE TABLE IF NOT EXISTS ceramic_one_block (
    multihash BYTEA NOT NULL,
    bytes BYTEA NOT NULL,
    PRIMARY KEY(multihash)
);

SELECT multihash, bytes FROM ceramic_one_block where false;
