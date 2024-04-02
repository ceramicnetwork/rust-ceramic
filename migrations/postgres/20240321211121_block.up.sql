-- Add up migration script here

CREATE TABLE IF NOT EXISTS "block" (
    multihash BYTEA NOT NULL,
    bytes BYTEA NOT NULL,
    PRIMARY KEY(multihash)
);

SELECT multihash, bytes FROM "block" where false;
