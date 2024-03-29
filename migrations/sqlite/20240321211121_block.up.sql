-- Add up migration script here

CREATE TABLE IF NOT EXISTS "block" (
    multihash BLOB NOT NULL,
    bytes BLOB NOT NULL,
    PRIMARY KEY(multihash)
);
