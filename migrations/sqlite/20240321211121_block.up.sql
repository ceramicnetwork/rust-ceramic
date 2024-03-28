-- Add up migration script here

CREATE TABLE IF NOT EXISTS "block" (
    cid BLOB NOT NULL, -- the cid of the Block as bytes no 0x00 prefix
    bytes BLOB NOT NULL,
    PRIMARY KEY(cid)
);
