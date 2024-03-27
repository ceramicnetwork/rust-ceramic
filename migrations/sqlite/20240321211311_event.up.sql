-- Add up migration script here

CREATE TABLE IF NOT EXISTS "event" (
    id BLOB NOT NULL, -- network_id sort_value controller StreamID height event_cid
    cid BLOB UNIQUE, -- NOT NULL? the cid of the event as bytes no 0x00 prefix
    ahash_0 INTEGER NOT NULL, -- the ahash is decomposed as [u32; 8]
    ahash_1 INTEGER NOT NULL,
    ahash_2 INTEGER NOT NULL,
    ahash_3 INTEGER NOT NULL,
    ahash_4 INTEGER NOT NULL,
    ahash_5 INTEGER NOT NULL,
    ahash_6 INTEGER NOT NULL,
    ahash_7 INTEGER NOT NULL,
    PRIMARY KEY(id)
);
