-- Add up migration script here

CREATE TABLE IF NOT EXISTS ceramic_one_event (
    order_key BLOB NOT NULL UNIQUE, -- network_id sep_key sep_value controller stream_id event_cid
    ahash_0 INTEGER NOT NULL, -- the ahash is decomposed as [u32; 8]
    ahash_1 INTEGER NOT NULL,
    ahash_2 INTEGER NOT NULL,
    ahash_3 INTEGER NOT NULL,
    ahash_4 INTEGER NOT NULL,
    ahash_5 INTEGER NOT NULL,
    ahash_6 INTEGER NOT NULL,
    ahash_7 INTEGER NOT NULL,
    cid BLOB NOT NULL, -- the cid of the event as bytes no 0x00 prefix
    discovered TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    delivered INTEGER UNIQUE, -- monotonic increasing counter indicating this can be delivered to clients
    PRIMARY KEY(cid)
);

SELECT order_key, ahash_0, ahash_1, ahash_2, ahash_3, ahash_4, ahash_5, ahash_6, ahash_7, cid, discovered, delivered FROM "ceramic_one_event" WHERE false;
