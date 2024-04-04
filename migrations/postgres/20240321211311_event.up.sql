-- Add up migration script here

CREATE TABLE IF NOT EXISTS ceramic_one_event (
    order_key BYTEA NOT NULL UNIQUE, -- network_id sep_key sep_value controller stream_id event_cid
    ahash_0 BIGINT NOT NULL CHECK (ahash_0 >= 0 AND ahash_0 < '4294967296'::BIGINT), -- the ahash is decomposed as [u32; 8]
    ahash_1 BIGINT NOT NULL CHECK (ahash_1 >= 0 AND ahash_1 < '4294967296'::BIGINT),
    ahash_2 BIGINT NOT NULL CHECK (ahash_2 >= 0 AND ahash_2 < '4294967296'::BIGINT),
    ahash_3 BIGINT NOT NULL CHECK (ahash_3 >= 0 AND ahash_3 < '4294967296'::BIGINT),
    ahash_4 BIGINT NOT NULL CHECK (ahash_4 >= 0 AND ahash_4 < '4294967296'::BIGINT),
    ahash_5 BIGINT NOT NULL CHECK (ahash_5 >= 0 AND ahash_5 < '4294967296'::BIGINT),
    ahash_6 BIGINT NOT NULL CHECK (ahash_6 >= 0 AND ahash_6 < '4294967296'::BIGINT),
    ahash_7 BIGINT NOT NULL CHECK (ahash_7 >= 0 AND ahash_7 < '4294967296'::BIGINT),
    cid BYTEA NOT NULL, -- the cid of the event as bytes no 0x00 prefix
    discovered TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    delivered BIGINT UNIQUE, -- monotonic increasing counter indicating this can be delivered to clients
    PRIMARY KEY(cid)
);

SELECT order_key, ahash_0, ahash_1, ahash_2, ahash_3, ahash_4, ahash_5, ahash_6, ahash_7, cid, discovered, delivered FROM "ceramic_one_event" WHERE false;
