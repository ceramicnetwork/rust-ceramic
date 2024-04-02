-- Add up migration script here

CREATE TABLE IF NOT EXISTS interest (
    order_key BYTEA NOT NULL, -- network_id sort_value controller StreamID event_cid
    ahash_0 BIGINT NOT NULL CHECK (ahash_0 >= 0 AND ahash_0 < '4294967296'::BIGINT), -- the ahash is decomposed as [u32; 8]
    ahash_1 BIGINT NOT NULL CHECK (ahash_1 >= 0 AND ahash_1 < '4294967296'::BIGINT),
    ahash_2 BIGINT NOT NULL CHECK (ahash_2 >= 0 AND ahash_2 < '4294967296'::BIGINT),
    ahash_3 BIGINT NOT NULL CHECK (ahash_3 >= 0 AND ahash_3 < '4294967296'::BIGINT),
    ahash_4 BIGINT NOT NULL CHECK (ahash_4 >= 0 AND ahash_4 < '4294967296'::BIGINT),
    ahash_5 BIGINT NOT NULL CHECK (ahash_5 >= 0 AND ahash_5 < '4294967296'::BIGINT),
    ahash_6 BIGINT NOT NULL CHECK (ahash_6 >= 0 AND ahash_6 < '4294967296'::BIGINT),
    ahash_7 BIGINT NOT NULL CHECK (ahash_7 >= 0 AND ahash_7 < '4294967296'::BIGINT),
    PRIMARY KEY(order_key)
);

SELECT order_key, ahash_0, ahash_1, ahash_2, ahash_3, ahash_4, ahash_5, ahash_6, ahash_7 FROM interest WHERE false;
