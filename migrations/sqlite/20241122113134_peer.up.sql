-- Add up migration script here

CREATE TABLE IF NOT EXISTS ceramic_one_peer (
    order_key BLOB NOT NULL, -- PeerKey <expiration>.<jws>
    ahash_0 INTEGER NOT NULL, -- the ahash is decomposed as [u32; 8]
    ahash_1 INTEGER NOT NULL,
    ahash_2 INTEGER NOT NULL,
    ahash_3 INTEGER NOT NULL,
    ahash_4 INTEGER NOT NULL,
    ahash_5 INTEGER NOT NULL,
    ahash_6 INTEGER NOT NULL,
    ahash_7 INTEGER NOT NULL,
    PRIMARY KEY(order_key)
);

SELECT order_key, ahash_0, ahash_1, ahash_2, ahash_3, ahash_4, ahash_5, ahash_6, ahash_7 FROM ceramic_one_peer WHERE false;
