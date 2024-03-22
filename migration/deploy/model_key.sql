-- Deploy ceramic-one:model_key to sqlite

BEGIN;

CREATE TABLE IF NOT EXISTS model_key (
            key BLOB, -- network_id sort_value controller StreamID height event_cid
            ahash_0 INTEGER, -- the ahash is decomposed as [u32; 8]
            ahash_1 INTEGER,
            ahash_2 INTEGER,
            ahash_3 INTEGER,
            ahash_4 INTEGER,
            ahash_5 INTEGER,
            ahash_6 INTEGER,
            ahash_7 INTEGER,
            value_retrieved BOOL, -- indicates if we have the value
            PRIMARY KEY(key)
        );

CREATE INDEX IF NOT EXISTS idx_key_value_retrieved
            ON model_key (key, value_retrieved);

COMMIT;
