-- Deploy ceramic-one:model_block to sqlite

BEGIN;

CREATE TABLE IF NOT EXISTS model_block (
            key BLOB, -- network_id sort_value controller StreamID height event_cid
            cid BLOB, -- the cid of the Block as bytes no 0x00 prefix
            idx INTEGER, -- the index of the block in the CAR file
            root BOOL, -- when true the block is a root in the CAR file
            bytes BLOB, -- the Block
            PRIMARY KEY(key, cid)
        );

CREATE INDEX IF NOT EXISTS idx_model_block_cid
            ON model_block (cid);

COMMIT;
