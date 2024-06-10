-- Add up migration script here
CREATE TABLE IF NOT EXISTS "ceramic_one_stream" (
    "cid" BLOB NOT NULL, -- init event cid
    "sep" TEXT NOT NULL, 
    "sep_value" blob NOT NULL,
    -- we ignore the composeDB/indexing related fields: should_index, unique, context
    PRIMARY KEY(cid)
);
