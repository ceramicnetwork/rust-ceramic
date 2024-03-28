-- Add up migration script here
CREATE TABLE IF NOT EXISTS event_block (
    event_cid BLOB NOT NULL,
    block_multihash BLOB NOT NULL,
    codec INTEGER NOT NULL, -- the codec of the block
    idx INTEGER NOT NULL, -- the index of the block in the CAR file
    "root" BOOL NOT NULL, -- when true the block is a root in the CAR file
    PRIMARY KEY(event_cid, block_multihash),
    foreign KEY(event_cid) references event(cid),
    foreign KEY(block_multihash) references block(multihash)
);

CREATE INDEX IF NOT EXISTS idx_event_block_multihash ON event_block (block_multihash);
