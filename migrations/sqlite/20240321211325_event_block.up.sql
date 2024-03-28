-- Add up migration script here
CREATE TABLE IF NOT EXISTS event_block (
    event_cid BLOB NOT NULL,
    block_cid BLOB NOT NULL,
    idx INTEGER NOT NULL, -- the index of the block in the CAR file
    "root" BOOL NOT NULL, -- when true the block is a root in the CAR file
    PRIMARY KEY(event_cid, block_cid),
    foreign KEY(event_cid) references event(cid),
    foreign KEY(block_cid) references block(cid)
);

CREATE INDEX IF NOT EXISTS idx_event_block_cid ON event_block (block_cid);
