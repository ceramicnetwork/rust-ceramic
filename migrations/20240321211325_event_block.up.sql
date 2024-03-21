-- Add up migration script here
CREATE TABLE IF NOT EXISTS event_block (
    id BLOB NOT NULL, -- network_id sort_value controller StreamID height event_cid
    cid BLOB NOT NULL, -- the cid of the Block as bytes no 0x00 prefix
    idx INTEGER NOT NULL, -- the index of the block in the CAR file
    root BOOL NOT NULL, -- bool with 0=false, 1=true. when true the block is a root in the CAR file
    PRIMARY KEY(id, cid),
    foreign KEY(id) references event(id),
    foreign KEY(cid) references block(cid)
);

CREATE INDEX IF NOT EXISTS idx_event_block_cid ON event_block (cid);