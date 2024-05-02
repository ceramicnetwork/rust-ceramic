-- Add up migration script here
CREATE TABLE IF NOT EXISTS ceramic_one_anchor_request (
    cid BLOB NOT NULL,            -- CID of the event for which an anchor has been requested
    detached_time_event_cid BLOB, -- CID of the Detached Time Event
    PRIMARY KEY(cid)
);

SELECT cid, detached_time_event_cid FROM ceramic_one_anchor_request WHERE false;
