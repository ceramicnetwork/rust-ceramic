-- Add up migration script here
CREATE TABLE IF NOT EXISTS "ceramic_one_event_header" (
    "cid" BLOB NOT NULL, -- event cid
    "event_type" INTEGER NOT NULL, -- enum EventType: Init, Data, Time
    "stream_cid" BLOB NOT NULL,  -- id field in header. can't have FK because stream may not exist until we discover it but should reference ceramic_one_stream(cid)
    "prev" BLOB, -- prev event cid (can't have FK because node may not know about prev event)
    PRIMARY KEY(cid),
    FOREIGN KEY(cid) REFERENCES ceramic_one_event(cid)
);
