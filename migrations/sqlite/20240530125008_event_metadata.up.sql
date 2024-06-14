-- Add up migration script here
CREATE TABLE IF NOT EXISTS "ceramic_one_event_metadata" (
    "cid" BLOB NOT NULL, -- event cid
    "event_type" INTEGER NOT NULL, -- enum EventType: Init, Data, Time
    "stream_cid" BLOB NOT NULL,  -- id field in header. can't have FK because stream may not exist until we discover it but should reference ceramic_one_stream(cid)
    "prev" BLOB, -- prev event cid. can't have a foreign key because node may not know about prev event but it should reference ceramic_one_event(cid)
    PRIMARY KEY(cid),
    FOREIGN KEY(cid) REFERENCES ceramic_one_event(cid)
);

CREATE INDEX IF NOT EXISTS "idx_ceramic_one_event_metadata_stream_cid" ON "ceramic_one_event_metadata" ("stream_cid");
