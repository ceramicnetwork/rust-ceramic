-- Add up migration script here

-- The CID of the Init Event for the stream
ALTER TABLE ceramic_one_event ADD COLUMN init_cid BLOB DEFAULT NULL;

-- The ID of the node that sent the event or our own ID if the event came in via API
ALTER TABLE ceramic_one_event ADD COLUMN informant TEXT DEFAULT NULL;

-- Whether this is a Time Event or not
ALTER TABLE ceramic_one_event ADD COLUMN is_time_event BOOLEAN DEFAULT FALSE;

SELECT init_cid, informant, is_time_event FROM "ceramic_one_event" WHERE false;
