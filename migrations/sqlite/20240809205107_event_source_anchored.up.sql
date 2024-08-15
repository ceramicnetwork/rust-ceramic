-- Add up migration script here

-- The CID of the Init Event for the stream
ALTER TABLE ceramic_one_event ADD COLUMN init_cid BLOB DEFAULT NULL;

-- The DID of the node that sent the event or our own DID if the event came in via API
ALTER TABLE ceramic_one_event ADD COLUMN source TEXT DEFAULT NULL;

-- The time a Time Event was anchored. NOTE: This will be NULL for Data Events.
ALTER TABLE ceramic_one_event ADD COLUMN anchored TIMESTAMP DEFAULT NULL;

SELECT init_cid, source, anchored FROM "ceramic_one_event" WHERE false;
