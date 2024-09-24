-- Add up migration script here

-- The CID of the Init Event for the stream
ALTER TABLE ceramic_one_event ADD COLUMN init_cid BLOB DEFAULT NULL;

-- The ID of the node that sent the event or our own ID if the event came in via API
ALTER TABLE ceramic_one_event ADD COLUMN informant TEXT DEFAULT NULL;

SELECT init_cid, informant FROM "ceramic_one_event" WHERE false;
