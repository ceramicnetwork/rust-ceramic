-- Add down migration script here

-- Remove the newly added init_cid, source and anchored columns
ALTER TABLE ceramic_one_event DROP COLUMN init_cid;
ALTER TABLE ceramic_one_event DROP COLUMN source;
ALTER TABLE ceramic_one_event DROP COLUMN anchored;
