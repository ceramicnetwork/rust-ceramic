-- Add down migration script here

-- Remove the newly added init_cid and informant columns
ALTER TABLE ceramic_one_event DROP COLUMN init_cid;
ALTER TABLE ceramic_one_event DROP COLUMN informant;
