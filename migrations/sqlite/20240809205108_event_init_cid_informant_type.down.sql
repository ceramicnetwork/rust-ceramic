-- Add down migration script here

-- Remove the newly added init_cid, informant, type columns
ALTER TABLE ceramic_one_event DROP COLUMN is_time_event;
