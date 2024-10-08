-- Add up migration script here

-- Whether this is a Time Event or not
ALTER TABLE ceramic_one_event ADD COLUMN is_time_event BOOLEAN DEFAULT FALSE;

SELECT is_time_event FROM "ceramic_one_event" WHERE false;
