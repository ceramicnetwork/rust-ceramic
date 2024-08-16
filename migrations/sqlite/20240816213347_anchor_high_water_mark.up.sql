-- Add up migration script here

CREATE TABLE IF NOT EXISTS ceramic_one_anchor_high_water_mark (
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    high_water_mark INTEGER NOT NULL,
    PRIMARY KEY(timestamp)
);

SELECT timestamp, high_water_mark FROM "ceramic_one_anchor_high_water_mark" WHERE false;
