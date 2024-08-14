-- Add up migration script here

CREATE TABLE IF NOT EXISTS ceramic_one_anchor_high_water_mark (
    high_water_mark INTEGER NOT NULL,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(high_water_mark)
);

SELECT timestamp, high_water_mark FROM "ceramic_one_anchor_high_water_mark" WHERE false;
