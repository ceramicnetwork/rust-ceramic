-- Add up migration script here

CREATE TABLE IF NOT EXISTS ceramic_one_data_migration (
    "name" TEXT PRIMARY KEY NOT NULL,
    "version" TEXT NOT NULL,
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_attempted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
);
