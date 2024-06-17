-- Add up migration script here
CREATE TABLE IF NOT EXISTS "ceramic_one_version" (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    "version" TEXT NOT NULL UNIQUE,
    "installed_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);