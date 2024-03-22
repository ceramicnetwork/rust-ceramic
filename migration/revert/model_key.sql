-- Revert ceramic-one:model_key from sqlite

BEGIN;

DROP TABLE model_key;

COMMIT;
