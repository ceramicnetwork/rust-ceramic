-- Revert ceramic-one:model_block from sqlite

BEGIN;

DROP TABLE model_block;

COMMIT;
