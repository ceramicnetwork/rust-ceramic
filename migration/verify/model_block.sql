-- Verify ceramic-one:model_block on sqlite

BEGIN;

SELECT key, cid, idx, root, bytes FROM model_block WHERE 0;

ROLLBACK;
