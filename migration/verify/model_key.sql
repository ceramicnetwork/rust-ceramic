-- Verify ceramic-one:model_key on sqlite

BEGIN;

SELECT key, ahash_0, ahash_1, ahash_2, ahash_3, ahash_4, ahash_5, ahash_6, ahash_7, value_retrieved
FROM model_key
WHERE 0;

ROLLBACK;
