pub struct BlockQuery;

impl BlockQuery {
    /// Requires 1 parameter. returns a `CountRow` struct
    pub fn length() -> &'static str {
        "SELECT length(bytes) as res FROM ceramic_one_block WHERE multihash = $1;"
    }
    /// Requires 1 parameter. returns a `BlockBytes` struct
    pub fn get() -> &'static str {
        "SELECT bytes FROM ceramic_one_block WHERE multihash = $1;"
    }
    /// Requires 1 parameter. Return type depends on backend. Make sure you are using int4, int8 correctly
    pub fn has() -> &'static str {
        "SELECT count(1) > 0 as res FROM ceramic_one_block WHERE multihash = $1;"
    }

    /// Requries binding 2 parameters.
    pub fn put() -> &'static str {
        "INSERT INTO ceramic_one_block (multihash, bytes) VALUES ($1, $2);"
    }
}

pub struct EventQuery;

impl EventQuery {
    /// Requires binding 1 parameter. Finds the `BlockRow` values needed to rebuild the event
    /// Looks up the event by the EventID (ie order_key).
    pub fn value_blocks_by_order_key_one() -> &'static str {
        r#"SELECT
                eb.codec, eb.root, b.multihash, b.bytes
        FROM ceramic_one_event_block eb
            JOIN ceramic_one_block b on b.multihash = eb.block_multihash
            JOIN ceramic_one_event e on e.cid = eb.event_cid
        WHERE e.order_key = $1
            ORDER BY eb.idx;"#
    }

    /// Requires binding 1 parameter. Finds the `BlockRow` values needed to rebuild the event
    /// Looks up the event by the root CID of the event.
    pub fn value_blocks_by_cid_one() -> &'static str {
        r#"SELECT
                eb.codec, eb.root, b.multihash, b.bytes
        FROM ceramic_one_event_block eb
            JOIN ceramic_one_block b on b.multihash = eb.block_multihash
        WHERE eb.event_cid = $1
            ORDER BY eb.idx;"#
    }

    /// Requires binding 1 parameter. Would be nice to support IN/ANY for multiple CIDs
    /// but might require rarray/carray support (see rusqlite)
    pub fn value_delivered_by_cid() -> &'static str {
        r#"SELECT count(1) > 0 as "exists", e.delivered is NOT NULL as "delivered"
            FROM ceramic_one_event_block eb
            join ceramic_one_event e on e.cid = eb.event_cid
            WHERE e.cid = $1;"#
    }

    /// Requires binding 4 parameters. Finds the `EventValueRaw` values needed to rebuild the event
    pub fn value_blocks_by_order_key_many() -> &'static str {
        r#"SELECT
                key.order_key, key.event_cid, eb.codec, eb.root, eb.idx, b.multihash, b.bytes
            FROM (
                SELECT
                    e.cid as event_cid, e.order_key
                FROM ceramic_one_event e
                WHERE
                    EXISTS (SELECT 1 FROM ceramic_one_event_block where event_cid = e.cid)
                    AND e.order_key >= $1 AND e.order_key < $2
                ORDER BY
                    e.order_key ASC
                LIMIT
                    $3
                OFFSET
                    $4
            ) key
            JOIN
                ceramic_one_event_block eb ON key.event_cid = eb.event_cid
            JOIN ceramic_one_block b on b.multihash = eb.block_multihash
                ORDER BY key.order_key, eb.idx;"#
    }

    /// Find event CIDs that have not yet been delivered to the client
    /// Useful after a restart, or if the task managing delivery has availability to try old events
    /// Requires binding two parameters:
    ///     $1: limit (i64)
    ///     $2: rowid (i64)
    ///     $3: number_of_tasks/partitions (i32)
    ///     $4: task_id (i32)
    pub fn undelivered_with_values() -> &'static str {
        r#"SELECT
                key.order_key, key.event_cid, eb.codec, eb.root, eb.idx, b.multihash, b.bytes, key.rowid
            FROM (
                SELECT
                    e.cid as event_cid, e.order_key, e.rowid
                FROM ceramic_one_event e
                WHERE
                    EXISTS (SELECT 1 FROM ceramic_one_event_block where event_cid = e.cid)
                    AND e.delivered IS NULL and e.rowid > $1
                    AND (e.rowid % $3) = $4
                LIMIT
                    $2
            ) key
            JOIN
                ceramic_one_event_block eb ON key.event_cid = eb.event_cid
            JOIN ceramic_one_block b on b.multihash = eb.block_multihash
                ORDER BY key.order_key, eb.idx;"#
    }

    /// Requires binding 2 parameters.
    ///     $1 delivered highwater mark (i64)
    ///     $2 limit (i64)
    ///
    /// Returns the event blocks that can be used to reconstruct the event carfile
    ///
    /// IMPORTANT: The results should be sorted by the delivered value before being given to clients
    pub fn new_delivered_events_with_data() -> &'static str {
        r#"SELECT
                key.order_key, key.event_cid, eb.codec, eb.root, eb.idx, b.multihash, b.bytes, key.new_highwater_mark, key.delivered
            FROM (
                SELECT
                    e.cid as event_cid, e.order_key, COALESCE(e.delivered, 0) as "new_highwater_mark", e.delivered
                FROM ceramic_one_event e
                WHERE e.delivered >= $1 -- we return delivered+1 so we must match it next search
                ORDER BY e.delivered
                LIMIT
                    $2
            ) key
            JOIN
                ceramic_one_event_block eb ON key.event_cid = eb.event_cid
            JOIN ceramic_one_block b on b.multihash = eb.block_multihash
                ORDER BY key.order_key, eb.idx;"#
    }

    pub fn new_delivered_events_id_only() -> &'static str {
        r#"SELECT
                cid, COALESCE(delivered, 0) as "new_highwater_mark"
            FROM ceramic_one_event
            WHERE delivered >= $1 -- we return delivered+1 so we must match it next search
            ORDER BY delivered
            LIMIT $2"#
    }

    /// Returns the max delivered value in the event table
    pub fn max_delivered() -> &'static str {
        r#"SELECT
            COALESCE(MAX(delivered), 0) as res
        FROM ceramic_one_event;"#
    }

    /// Updates the delivered column in the event table so it can be set to the client
    /// Requires 2 parameters:
    ///     $1 = delivered (i64)
    ///     $2 = cid (bytes)
    pub fn mark_ready_to_deliver() -> &'static str {
        "UPDATE ceramic_one_event SET delivered = $1 WHERE cid = $2 and delivered is NULL;"
    }

    /// Fetch data event CIDs from a specified source that are above the current high water mark.
    /// Requires 3 parameters:
    ///    $1 = informant (Ceramic node DID Key)
    ///    $2 = high water mark (i64)
    ///    $3 = limit (i64)
    pub fn data_events_by_informant() -> &'static str {
        r#"SELECT
               order_key, init_cid, cid, rowid
           FROM ceramic_one_event
           WHERE
               informant = $1
               AND ROWID > $2
               AND is_time_event = false
               ORDER BY rowid
           LIMIT $3;"#
    }
}

#[derive(Debug, Clone)]
pub struct EventBlockQuery {}

impl EventBlockQuery {
    pub fn upsert() -> &'static str {
        "INSERT INTO ceramic_one_event_block (event_cid, idx, root, block_multihash, codec) VALUES ($1, $2, $3, $4, $5) on conflict do nothing;"
    }
}

/// Holds the SQL queries for accessing events across DB types
pub struct ReconQuery {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SqlBackend {
    Sqlite,
}

impl ReconQuery {
    /// Requires 14 parameters: the order_key, cid, 8 hash values, delivered flag, init_cid, informant,
    /// whether this is a time event
    pub fn insert_event() -> &'static str {
        "INSERT INTO ceramic_one_event (
            order_key, cid,
            ahash_0, ahash_1, ahash_2, ahash_3,
            ahash_4, ahash_5, ahash_6, ahash_7,
            delivered, init_cid, informant, is_time_event
        ) VALUES (
            $1, $2,
            $3, $4, $5, $6,
            $7, $8, $9, $10,
            $11, $12, $13, $14
        );"
    }

    /// Requires binding 2 parameters. Returned as `ReconHash` struct
    pub fn hash_range(db: SqlBackend) -> &'static str {
        match db {
            SqlBackend::Sqlite => {
                r#"SELECT
                    TOTAL(ahash_0) & 0xFFFFFFFF as ahash_0, TOTAL(ahash_1) & 0xFFFFFFFF as ahash_1,
                    TOTAL(ahash_2) & 0xFFFFFFFF as ahash_2, TOTAL(ahash_3) & 0xFFFFFFFF as ahash_3,
                    TOTAL(ahash_4) & 0xFFFFFFFF as ahash_4, TOTAL(ahash_5) & 0xFFFFFFFF as ahash_5,
                    TOTAL(ahash_6) & 0xFFFFFFFF as ahash_6, TOTAL(ahash_7) & 0xFFFFFFFF as ahash_7,
                    COUNT(1) as count
                FROM ceramic_one_event
                WHERE order_key >= $1 AND order_key < $2;"#
            }
        }
    }
    /// Requires binding 2 parameters
    pub fn range() -> &'static str {
        r#"SELECT
                order_key
            FROM
            ceramic_one_event
            WHERE
                order_key >= $1 AND order_key < $2
            ORDER BY
                order_key ASC;"#
    }
    /// Requires binding 2 parameters
    pub fn first() -> &'static str {
        r#"SELECT
                order_key
            FROM
            ceramic_one_event
            WHERE
                order_key >= $1 AND order_key < $2
            ORDER BY
                order_key ASC
            LIMIT 1;"#
    }
    /// Requires binding 3 parameters
    pub fn middle() -> &'static str {
        r#"SELECT
                order_key
            FROM
            ceramic_one_event
            WHERE
                order_key >= $1 AND order_key < $2
            ORDER BY
                order_key ASC
            LIMIT
                1
            OFFSET
                $3;"#
    }

    pub fn count(db: SqlBackend) -> &'static str {
        match db {
            SqlBackend::Sqlite => {
                r#"SELECT
                    count(order_key) as res
                FROM
                    ceramic_one_event
                WHERE
                    order_key >= $1 AND order_key < $2"#
            }
        }
    }
}

/// Represents access to the ceramic_one_chain_proof and ceramic_one_chain_timestamp tables
/// In the future, transactions could be stored without block information, and retried until we discover something.
/// For now, we only store the transaction when we discover the block information and timestamp.
pub struct ChainProofQuery;

impl ChainProofQuery {
    /// Requires binding 3 parameters
    ///     $1: chain_id
    ///     $2: block_hash
    ///     $3: timestamp
    pub fn upsert_timestamp() -> &'static str {
        r#"
            INSERT INTO ceramic_one_chain_timestamp (
                chain_id, block_hash, timestamp
            ) VALUES (
                $1, $2, $3
            ) ON CONFLICT DO UPDATE SET timestamp = $3;
        "#
    }

    /// Requires binding 4 parameters
    ///     $1: chain_id
    ///     $2: block_hash
    ///     $3: transaction_hash
    ///     $4: transaction_input
    pub fn upsert_proof() -> &'static str {
        r#"
            INSERT INTO ceramic_one_chain_proof (
                chain_id, block_hash, transaction_hash, transaction_input
            ) VALUES (
                $1, $2, $3, $4
            ) ON CONFLICT DO UPDATE SET block_hash = $2;
        "#
    }

    /// Requires binding 2 parameters
    ///     $1: chain_id
    ///     $2: transaction_hash
    pub fn by_chain_id_and_tx_hash() -> &'static str {
        r#"
            SELECT 
                p.chain_id,
                p.transaction_hash,
                p.transaction_input,
                p.block_hash,
                COALESCE(
                    (SELECT t.timestamp 
                     FROM ceramic_one_chain_timestamp t 
                     WHERE t.chain_id = p.chain_id 
                     AND t.block_hash = p.block_hash),
                    NULL
                ) as timestamp
            FROM ceramic_one_chain_proof p
            WHERE p.chain_id = $1 
            AND p.transaction_hash = $2 
            AND EXISTS (
                SELECT 1
                FROM ceramic_one_chain_timestamp t
                WHERE t.chain_id = p.chain_id
                AND t.block_hash = p.block_hash
            )
        "#
    }
}
