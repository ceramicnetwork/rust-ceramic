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
    pub fn undelivered_with_values() -> &'static str {
        r#"SELECT
                key.order_key, key.event_cid, eb.codec, eb.root, eb.idx, b.multihash, b.bytes
            FROM (
                SELECT
                    e.cid as event_cid, e.order_key
                FROM ceramic_one_event e
                WHERE
                    EXISTS (SELECT 1 FROM ceramic_one_event_block where event_cid = e.cid)
                    AND e.delivered IS NULL
                LIMIT
                    $1
                OFFSET
                    $2
            ) key
            JOIN
                ceramic_one_event_block eb ON key.event_cid = eb.event_cid
            JOIN ceramic_one_block b on b.multihash = eb.block_multihash
                ORDER BY key.order_key, eb.idx;"#
    }

    /// Requires binding 2 parameters. Fetches the new rows as `DeliveredEvent` objects
    pub fn new_delivered_events() -> &'static str {
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
    pub fn mark_ready_to_deliver() -> &'static str {
        "UPDATE ceramic_one_event SET delivered = $1 WHERE cid = $2 and delivered is NULL;"
    }
}

#[derive(Debug, Clone)]
pub struct EventBlockQuery {}

impl EventBlockQuery {
    pub fn upsert() -> &'static str {
        "INSERT INTO ceramic_one_event_block (event_cid, idx, root, block_multihash, codec) VALUES ($1, $2, $3, $4, $5) on conflict do nothing;"
    }
}

/// Holds the SQL queries than can be shared between interests, events, and across DB types
pub struct ReconQuery {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReconType {
    Event,
    Interest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SqlBackend {
    Sqlite,
}

impl ReconQuery {
    /// Requires 9 parameters: the order_key and the 8 hash values
    pub fn insert_interest() -> &'static str {
        "INSERT INTO ceramic_one_interest (
                    order_key,
                    ahash_0, ahash_1, ahash_2, ahash_3,
                    ahash_4, ahash_5, ahash_6, ahash_7
                ) VALUES (
                    $1, 
                    $2, $3, $4, $5,
                    $6, $7, $8, $9
                );"
    }

    /// Requires 10 parameters: the order_key, cid and the 8 hash values
    pub fn insert_event() -> &'static str {
        "INSERT INTO ceramic_one_event (
            order_key, cid,
            ahash_0, ahash_1, ahash_2, ahash_3,
            ahash_4, ahash_5, ahash_6, ahash_7,
            delivered
        ) VALUES (
            $1, $2,
            $3, $4, $5, $6,
            $7, $8, $9, $10,
            $11
        );"
    }

    /// Requires binding 2 parameters. Returned as `ReconHash` struct
    pub fn hash_range(key_type: ReconType, db: SqlBackend) -> &'static str {
        match (key_type, db) {
            (ReconType::Event, SqlBackend::Sqlite) => {
                r#"SELECT
                    TOTAL(ahash_0) & 0xFFFFFFFF as ahash_0, TOTAL(ahash_1) & 0xFFFFFFFF as ahash_1,
                    TOTAL(ahash_2) & 0xFFFFFFFF as ahash_2, TOTAL(ahash_3) & 0xFFFFFFFF as ahash_3,
                    TOTAL(ahash_4) & 0xFFFFFFFF as ahash_4, TOTAL(ahash_5) & 0xFFFFFFFF as ahash_5,
                    TOTAL(ahash_6) & 0xFFFFFFFF as ahash_6, TOTAL(ahash_7) & 0xFFFFFFFF as ahash_7,
                    COUNT(1) as count
                FROM ceramic_one_event 
                WHERE order_key >= $1 AND order_key < $2;"#
            }
            (ReconType::Interest, SqlBackend::Sqlite) => {
                r#"SELECT
                    TOTAL(ahash_0) & 0xFFFFFFFF as ahash_0, TOTAL(ahash_1) & 0xFFFFFFFF as ahash_1,
                    TOTAL(ahash_2) & 0xFFFFFFFF as ahash_2, TOTAL(ahash_3) & 0xFFFFFFFF as ahash_3,
                    TOTAL(ahash_4) & 0xFFFFFFFF as ahash_4, TOTAL(ahash_5) & 0xFFFFFFFF as ahash_5,
                    TOTAL(ahash_6) & 0xFFFFFFFF as ahash_6, TOTAL(ahash_7) & 0xFFFFFFFF as ahash_7,
                    COUNT(1) as count
                FROM ceramic_one_interest
                WHERE order_key >= $1 AND order_key < $2;"#
            }
        }
    }
    /// Requires binding 2 parameters
    pub fn range(key_type: ReconType) -> &'static str {
        match key_type {
            ReconType::Event => {
                r#"SELECT
                        order_key
                    FROM
                    ceramic_one_event
                    WHERE
                        order_key >= $1 AND order_key < $2
                    ORDER BY
                        order_key ASC
                    LIMIT
                        $3
                    OFFSET
                        $4;"#
            }
            ReconType::Interest => {
                r#"SELECT
                        order_key
                    FROM
                        ceramic_one_interest
                    WHERE
                        order_key >= $1 AND order_key < $2
                    ORDER BY
                        order_key ASC
                    LIMIT
                        $3
                    OFFSET
                        $4;"#
            }
        }
    }

    pub fn count(key_type: ReconType, db: SqlBackend) -> &'static str {
        match (key_type, db) {
            (ReconType::Event, SqlBackend::Sqlite) => {
                r#"SELECT
                    count(order_key) as res
                FROM
                    ceramic_one_event
                WHERE
                    order_key >= $1 AND order_key < $2"#
            }
            (ReconType::Interest, SqlBackend::Sqlite) => {
                r#"SELECT
                    count(order_key) as res
                FROM
                    ceramic_one_interest
                WHERE
                    order_key >= $1 AND order_key < $2"#
            }
        }
    }
}
