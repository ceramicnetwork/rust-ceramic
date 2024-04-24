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
    /// Requires 1 parameter. Depends on backend. Make sure you are using int4, int8 correctly
    pub fn has() -> &'static str {
        "SELECT count(1) > 0 as res FROM ceramic_one_block WHERE multihash = $1;"
    }
}

pub struct EventQuery;

impl EventQuery {
    /// Requires binding 1 parameter. Finds the `EventValueRaw` values needed to rebuild the event
    pub fn value_blocks_one() -> &'static str {
        r#"SELECT 
                e.order_key, eb.codec, eb.root, b.multihash, b.bytes
        FROM ceramic_one_event_block eb 
            JOIN ceramic_one_block b on b.multihash = eb.block_multihash
            JOIN ceramic_one_event e on e.cid = eb.event_cid
        WHERE e.order_key = $1
            ORDER BY eb.idx;"#
    }

    /// Requires binding 4 parameters. Finds the `EventValueRaw` values needed to rebuild the event
    pub fn value_blocks_many() -> &'static str {
        r#"SELECT
                key.order_key, eb.codec, eb.root, eb.idx, b.multihash, b.bytes
            FROM (
                SELECT
                    e.cid as event_cid, e.order_key
                FROM ceramic_one_event e
                WHERE
                    EXISTS (SELECT 1 FROM ceramic_one_event_block where event_cid = e.cid)
                    AND e.order_key > $1 AND e.order_key < $2
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

    /// Requires binding 2 parameters. Fetches the new rows as `DeliveredEvent` objects
    pub fn new_delivered_events() -> &'static str {
        r#"SELECT 
                order_key, COALESCE(delivered, 0) as "new_highwater_mark"
            FROM ceramic_one_event
            WHERE delivered >= $1 -- we return delivered+1 so we must match it next search
            ORDER BY delivered
            LIMIT $2"#
    }

    pub fn max_delivered() -> &'static str {
        r#"SELECT 
            COALESCE(MAX(delivered), 0) as res 
        FROM ceramic_one_event;"#
    }

    pub fn missing_values() -> &'static str {
        r#"SELECT order_key 
        FROM ceramic_one_event e
        WHERE
            NOT EXISTS (SELECT 1 FROM ceramic_one_event_block where order_key = e.order_key) 
            AND order_key > $1
            AND order_key < $2;"#
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
    _Postgres,
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
            ahash_4, ahash_5, ahash_6, ahash_7
        ) VALUES (
            $1, $2,
            $3, $4, $5, $6,
            $7, $8, $9, $10
        );"
    }

    /// Requires binding 2 parameters. Returned as `ReconHash` struct
    pub fn hash_range(key_type: ReconType, db: SqlBackend) -> &'static str {
        match (key_type, db) {
            (ReconType::Event, SqlBackend::_Postgres) => {
                r#"SELECT
                    COALESCE(SUM(ahash_0), 0)::bigint & 4294967295 as ahash_0, COALESCE(SUM(ahash_1), 0)::bigint & 4294967295 as ahash_1,
                    COALESCE(SUM(ahash_2), 0)::bigint & 4294967295 as ahash_2, COALESCE(SUM(ahash_3), 0)::bigint & 4294967295 as ahash_3,
                    COALESCE(SUM(ahash_4), 0)::bigint & 4294967295 as ahash_4, COALESCE(SUM(ahash_5), 0)::bigint & 4294967295 as ahash_5,
                    COALESCE(SUM(ahash_6), 0)::bigint & 4294967295 as ahash_6, COALESCE(SUM(ahash_7), 0)::bigint & 4294967295 as ahash_7,
                    COUNT(1) as count
                FROM ceramic_one_event 
                WHERE order_key > $1 AND order_key < $2;"#
            }
            (ReconType::Interest, SqlBackend::_Postgres) => {
                r#"SELECT
                    COALESCE(SUM(ahash_0), 0)::bigint & 4294967295 as ahash_0, COALESCE(SUM(ahash_1), 0)::bigint & 4294967295 as ahash_1,
                    COALESCE(SUM(ahash_2), 0)::bigint & 4294967295 as ahash_2, COALESCE(SUM(ahash_3), 0)::bigint & 4294967295 as ahash_3,
                    COALESCE(SUM(ahash_4), 0)::bigint & 4294967295 as ahash_4, COALESCE(SUM(ahash_5), 0)::bigint & 4294967295 as ahash_5,
                    COALESCE(SUM(ahash_6), 0)::bigint & 4294967295 as ahash_6, COALESCE(SUM(ahash_7), 0)::bigint & 4294967295 as ahash_7,
                    COUNT(1) as count
                FROM ceramic_one_interest 
                WHERE order_key > $1 AND order_key < $2;"#
            }
            (ReconType::Event, SqlBackend::Sqlite) => {
                r#"SELECT
                    TOTAL(ahash_0) & 0xFFFFFFFF as ahash_0, TOTAL(ahash_1) & 0xFFFFFFFF as ahash_1,
                    TOTAL(ahash_2) & 0xFFFFFFFF as ahash_2, TOTAL(ahash_3) & 0xFFFFFFFF as ahash_3,
                    TOTAL(ahash_4) & 0xFFFFFFFF as ahash_4, TOTAL(ahash_5) & 0xFFFFFFFF as ahash_5,
                    TOTAL(ahash_6) & 0xFFFFFFFF as ahash_6, TOTAL(ahash_7) & 0xFFFFFFFF as ahash_7,
                    COUNT(1) as count
                FROM ceramic_one_event 
                WHERE order_key > $1 AND order_key < $2;"#
            }
            (ReconType::Interest, SqlBackend::Sqlite) => {
                r#"SELECT
                    TOTAL(ahash_0) & 0xFFFFFFFF as ahash_0, TOTAL(ahash_1) & 0xFFFFFFFF as ahash_1,
                    TOTAL(ahash_2) & 0xFFFFFFFF as ahash_2, TOTAL(ahash_3) & 0xFFFFFFFF as ahash_3,
                    TOTAL(ahash_4) & 0xFFFFFFFF as ahash_4, TOTAL(ahash_5) & 0xFFFFFFFF as ahash_5,
                    TOTAL(ahash_6) & 0xFFFFFFFF as ahash_6, TOTAL(ahash_7) & 0xFFFFFFFF as ahash_7,
                    COUNT(1) as count
                FROM ceramic_one_interest
                WHERE order_key > $1 AND order_key < $2;"#
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
                        order_key > $1 AND order_key < $2
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
                        order_key > $1 AND order_key < $2
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
            (ReconType::Event, SqlBackend::_Postgres) => {
                r#"SELECT
                    count(order_key)::bigint as res
                FROM
                    ceramic_one_event
                WHERE
                    order_key > $1 AND order_key < $2"#
            }
            (ReconType::Interest, SqlBackend::_Postgres) => {
                r#"SELECT
                    count(order_key)::bigint as res
                FROM
                    ceramic_one_interest
                WHERE
                    order_key > $1 AND order_key < $2"#
            }
            (ReconType::Event, SqlBackend::Sqlite) => {
                r#"SELECT
                    count(order_key) as res
                FROM
                    ceramic_one_event
                WHERE
                    order_key > $1 AND order_key < $2"#
            }
            (ReconType::Interest, SqlBackend::Sqlite) => {
                r#"SELECT
                    count(order_key) as res
                FROM
                    ceramic_one_interest
                WHERE
                    order_key > $1 AND order_key < $2"#
            }
        }
    }

    /// Requires binding 2 parameters
    pub fn first_key(key_type: ReconType) -> &'static str {
        match key_type {
            ReconType::Event => {
                r#"SELECT
                        order_key as id
                    FROM
                        ceramic_one_event
                    WHERE
                        order_key > $1 AND order_key < $2
                    ORDER BY
                        order_key ASC
                    LIMIT
                        1"#
            }
            ReconType::Interest => {
                r#"SELECT
                        order_key as id
                    FROM
                        ceramic_one_interest
                    WHERE
                        order_key > $1 AND order_key < $2
                    ORDER BY
                        order_key ASC
                    LIMIT
                        1"#
            }
        }
    }

    /// Requires binding 2 parameters
    pub fn last_key(key_type: ReconType) -> &'static str {
        match key_type {
            ReconType::Event => {
                r#"SELECT
                    order_key as id
                FROM
                    ceramic_one_event
                WHERE
                    order_key > $1 AND order_key < $2
                ORDER BY
                    order_key DESC
                LIMIT
                    1"#
            }
            ReconType::Interest => {
                r#"SELECT
                    order_key as id
                FROM
                    ceramic_one_interest
                WHERE
                    order_key > $1 AND order_key < $2
                ORDER BY
                    order_key DESC
                LIMIT
                    1"#
            }
        }
    }

    /// Requires 4 parameters. Can be converted to FirstAndLast struct
    pub fn first_and_last(key_type: ReconType, db: SqlBackend) -> &'static str {
        match (key_type, db) {
            (ReconType::Event, SqlBackend::_Postgres) => {
                r#"with first as (
                    SELECT order_key
                    FROM ceramic_one_event
                    WHERE
                        order_key > $1 AND order_key < $2
                    ORDER BY order_key ASC
                    LIMIT 1
                ), last as (
                    SELECT order_key
                    FROM ceramic_one_event
                    WHERE
                        order_key > $3 AND order_key < $4
                    ORDER BY order_key DESC
                    LIMIT 1
                ) select first.order_key as "first_key", last.order_key as "last_key" from first, last;"#
            }
            (ReconType::Interest, SqlBackend::_Postgres) => {
                r#"with first as (
                    SELECT order_key
                    FROM ceramic_one_interest
                    WHERE
                        order_key > $1 AND order_key < $2
                    ORDER BY order_key ASC
                    LIMIT 1
                ), last as (
                    SELECT order_key
                    FROM ceramic_one_interest
                    WHERE
                        order_key > $3 AND order_key < $4
                    ORDER BY order_key DESC
                    LIMIT 1
                ) select first.order_key as "first_key", last.order_key as "last_key" from first, last;"#
            }

            (ReconType::Event, SqlBackend::Sqlite) => {
                r#"SELECT first.order_key as "first_key", last.order_key as "last_key"
                    FROM
                        (
                            SELECT order_key
                            FROM ceramic_one_event
                            WHERE
                                order_key > $1 AND order_key < $2
                            ORDER BY order_key ASC
                            LIMIT 1
                        ) as first
                    JOIN
                        (
                            SELECT order_key
                            FROM ceramic_one_event
                            WHERE
                                order_key > $3 AND order_key < $4
                            ORDER BY order_key DESC
                            LIMIT 1
                        ) as last;"#
            }
            (ReconType::Interest, SqlBackend::Sqlite) => {
                r#"SELECT first.order_key as "first_key", last.order_key as "last_key"
                FROM
                    (
                        SELECT order_key
                        FROM ceramic_one_interest
                        WHERE
                            order_key > $1 AND order_key < $2
                        ORDER BY order_key ASC
                        LIMIT 1
                    ) as first
                JOIN
                    (
                        SELECT order_key
                        FROM ceramic_one_interest
                        WHERE
                            order_key > $1 AND order_key < $2
                        ORDER BY order_key DESC
                        LIMIT 1
                    ) as last
                ;"#
            }
        }
    }
}
