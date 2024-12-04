/// Holds the SQL queries for accessing peers across DB types
pub struct ReconQuery {}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SqlBackend {
    Sqlite,
}

impl ReconQuery {
    /// Requires 9 parameters: the order_key and the 8 hash values
    pub fn insert_peer() -> &'static str {
        "INSERT INTO ceramic_one_peer (
                    order_key,
                    ahash_0, ahash_1, ahash_2, ahash_3,
                    ahash_4, ahash_5, ahash_6, ahash_7
                ) VALUES (
                    $1,
                    $2, $3, $4, $5,
                    $6, $7, $8, $9
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
                FROM ceramic_one_peer
                WHERE order_key >= $1 AND order_key < $2;"#
            }
        }
    }
    /// Requires binding 2 parameters
    pub fn range() -> &'static str {
        r#"SELECT
                order_key
            FROM
                ceramic_one_peer
            WHERE
                order_key >= $1 AND order_key < $2
            ORDER BY
                order_key ASC;"#
    }
    /// Requires binding 2 parameters
    pub fn delete_range() -> &'static str {
        r#" DELETE FROM
                ceramic_one_peer
            WHERE
                order_key >= $1 AND order_key < $2;"#
    }
    /// Requires binding 2 parameters
    pub fn first() -> &'static str {
        r#"SELECT
                order_key
            FROM
                ceramic_one_peer
            WHERE
                order_key >= $1 AND order_key < $2
            ORDER BY
                order_key ASC
            LIMIT
                1;"#
    }
    /// Requires binding 3 parameters
    pub fn middle() -> &'static str {
        r#"SELECT
                order_key
            FROM
                ceramic_one_peer
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
                    ceramic_one_peer
                WHERE
                    order_key >= $1 AND order_key < $2"#
            }
        }
    }
}
