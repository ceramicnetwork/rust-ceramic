use std::{collections::BTreeSet, str::FromStr};

use ceramic_api::AccessInterestStore;
use ceramic_core::{
    interest::{Builder, WithPeerId},
    Interest, PeerId,
};
use rand::{thread_rng, Rng};
use recon::{AssociativeHash, ReconItem, Sha256a};

use expect_test::expect;

const SEP_KEY: &str = "model";
const PEER_ID: &str = "1AdgHpWeBKTU3F2tUkAQqL2Y2Geh4QgHJwcWMPuiY1qiRQ";

macro_rules! test_with_sqlite {
    ($test_name: ident, $test_fn: expr $(, $sql_stmts:expr)?) => {
        paste::paste! {
            #[tokio::test]
            async fn [<$test_name _sqlite>]() {
                let _ = ceramic_metrics::init_local_tracing();

                let conn = ceramic_store::SqlitePool::connect_in_memory().await.unwrap();
                let store = $crate::CeramicInterestService::new(conn).await.unwrap();
                $(
                    for stmt in $sql_stmts {
                        store.pool.run_statement(stmt).await.unwrap();
                    }
                )?
                $test_fn(store).await;
            }
        }
    };
}

/// test_name (will eventually generate multiple tests when we have multiple databases to test)
/// test_fn (the test function that will be run for both databases)
/// sql_stmts (optional, array of sql statements to run before the test)
macro_rules! test_with_dbs {
    ($test_name: ident, $test_fn: expr $(, $sql_stmts:expr)?) => {
        test_with_sqlite!($test_name, $test_fn $(, $sql_stmts)?);
    }
}

// Return an builder for an event with the same network,model,controller,stream.
pub(crate) fn interest_builder() -> Builder<WithPeerId> {
    Interest::builder()
        .with_sep_key(SEP_KEY)
        .with_peer_id(&PeerId::from_str(PEER_ID).unwrap())
}

// Generate an event for the same network,model,controller,stream
// The event and height are random when when its None.
pub(crate) fn random_interest<'a>(
    range: Option<(&'a [u8], &'a [u8])>,
    not_after: Option<u64>,
) -> Interest {
    let rand_range = (&[0u8][..], &[thread_rng().gen::<u8>()][..]);
    interest_builder()
        .with_range(range.unwrap_or(rand_range))
        .with_not_after(not_after.unwrap_or_else(|| thread_rng().gen()))
        .build()
}
// The EventId that is the minumum of all possible random event ids
pub(crate) fn random_interest_min() -> Interest {
    interest_builder().with_min_range().build_fencepost()
}
// The EventId that is the maximum of all possible random event ids
pub(crate) fn random_interest_max() -> Interest {
    interest_builder().with_max_range().build_fencepost()
}

test_with_dbs!(
    access_interest_model,
    access_interest_model,
    ["delete from ceramic_one_interest"]
);

// This is the same as the recon::Store range test, but with the interest store (hits all its methods)
async fn access_interest_model(store: impl AccessInterestStore) {
    let interest_0 = random_interest(None, None);
    let interest_1 = random_interest(None, None);
    AccessInterestStore::insert(&store, interest_0.clone())
        .await
        .unwrap();
    AccessInterestStore::insert(&store, interest_1.clone())
        .await
        .unwrap();
    let interests = AccessInterestStore::range(
        &store,
        &random_interest_min(),
        &random_interest_max(),
        0,
        usize::MAX,
    )
    .await
    .unwrap();
    assert_eq!(
        BTreeSet::from_iter([interest_0, interest_1]),
        BTreeSet::from_iter(interests)
    );
}

test_with_dbs!(
    test_hash_range_query,
    test_hash_range_query,
    ["delete from ceramic_one_interest"]
);

async fn test_hash_range_query<S>(store: S)
where
    S: recon::Store<Key = Interest, Hash = Sha256a>,
{
    recon::Store::insert(
        &store,
        &ReconItem::new(&random_interest(Some((&[0], &[1])), Some(42)), &vec![]),
    )
    .await
    .unwrap();

    recon::Store::insert(
        &store,
        &ReconItem::new(&random_interest(Some((&[0], &[1])), Some(24)), &vec![]),
    )
    .await
    .unwrap();
    let hash_cnt = store
        .hash_range(&random_interest_min()..&random_interest_max())
        .await
        .unwrap();
    expect!["D6C3CBCCE02E4AF2900ACF7FC84BE91168A42A0B1164534C426C782057E13BBC"]
        .assert_eq(&hash_cnt.hash().to_hex());
}

test_with_dbs!(
    test_range_query,
    test_range_query,
    ["delete from ceramic_one_interest"]
);

async fn test_range_query<S>(store: S)
where
    S: recon::Store<Key = Interest, Hash = Sha256a>,
{
    let interest_0 = random_interest(None, None);
    let interest_1 = random_interest(None, None);

    recon::Store::insert(&store, &ReconItem::new(&interest_0, &vec![]))
        .await
        .unwrap();
    recon::Store::insert(&store, &ReconItem::new(&interest_1, &vec![]))
        .await
        .unwrap();
    let ids = recon::Store::range(
        &store,
        &random_interest_min()..&random_interest_max(),
        0,
        usize::MAX,
    )
    .await
    .unwrap();
    let interests = ids.collect::<BTreeSet<Interest>>();
    assert_eq!(BTreeSet::from_iter([interest_0, interest_1]), interests);
}

test_with_dbs!(
    test_range_with_values_query,
    test_range_with_values_query,
    ["delete from ceramic_one_interest"]
);

async fn test_range_with_values_query<S>(store: S)
where
    S: recon::Store<Key = Interest, Hash = Sha256a>,
{
    let interest_0 = random_interest(None, None);
    let interest_1 = random_interest(None, None);

    store
        .insert(&ReconItem::new(&interest_0, &vec![]))
        .await
        .unwrap();
    store
        .insert(&ReconItem::new(&interest_1, &vec![]))
        .await
        .unwrap();
    let ids = store
        .range_with_values(
            &random_interest_min()..&random_interest_max(),
            0,
            usize::MAX,
        )
        .await
        .unwrap();
    let interests = ids
        .into_iter()
        .map(|(i, _v)| i)
        .collect::<BTreeSet<Interest>>();
    assert_eq!(BTreeSet::from_iter([interest_0, interest_1]), interests);
}

test_with_dbs!(
    test_double_insert,
    test_double_insert,
    ["delete from ceramic_one_interest"]
);

async fn test_double_insert<S>(store: S)
where
    S: recon::Store<Key = Interest, Hash = Sha256a>,
{
    let interest = random_interest(None, None);
    // do take the first one
    expect![
        r#"
        Ok(
            true,
        )
        "#
    ]
    .assert_debug_eq(&recon::Store::insert(&store, &ReconItem::new(&interest, &vec![])).await);

    // reject the second insert of same key
    expect![
        r#"
        Ok(
            false,
        )
        "#
    ]
    .assert_debug_eq(&recon::Store::insert(&store, &ReconItem::new(&interest, &vec![])).await);
}

test_with_dbs!(
    test_store_value_for_key_error,
    test_store_value_for_key_error,
    ["delete from ceramic_one_interest"]
);

async fn test_store_value_for_key_error<S>(store: S)
where
    S: recon::Store<Key = Interest, Hash = Sha256a>,
{
    let key = random_interest(None, None);
    let store_value = random_interest(None, None);
    let err = recon::Store::insert(&store, &ReconItem::new(&key, store_value.as_slice())).await;
    let err = err.unwrap_err();
    assert!(err
        .to_string()
        .contains("Interests do not support values! Invalid request."));
}

test_with_dbs!(
    test_value_for_key,
    test_value_for_key,
    ["delete from ceramic_one_interest"]
);

async fn test_value_for_key<S>(store: S)
where
    S: recon::Store<Key = Interest, Hash = Sha256a>,
{
    let key = random_interest(None, None);
    recon::Store::insert(&store, &ReconItem::new(&key, &vec![]))
        .await
        .unwrap();
    let value = store.value_for_key(&key).await.unwrap();
    let val = value.unwrap();
    let empty: Vec<u8> = vec![];
    assert_eq!(empty, val);
}
