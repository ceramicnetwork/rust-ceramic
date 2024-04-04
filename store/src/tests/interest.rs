use std::{collections::BTreeSet, str::FromStr, sync::OnceLock};

use crate::PostgresPool;

use self::sql::InterestStorePostgres;

use super::*;

use ceramic_api::AccessInterestStore;
use ceramic_core::{
    interest::{Builder, WithPeerId},
    Interest, PeerId,
};
use rand::{thread_rng, Rng};
use recon::{AssociativeHash, FullInterests, InterestProvider, Key, ReconItem, Sha256a, Store};

use expect_test::expect;
use test_log::test;
use tokio::sync::{Mutex, MutexGuard};

const SORT_KEY: &str = "model";
const PEER_ID: &str = "1AdgHpWeBKTU3F2tUkAQqL2Y2Geh4QgHJwcWMPuiY1qiRQ";

static LOCK: OnceLock<Mutex<()>> = OnceLock::new();

async fn prep_test(store: &InterestStorePostgres<Sha256a>) -> MutexGuard<'static, ()> {
    let lock = LOCK.get_or_init(|| Mutex::new(())).lock().await;
    store
        .pool
        .run_statement("delete from ceramic_one_interest;")
        .await
        .unwrap();
    lock
}

// Return an builder for an event with the same network,model,controller,stream.
pub(crate) fn interest_builder() -> Builder<WithPeerId> {
    Interest::builder()
        .with_sort_key(SORT_KEY)
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

async fn new_store() -> InterestStorePostgres<Sha256a> {
    let conn = PostgresPool::connect_in_memory().await.unwrap();
    InterestStorePostgres::<Sha256a>::new(conn).await.unwrap()
}

#[test(tokio::test)]
// This is the same as the recon::Store range test, but with the interest store (hits all its methods)
async fn access_interest_model() {
    let store = new_store().await;
    let _lock = prep_test(&store).await;

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
        random_interest_min(),
        random_interest_max(),
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

#[test(tokio::test)]
async fn test_hash_range_query() {
    let mut store = new_store().await;
    let _lock = prep_test(&store).await;
    recon::Store::insert(
        &mut store,
        ReconItem::new_key(&random_interest(Some((&[0], &[1])), Some(42))),
    )
    .await
    .unwrap();

    recon::Store::insert(
        &mut store,
        ReconItem::new_key(&random_interest(Some((&[0], &[1])), Some(24))),
    )
    .await
    .unwrap();
    let hash_cnt = store
        .hash_range(&random_interest_min(), &random_interest_max())
        .await
        .unwrap();
    expect!["D6C3CBCCE02E4AF2900ACF7FC84BE91168A42A0B1164534C426C782057E13BBC"]
        .assert_eq(&hash_cnt.hash().to_hex());
}

#[test(tokio::test)]
async fn test_hash_range_query_defaults() {
    let mut store = new_store().await;
    let _lock = prep_test(&store).await;
    store.pool.run_statement(r#"INSERT INTO public.ceramic_one_interest
        (order_key, ahash_0, ahash_1, ahash_2, ahash_3, ahash_4, ahash_5, ahash_6, ahash_7)
        VALUES(decode('480F70D652B6B825E45826002408011220FCD119D77CA668F157CED3FB79498A82B41CA4D7DC05F90B6B227196F3EC856401581DCE010580808080107E710E217FA0E25900000000000000000000000000581DCE010580808080107E710E217FA0E259FFFFFFFFFFFFFFFFFFFFFFFFFF00','hex'), 3708858614, 3187057933, 90630269, 2944836858, 2664423810, 3186949905, 2792292527, 515406059);"#).await.unwrap();

    store.pool.run_statement("INSERT INTO public.ceramic_one_interest
        (order_key, ahash_0, ahash_1, ahash_2, ahash_3, ahash_4, ahash_5, ahash_6, ahash_7)
        VALUES(decode('480F70D652B6B825E45826002408011220FCD119D77CA668F157CED3FB79498A82B41CA4D7DC05F90B6B227196F3EC856401581DCE01058080808010A252AB059F8F49FD00000000000000000000000000581DCE01058080808010A252AB059F8F49FDFFFFFFFFFFFFFFFFFFFFFFFFFF00','hex'), 2841278, 2946150166, 1420163820, 754142617, 2283458068, 1856053704, 3039129056, 3387910774);").await.unwrap();
    let interests = FullInterests::<Interest>::default()
        .interests()
        .await
        .unwrap();

    for int in interests {
        let hash_cnt = store.hash_range(&int.start, &int.end).await.unwrap();
        assert_eq!(2, hash_cnt.count())
    }
}

#[test(tokio::test)]
async fn test_range_query() {
    let mut store = new_store().await;
    let interest_0 = random_interest(None, None);
    let interest_1 = random_interest(None, None);
    let _lock = prep_test(&store).await;

    recon::Store::insert(&mut store, ReconItem::new_key(&interest_0))
        .await
        .unwrap();
    recon::Store::insert(&mut store, ReconItem::new_key(&interest_1))
        .await
        .unwrap();
    let ids = recon::Store::range(
        &mut store,
        &random_interest_min(),
        &random_interest_max(),
        0,
        usize::MAX,
    )
    .await
    .unwrap();
    let interests = ids.collect::<BTreeSet<Interest>>();
    assert_eq!(BTreeSet::from_iter([interest_0, interest_1]), interests);
}

#[test(tokio::test)]
async fn test_range_with_values_query() {
    let mut store = new_store().await;
    let _lock = prep_test(&store).await;
    let interest_0 = random_interest(None, None);
    let interest_1 = random_interest(None, None);

    store.insert(interest_0.clone()).await.unwrap();
    store.insert(interest_1.clone()).await.unwrap();
    let ids = store
        .range_with_values(
            &random_interest_min(),
            &random_interest_max(),
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

#[test(tokio::test)]
async fn test_double_insert() {
    let mut store = new_store().await;
    let _lock = prep_test(&store).await;

    let interest = random_interest(None, None);
    // do take the first one
    expect![
        r#"
        Ok(
            true,
        )
        "#
    ]
    .assert_debug_eq(&recon::Store::insert(&mut store, ReconItem::new_key(&interest)).await);

    // reject the second insert of same key
    expect![
        r#"
        Ok(
            false,
        )
        "#
    ]
    .assert_debug_eq(&recon::Store::insert(&mut store, ReconItem::new_key(&interest)).await);
}

#[test(tokio::test)]
async fn test_first_and_last() {
    let mut store = new_store().await;
    let _lock = prep_test(&store).await;

    let interest_0 = random_interest(Some((&[], &[])), Some(42));
    let interest_1 = random_interest(Some((&[], &[])), Some(43));
    recon::Store::insert(&mut store, ReconItem::new_key(&interest_0))
        .await
        .unwrap();
    recon::Store::insert(&mut store, ReconItem::new_key(&interest_1))
        .await
        .unwrap();

    // Only one key in range, we expect to get the same key as first and last
    let ret = store
        .first_and_last(
            &random_interest(Some((&[], &[])), Some(40)),
            &random_interest(Some((&[], &[])), Some(43)),
        )
        .await
        .unwrap()
        .unwrap();

    expect![[r#"
            (
                Interest {
                    bytes: "480f70d652b6b825e4582200206ce9f954100188eb6e7939dbf45ac845d3399dba29da4e0b6ef1fdd8636a326b014040182a",
                    sort_key_hash: "0f70d652b6b825e4",
                    peer_id: PeerId(
                        "1AdgHpWeBKTU3F2tUkAQqL2Y2Geh4QgHJwcWMPuiY1qiRQ",
                    ),
                    range: RangeOpen {
                        start: Ok(
                            EventId {
                                bytes: "MIN",
                            },
                        ),
                        end: Ok(
                            EventId {
                                bytes: "MIN",
                            },
                        ),
                    },
                    not_after: 42,
                },
                Interest {
                    bytes: "480f70d652b6b825e4582200206ce9f954100188eb6e7939dbf45ac845d3399dba29da4e0b6ef1fdd8636a326b014040182a",
                    sort_key_hash: "0f70d652b6b825e4",
                    peer_id: PeerId(
                        "1AdgHpWeBKTU3F2tUkAQqL2Y2Geh4QgHJwcWMPuiY1qiRQ",
                    ),
                    range: RangeOpen {
                        start: Ok(
                            EventId {
                                bytes: "MIN",
                            },
                        ),
                        end: Ok(
                            EventId {
                                bytes: "MIN",
                            },
                        ),
                    },
                    not_after: 42,
                },
            )
        "#]]
        .assert_debug_eq(&ret);

    // No keys in range
    let ret = store
        .first_and_last(
            &random_interest(Some((&[], &[])), Some(50)),
            &random_interest(Some((&[], &[])), Some(53)),
        )
        .await
        .unwrap();
    expect![[r#"
            None
        "#]]
    .assert_debug_eq(&ret);

    // Two keys in range
    let ret = store
        .first_and_last(
            &random_interest(Some((&[], &[])), Some(40)),
            &random_interest(Some((&[], &[])), Some(50)),
        )
        .await
        .unwrap()
        .unwrap();
    // both keys exist
    expect![[r#"
            (
                Interest {
                    bytes: "480f70d652b6b825e4582200206ce9f954100188eb6e7939dbf45ac845d3399dba29da4e0b6ef1fdd8636a326b014040182a",
                    sort_key_hash: "0f70d652b6b825e4",
                    peer_id: PeerId(
                        "1AdgHpWeBKTU3F2tUkAQqL2Y2Geh4QgHJwcWMPuiY1qiRQ",
                    ),
                    range: RangeOpen {
                        start: Ok(
                            EventId {
                                bytes: "MIN",
                            },
                        ),
                        end: Ok(
                            EventId {
                                bytes: "MIN",
                            },
                        ),
                    },
                    not_after: 42,
                },
                Interest {
                    bytes: "480f70d652b6b825e4582200206ce9f954100188eb6e7939dbf45ac845d3399dba29da4e0b6ef1fdd8636a326b014040182b",
                    sort_key_hash: "0f70d652b6b825e4",
                    peer_id: PeerId(
                        "1AdgHpWeBKTU3F2tUkAQqL2Y2Geh4QgHJwcWMPuiY1qiRQ",
                    ),
                    range: RangeOpen {
                        start: Ok(
                            EventId {
                                bytes: "MIN",
                            },
                        ),
                        end: Ok(
                            EventId {
                                bytes: "MIN",
                            },
                        ),
                    },
                    not_after: 43,
                },
            )
        "#]]
        .assert_debug_eq(&ret);
}

#[test(tokio::test)]
#[should_panic(expected = "Interests do not support values! Invalid request.")]
async fn test_store_value_for_key_error() {
    let mut store = new_store().await;
    let _lock = prep_test(&store).await;
    let key = random_interest(None, None);
    let store_value = random_interest(None, None);
    recon::Store::insert(
        &mut store,
        ReconItem::new_with_value(&key, store_value.as_slice()),
    )
    .await
    .unwrap();
}

#[test(tokio::test)]
async fn test_keys_with_missing_value() {
    let mut store = new_store().await;
    let _lock = prep_test(&store).await;
    let key = random_interest(None, None);
    recon::Store::insert(&mut store, ReconItem::new(&key, None))
        .await
        .unwrap();
    let missing_keys = store
        .keys_with_missing_values((Interest::min_value(), Interest::max_value()).into())
        .await
        .unwrap();
    expect![[r#"
            []
        "#]]
    .assert_debug_eq(&missing_keys);

    recon::Store::insert(&mut store, ReconItem::new(&key, Some(&[])))
        .await
        .unwrap();
    let missing_keys = store
        .keys_with_missing_values((Interest::min_value(), Interest::max_value()).into())
        .await
        .unwrap();
    expect![[r#"
            []
        "#]]
    .assert_debug_eq(&missing_keys);
}

#[test(tokio::test)]
async fn test_value_for_key() {
    let mut store = new_store().await;
    let _lock = prep_test(&store).await;
    let key = random_interest(None, None);
    recon::Store::insert(&mut store, ReconItem::new(&key, None))
        .await
        .unwrap();
    let value = store.value_for_key(&key).await.unwrap();
    let val = value.unwrap();
    let empty: Vec<u8> = vec![];
    assert_eq!(empty, val);
}
