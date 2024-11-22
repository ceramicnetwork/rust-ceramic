use std::collections::BTreeSet;

use ceramic_core::{
    peer::{Builder, Init},
    NodeKey, PeerKey,
};
use rand::{thread_rng, Rng};
use recon::{ReconItem, Sha256a};
use test_log::test;

macro_rules! test_with_sqlite {
    ($test_name: ident, $test_fn: expr $(, $sql_stmts:expr)?) => {
        paste::paste! {
            #[test(tokio::test)]
            async fn [<$test_name _sqlite>]() {

                let conn = $crate::store::SqlitePool::connect_in_memory().await.unwrap();
                let service = $crate::PeerService::new(conn);
                $(
                    for stmt in $sql_stmts {
                        service.pool.run_statement(stmt).await.unwrap();
                    }
                )?
                $test_fn(&service).await;
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
pub(crate) fn peer_key_builder() -> Builder<Init> {
    PeerKey::builder()
}

// Generate an event for the same network,model,controller,stream
// The event and height are random when when its None.
pub(crate) fn random_peer_key<'a>(expiration: Option<u64>) -> PeerKey {
    peer_key_builder()
        .with_expiration(expiration.unwrap_or_else(|| thread_rng().gen()))
        .with_id(&NodeKey::random())
        .with_addresses(vec![
            format!("/ip4/127.0.0.1/tcp/{}", thread_rng().gen::<u16>())
                .parse()
                .unwrap(),
            format!("/ip4/127.0.0.1/udp/{}/quic-v1", thread_rng().gen::<u16>())
                .parse()
                .unwrap(),
        ])
        .build()
}
// The EventId that is the minumum of all possible random event ids
pub(crate) fn random_peer_min() -> PeerKey {
    peer_key_builder().with_min_expiration().build_fencepost()
}
// The EventId that is the maximum of all possible random event ids
pub(crate) fn random_peer_max() -> PeerKey {
    peer_key_builder().with_max_expiration().build_fencepost()
}

test_with_dbs!(
    test_hash_range_query,
    test_hash_range_query,
    ["delete from ceramic_one_interest"]
);

async fn test_hash_range_query<S>(store: &S)
where
    S: recon::Store<Key = PeerKey, Hash = Sha256a>,
{
    recon::Store::insert_many(
        store,
        &[ReconItem::new(random_peer_key(Some(42)), vec![])],
        NodeKey::random().id(),
    )
    .await
    .unwrap();

    let hash_cnt = store
        .hash_range(&random_peer_min()..&random_peer_max())
        .await
        .unwrap();
    assert_eq!(1, hash_cnt.count());

    recon::Store::insert_many(
        store,
        &[ReconItem::new(random_peer_key(Some(24)), vec![])],
        NodeKey::random().id(),
    )
    .await
    .unwrap();

    let hash_cnt = store
        .hash_range(&random_peer_min()..&random_peer_max())
        .await
        .unwrap();
    assert_eq!(2, hash_cnt.count());
}

test_with_dbs!(
    test_range_query,
    test_range_query,
    ["delete from ceramic_one_interest"]
);

async fn test_range_query<S>(store: &S)
where
    S: recon::Store<Key = PeerKey, Hash = Sha256a>,
{
    let interest_0 = random_peer_key(None);
    let interest_1 = random_peer_key(None);

    recon::Store::insert_many(
        store,
        &[ReconItem::new(interest_0.clone(), Vec::new())],
        NodeKey::random().id(),
    )
    .await
    .unwrap();
    recon::Store::insert_many(
        store,
        &[ReconItem::new(interest_1.clone(), Vec::new())],
        NodeKey::random().id(),
    )
    .await
    .unwrap();
    let ids = recon::Store::range(store, &random_peer_min()..&random_peer_max(), 0, usize::MAX)
        .await
        .unwrap();
    let interests = ids.collect::<BTreeSet<PeerKey>>();
    assert_eq!(BTreeSet::from_iter([interest_0, interest_1]), interests);
}

test_with_dbs!(
    test_range_with_values_query,
    test_range_with_values_query,
    ["delete from ceramic_one_interest"]
);

async fn test_range_with_values_query<S>(store: &S)
where
    S: recon::Store<Key = PeerKey, Hash = Sha256a>,
{
    let interest_0 = random_peer_key(None);
    let interest_1 = random_peer_key(None);

    store
        .insert_many(
            &[ReconItem::new(interest_0.clone(), Vec::new())],
            NodeKey::random().id(),
        )
        .await
        .unwrap();
    store
        .insert_many(
            &[ReconItem::new(interest_1.clone(), Vec::new())],
            NodeKey::random().id(),
        )
        .await
        .unwrap();
    let ids = store
        .range_with_values(&random_peer_min()..&random_peer_max(), 0, usize::MAX)
        .await
        .unwrap();
    let interests = ids
        .into_iter()
        .map(|(i, _v)| i)
        .collect::<BTreeSet<PeerKey>>();
    assert_eq!(BTreeSet::from_iter([interest_0, interest_1]), interests);
}

test_with_dbs!(
    test_double_insert,
    test_double_insert,
    ["delete from ceramic_one_interest"]
);

async fn test_double_insert<S>(store: &S)
where
    S: recon::Store<Key = PeerKey, Hash = Sha256a>,
{
    let interest = random_peer_key(None);
    // do take the first one
    assert!(&recon::Store::insert_many(
        store,
        &[ReconItem::new(interest.clone(), Vec::new())],
        NodeKey::random().id(),
    )
    .await
    .unwrap()
    .included_new_key());

    // reject the second insert of same key
    assert!(!recon::Store::insert_many(
        store,
        &[ReconItem::new(interest.clone(), Vec::new())],
        NodeKey::random().id(),
    )
    .await
    .unwrap()
    .included_new_key());
}

test_with_dbs!(
    test_value_for_key,
    test_value_for_key,
    ["delete from ceramic_one_interest"]
);

async fn test_value_for_key<S>(store: &S)
where
    S: recon::Store<Key = PeerKey, Hash = Sha256a>,
{
    let key = random_peer_key(None);
    recon::Store::insert_many(
        store,
        &[ReconItem::new(key.clone(), Vec::new())],
        NodeKey::random().id(),
    )
    .await
    .unwrap();
    let value = store.value_for_key(&key).await.unwrap();
    let val = value.unwrap();
    let empty: Vec<u8> = vec![];
    assert_eq!(empty, val);
}

test_with_dbs!(
    test_insert,
    test_insert,
    ["delete from ceramic_one_interest"]
);

async fn test_insert<S>(service: &S)
where
    S: ceramic_p2p::PeerService,
{
    let key_0 = random_peer_key(Some(42));
    let key_1 = random_peer_key(Some(43));
    service.insert(&key_0).await.unwrap();
    let peers = service.all_peers().await.unwrap();
    assert_eq!(vec![key_0.clone()], peers);
    service.insert(&key_1).await.unwrap();
    let peers = service.all_peers().await.unwrap();
    assert_eq!(vec![key_0, key_1], peers);
}

test_with_dbs!(
    test_insert_delete,
    test_insert_delete,
    ["delete from ceramic_one_interest"]
);

async fn test_insert_delete<S>(service: &S)
where
    S: ceramic_p2p::PeerService,
{
    let key_0 = random_peer_key(Some(42));
    let key_1 = random_peer_key(Some(43));
    service.insert(&key_0).await.unwrap();
    let peers = service.all_peers().await.unwrap();
    assert_eq!(vec![key_0.clone()], peers);
    service.insert(&key_1).await.unwrap();
    let peers = service.all_peers().await.unwrap();
    assert_eq!(vec![key_0.clone(), key_1.clone()], peers);
    // Delete key_0
    service
        .delete_range(&random_peer_min()..&key_1)
        .await
        .unwrap();
    let peers = service.all_peers().await.unwrap();
    assert_eq!(vec![key_1.clone()], peers);
}
