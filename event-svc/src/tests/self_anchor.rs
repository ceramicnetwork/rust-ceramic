//! Tests for self-anchored events via anchor-evm.
//!
//! These tests verify that chain inclusion proofs created by self-anchoring
//! can be correctly stored and retrieved for event validation.

use ceramic_anchor_service::ChainInclusionData;
use ceramic_sql::sqlite::SqlitePool;
use cid::Cid;
use multihash_codetable::{Code, MultihashDigest};

use crate::blockchain::tx_hash_try_from_cid;
use crate::store::{ChainProof, EventAccess};

/// Ethereum transaction codec for IPLD (from multicodec table)
/// This matches the constant in anchor-evm/src/proof_builder.rs
const ETH_TX_CODEC: u64 = 0x93;

/// Simulate what ProofBuilder::tx_hash_to_cid does in anchor-evm
fn tx_hash_to_cid(tx_hash: &str) -> Cid {
    let hex_str = tx_hash.strip_prefix("0x").unwrap_or(tx_hash);
    let tx_bytes = hex::decode(hex_str).unwrap();
    let multihash = Code::Keccak256.wrap(&tx_bytes).unwrap();
    Cid::new_v1(ETH_TX_CODEC, multihash)
}

/// Test that chain proofs persisted by self-anchoring can be retrieved by the lookup path.
///
/// This is the critical round-trip test that verifies:
/// 1. Self-anchoring stores chain proof with tx_hash as "0x{hex}"
/// 2. Validation lookup converts tx_hash CID back to "0x{hex}"
/// 3. The lookup finds the stored proof
#[tokio::test]
async fn test_self_anchored_chain_proof_roundtrip() {
    let pool = SqlitePool::connect_in_memory().await.unwrap();
    let event_access = EventAccess::try_new(pool).await.unwrap();

    // Create chain inclusion data as self-anchoring would
    let tx_hash = "0xa1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2";
    let chain_id = "eip155:100";
    let block_hash = "0xdef456abc123def456abc123def456abc123def456abc123def456abc123def4";
    let timestamp = 1704067200u64;

    let chain_data = ChainInclusionData {
        chain_id: chain_id.to_string(),
        transaction_hash: tx_hash.to_string(),
        transaction_input: "0x97ad09eb".to_string() + &"ab".repeat(32),
        block_hash: block_hash.to_string(),
        timestamp,
    };

    // Persist as self-anchoring does (convert to ChainProof via From impl)
    let proof: ChainProof = chain_data.into();
    event_access
        .persist_chain_inclusion_proofs(&[proof])
        .await
        .unwrap();

    // Lookup as validation does - directly with the tx_hash string
    let found = event_access
        .get_chain_proof(chain_id, tx_hash)
        .await
        .unwrap();

    assert!(
        found.is_some(),
        "Chain proof should be found for self-anchored event"
    );
    let found = found.unwrap();
    assert_eq!(found.chain_id, chain_id);
    assert_eq!(found.transaction_hash, tx_hash);
    assert_eq!(found.block_hash, block_hash);
    assert_eq!(found.timestamp, timestamp as i64);
}

/// Test the complete discover_chain_proof path for self-anchored events.
///
/// This simulates the actual validation flow:
/// 1. Self-anchoring creates a proof with tx_hash and stores ChainInclusionData
/// 2. Later, validation calls discover_chain_proof with a TimeEvent
/// 3. discover_chain_proof extracts tx_hash CID from the proof, converts to hex, and looks up
/// 4. The lookup succeeds and returns the stored chain proof
#[tokio::test]
async fn test_discover_chain_proof_for_self_anchored_event() {
    let pool = SqlitePool::connect_in_memory().await.unwrap();
    let event_access = EventAccess::try_new(pool).await.unwrap();

    // Step 1: Simulate what anchor-evm does
    // The actual blockchain tx_hash (as returned by the EVM)
    let actual_tx_hash = "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
    let chain_id = "eip155:100";
    let timestamp = 1704067200u64;

    // ProofBuilder creates a tx_hash CID from the actual tx_hash
    let tx_hash_cid = tx_hash_to_cid(actual_tx_hash);

    // Self-anchoring creates and stores ChainInclusionData
    let chain_data = ChainInclusionData {
        chain_id: chain_id.to_string(),
        transaction_hash: actual_tx_hash.to_string(),
        transaction_input: "0x97ad09eb".to_string() + &"cd".repeat(32),
        block_hash: "0x".to_string() + &"ef".repeat(32),
        timestamp,
    };

    let proof: ChainProof = chain_data.into();
    event_access
        .persist_chain_inclusion_proofs(&[proof])
        .await
        .unwrap();

    // Step 2: Simulate what discover_chain_proof does
    // It receives a TimeEvent with proof.tx_hash() returning the tx_hash_cid
    // It converts the CID back to a tx_hash string for lookup:
    let lookup_tx_hash = tx_hash_try_from_cid(tx_hash_cid).unwrap().to_string();

    // Step 3: Perform the lookup
    let found = event_access
        .get_chain_proof(chain_id, &lookup_tx_hash)
        .await
        .unwrap();

    // Step 4: Verify lookup succeeds
    assert!(
        found.is_some(),
        "Chain proof should be found via tx_hash CID round-trip. \
         Original tx_hash: {}, Lookup tx_hash: {}",
        actual_tx_hash,
        lookup_tx_hash
    );

    let found = found.unwrap();
    assert_eq!(found.chain_id, chain_id);
    assert_eq!(found.timestamp, timestamp as i64);

    // Verify the tx_hash formats match
    assert_eq!(
        actual_tx_hash.to_lowercase(),
        lookup_tx_hash.to_lowercase(),
        "Self-anchored tx_hash must match validation lookup tx_hash"
    );
}

/// Test that chain proof upserts are idempotent.
///
/// This verifies that persisting the same chain proof multiple times
/// (e.g., due to retries) doesn't cause errors or duplicate data.
#[tokio::test]
async fn test_chain_proof_upsert_idempotent() {
    let pool = SqlitePool::connect_in_memory().await.unwrap();
    let event_access = EventAccess::try_new(pool).await.unwrap();

    let chain_data = ChainInclusionData {
        chain_id: "eip155:100".to_string(),
        transaction_hash: "0x".to_string() + &"ab".repeat(32),
        transaction_input: "0x97ad09eb".to_string() + &"cd".repeat(32),
        block_hash: "0x".to_string() + &"ef".repeat(32),
        timestamp: 1704067200,
    };

    let proof: ChainProof = chain_data.clone().into();

    // Persist the same proof multiple times (simulating retries)
    for _ in 0..3 {
        event_access
            .persist_chain_inclusion_proofs(&[proof.clone()])
            .await
            .unwrap();
    }

    // Verify we can still look it up
    let found = event_access
        .get_chain_proof("eip155:100", &("0x".to_string() + &"ab".repeat(32)))
        .await
        .unwrap();

    assert!(found.is_some());
    assert_eq!(found.unwrap().timestamp, 1704067200);
}

/// Test chain proof lookup with different chain IDs.
///
/// Verifies that chain proofs are correctly scoped by chain_id.
#[tokio::test]
async fn test_chain_proof_scoped_by_chain_id() {
    let pool = SqlitePool::connect_in_memory().await.unwrap();
    let event_access = EventAccess::try_new(pool).await.unwrap();

    let tx_hash = "0x".to_string() + &"ab".repeat(32);

    // Store proof for chain 100
    let chain_data_100 = ChainInclusionData {
        chain_id: "eip155:100".to_string(),
        transaction_hash: tx_hash.clone(),
        transaction_input: "0x97ad09eb".to_string() + &"cd".repeat(32),
        block_hash: "0x".to_string() + &"ef".repeat(32),
        timestamp: 1000,
    };

    // Store proof for chain 137 with same tx_hash
    let chain_data_137 = ChainInclusionData {
        chain_id: "eip155:137".to_string(),
        transaction_hash: tx_hash.clone(),
        transaction_input: "0x97ad09eb".to_string() + &"de".repeat(32),
        block_hash: "0x".to_string() + &"12".repeat(32),
        timestamp: 2000,
    };

    let proof_100: ChainProof = chain_data_100.into();
    let proof_137: ChainProof = chain_data_137.into();

    event_access
        .persist_chain_inclusion_proofs(&[proof_100, proof_137])
        .await
        .unwrap();

    // Lookup for chain 100 should return timestamp 1000
    let found_100 = event_access
        .get_chain_proof("eip155:100", &tx_hash)
        .await
        .unwrap();
    assert_eq!(found_100.unwrap().timestamp, 1000);

    // Lookup for chain 137 should return timestamp 2000
    let found_137 = event_access
        .get_chain_proof("eip155:137", &tx_hash)
        .await
        .unwrap();
    assert_eq!(found_137.unwrap().timestamp, 2000);

    // Lookup for non-existent chain should return None
    let found_1 = event_access
        .get_chain_proof("eip155:1", &tx_hash)
        .await
        .unwrap();
    assert!(found_1.is_none());
}

/// Test that looking up a non-existent chain proof returns None.
#[tokio::test]
async fn test_chain_proof_not_found() {
    let pool = SqlitePool::connect_in_memory().await.unwrap();
    let event_access = EventAccess::try_new(pool).await.unwrap();

    let found = event_access
        .get_chain_proof("eip155:100", "0xnonexistent")
        .await
        .unwrap();

    assert!(found.is_none());
}

/// Test that Store::insert_many correctly persists chain inclusion proofs.
///
/// This tests the full integration through the ceramic_anchor_service::Store trait,
/// verifying that when TimeEventInsertable items have chain_inclusion data,
/// the chain proof is persisted before the events.
#[tokio::test]
async fn test_store_insert_many_persists_chain_proof() {
    use ceramic_anchor_service::{Store, TimeEventInsertable};
    use ceramic_core::{EventId, Network, NodeKey, StreamId};
    use ceramic_event::unvalidated;

    use crate::{EventService, UndeliveredEventReview};

    let pool = SqlitePool::connect_in_memory().await.unwrap();

    // Create EventService (which implements Store)
    let service = EventService::try_new(pool.clone(), UndeliveredEventReview::Skip, false, vec![])
        .await
        .unwrap();

    // Create test data: a TimeEvent with chain_inclusion
    let tx_hash = "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
    let chain_id = "eip155:100";
    let timestamp = 1704067200u64;

    // Create tx_hash CID (simulating what ProofBuilder does)
    let tx_hash_cid = tx_hash_to_cid(tx_hash);

    // Create a minimal time event
    let init_cid = super::deterministic_cid(b"test init cid");
    let prev_cid = super::deterministic_cid(b"test prev cid");

    let time_event = unvalidated::Builder::time()
        .with_id(init_cid)
        .with_tx(chain_id.to_string(), tx_hash_cid, "f(bytes32)".to_string())
        .with_prev(prev_cid)
        .build()
        .expect("test time event should build");

    let time_event_cid = time_event.to_cid().expect("time event should encode");

    // Create a model StreamId for the EventId
    let model = StreamId::document(super::random_cid());
    let event_id = EventId::builder()
        .with_network(&Network::DevUnstable)
        .with_sep("model", &model.to_vec())
        .with_controller(super::CONTROLLER)
        .with_init(&init_cid)
        .with_event(&time_event_cid)
        .build();

    // Create ChainInclusionData (simulating what anchor-evm produces)
    let chain_data = ChainInclusionData {
        chain_id: chain_id.to_string(),
        transaction_hash: tx_hash.to_string(),
        transaction_input: "0x97ad09eb".to_string() + &"cd".repeat(32),
        block_hash: "0x".to_string() + &"ef".repeat(32),
        timestamp,
    };

    let insertable = TimeEventInsertable {
        event_id,
        cid: time_event_cid,
        event: time_event,
        chain_inclusion: Some(chain_data),
    };

    // Call Store::insert_many
    Store::insert_many(&service, vec![insertable], NodeKey::random().id())
        .await
        .unwrap();

    // Verify the chain proof was persisted
    let event_access = EventAccess::try_new(pool).await.unwrap();
    let found = event_access
        .get_chain_proof(chain_id, tx_hash)
        .await
        .unwrap();

    assert!(
        found.is_some(),
        "Chain proof should be persisted by Store::insert_many"
    );
    let found = found.unwrap();
    assert_eq!(found.chain_id, chain_id);
    assert_eq!(found.transaction_hash, tx_hash);
    assert_eq!(found.timestamp, timestamp as i64);
}

/// Test that Store::insert_many works without chain_inclusion (remote CAS case).
#[tokio::test]
async fn test_store_insert_many_without_chain_inclusion() {
    use ceramic_anchor_service::{Store, TimeEventInsertable};
    use ceramic_core::{EventId, Network, NodeKey, StreamId};
    use ceramic_event::unvalidated;

    use crate::{EventService, UndeliveredEventReview};

    let pool = SqlitePool::connect_in_memory().await.unwrap();

    let service = EventService::try_new(pool.clone(), UndeliveredEventReview::Skip, false, vec![])
        .await
        .unwrap();

    // Create test data without chain_inclusion (simulating remote CAS)
    let tx_hash_cid =
        tx_hash_to_cid("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");
    let init_cid = super::deterministic_cid(b"test init cid 2");
    let prev_cid = super::deterministic_cid(b"test prev cid 2");

    let time_event = unvalidated::Builder::time()
        .with_id(init_cid)
        .with_tx(
            "eip155:1".to_string(),
            tx_hash_cid,
            "f(bytes32)".to_string(),
        )
        .with_prev(prev_cid)
        .build()
        .expect("test time event should build");

    let time_event_cid = time_event.to_cid().expect("time event should encode");

    let model = StreamId::document(super::random_cid());
    let event_id = EventId::builder()
        .with_network(&Network::DevUnstable)
        .with_sep("model", &model.to_vec())
        .with_controller(super::CONTROLLER)
        .with_init(&init_cid)
        .with_event(&time_event_cid)
        .build();

    let insertable = TimeEventInsertable {
        event_id,
        cid: time_event_cid,
        event: time_event,
        chain_inclusion: None, // No chain inclusion for remote CAS
    };

    // Should succeed without error even without chain_inclusion
    Store::insert_many(&service, vec![insertable], NodeKey::random().id())
        .await
        .unwrap();

    // Verify no chain proof was persisted
    let event_access = EventAccess::try_new(pool).await.unwrap();
    let found = event_access
        .get_chain_proof(
            "eip155:1",
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        )
        .await
        .unwrap();

    assert!(
        found.is_none(),
        "No chain proof should be persisted when chain_inclusion is None"
    );
}

/// Test that verifies ordering invariant: chain proofs and events both exist after Store::insert_many.
///
/// The implementation in event/store.rs persists chain proofs BEFORE events.
/// This ordering is important because:
/// 1. If the process crashes after proof persistence but before event insertion,
///    we have an orphaned proof (harmless - just extra data).
/// 2. If the order were reversed (events first), a crash would leave events
///    without chain proofs, causing validation failures.
///
/// This test verifies both artifacts exist after successful insertion, which
/// implicitly validates that the proof persistence completed (since we can query it).
#[tokio::test]
async fn test_store_insert_many_ordering_invariant() {
    use ceramic_anchor_service::{Store, TimeEventInsertable};
    use ceramic_core::{EventId, Network, NodeKey, StreamId};
    use ceramic_event::unvalidated;

    use crate::{EventService, UndeliveredEventReview};

    let pool = SqlitePool::connect_in_memory().await.unwrap();
    let service = EventService::try_new(pool.clone(), UndeliveredEventReview::Skip, false, vec![])
        .await
        .unwrap();

    // Create multiple time events in a batch
    let tx_hash = "0x1111111111111111111111111111111111111111111111111111111111111111";
    let chain_id = "eip155:100";
    let timestamp = 1704067200u64;
    let tx_hash_cid = tx_hash_to_cid(tx_hash);

    let mut insertables = Vec::new();
    let mut event_cids = Vec::new();

    // Create 3 time events in the same batch (sharing the same chain proof)
    for i in 0..3 {
        let init_cid = super::deterministic_cid(format!("ordering test init {}", i).as_bytes());
        let prev_cid = super::deterministic_cid(format!("ordering test prev {}", i).as_bytes());

        let time_event = unvalidated::Builder::time()
            .with_id(init_cid)
            .with_tx(chain_id.to_string(), tx_hash_cid, "f(bytes32)".to_string())
            .with_prev(prev_cid)
            .build()
            .expect("test time event should build");

        let time_event_cid = time_event.to_cid().expect("time event should encode");
        event_cids.push(time_event_cid);

        let model = StreamId::document(super::random_cid());
        let event_id = EventId::builder()
            .with_network(&Network::DevUnstable)
            .with_sep("model", &model.to_vec())
            .with_controller(super::CONTROLLER)
            .with_init(&init_cid)
            .with_event(&time_event_cid)
            .build();

        // All items in a batch share the same chain_inclusion (cloned from TimeEventBatch).
        // Store::insert_many uses find_map to extract any one of them (they're all identical).
        let chain_inclusion = Some(ChainInclusionData {
            chain_id: chain_id.to_string(),
            transaction_hash: tx_hash.to_string(),
            transaction_input: "0x97ad09eb".to_string() + &"11".repeat(32),
            block_hash: "0x".to_string() + &"22".repeat(32),
            timestamp,
        });

        insertables.push(TimeEventInsertable {
            event_id,
            cid: time_event_cid,
            event: time_event,
            chain_inclusion,
        });
    }

    // Insert the batch
    Store::insert_many(&service, insertables, NodeKey::random().id())
        .await
        .unwrap();

    // Verify the chain proof exists (proves proof persistence completed)
    let event_access = EventAccess::try_new(pool.clone()).await.unwrap();
    let found_proof = event_access
        .get_chain_proof(chain_id, tx_hash)
        .await
        .unwrap();

    assert!(
        found_proof.is_some(),
        "Chain proof must exist after Store::insert_many completes"
    );

    // Verify all events exist (proves event persistence completed)
    for (i, event_cid) in event_cids.iter().enumerate() {
        let (exists, _delivered) = event_access.deliverable_by_cid(event_cid).await.unwrap();
        assert!(
            exists,
            "Event {} with CID {} must exist after Store::insert_many completes",
            i, event_cid
        );
    }
}

/// Test that EventService::discover_chain_proof correctly retrieves chain proofs for self-anchored events.
///
/// This tests the actual production code path used by the pipeline's ConclusionFeed,
/// verifying that:
/// 1. Chain proof is stored via Store::insert_many (self-anchoring path)
/// 2. discover_chain_proof extracts tx_hash from TimeEvent.proof().tx_hash()
/// 3. discover_chain_proof converts the CID to hex and looks up the proof
/// 4. The returned proof matches what was stored
#[tokio::test]
async fn test_discover_chain_proof_direct() {
    use ceramic_anchor_service::{Store, TimeEventInsertable};
    use ceramic_core::{EventId, Network, NodeKey, StreamId};
    use ceramic_event::unvalidated;

    use crate::{EventService, UndeliveredEventReview};

    let pool = SqlitePool::connect_in_memory().await.unwrap();
    let service = EventService::try_new(pool.clone(), UndeliveredEventReview::Skip, false, vec![])
        .await
        .unwrap();

    // Set up test data
    let tx_hash = "0xfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeed";
    let chain_id = "eip155:100";
    let timestamp = 1704067200u64;
    let block_hash = "0x".to_string() + &"aa".repeat(32);
    let tx_input = "0x97ad09eb".to_string() + &"bb".repeat(32);

    // Create tx_hash CID (simulating what ProofBuilder does)
    let tx_hash_cid = tx_hash_to_cid(tx_hash);

    // Create a time event with this tx_hash CID
    let init_cid = super::deterministic_cid(b"discover_chain_proof test init");
    let prev_cid = super::deterministic_cid(b"discover_chain_proof test prev");

    // Create a time event for insertion
    let time_event_for_insert = unvalidated::Builder::time()
        .with_id(init_cid)
        .with_tx(chain_id.to_string(), tx_hash_cid, "f(bytes32)".to_string())
        .with_prev(prev_cid)
        .build()
        .expect("test time event should build");

    let time_event_cid = time_event_for_insert
        .to_cid()
        .expect("time event should encode");

    // Create EventId and ChainInclusionData
    let model = StreamId::document(super::random_cid());
    let event_id = EventId::builder()
        .with_network(&Network::DevUnstable)
        .with_sep("model", &model.to_vec())
        .with_controller(super::CONTROLLER)
        .with_init(&init_cid)
        .with_event(&time_event_cid)
        .build();

    let chain_data = ChainInclusionData {
        chain_id: chain_id.to_string(),
        transaction_hash: tx_hash.to_string(),
        transaction_input: tx_input.clone(),
        block_hash: block_hash.clone(),
        timestamp,
    };

    let insertable = TimeEventInsertable {
        event_id,
        cid: time_event_cid,
        event: time_event_for_insert,
        chain_inclusion: Some(chain_data),
    };

    // Insert via Store::insert_many (the self-anchoring path)
    Store::insert_many(&service, vec![insertable], NodeKey::random().id())
        .await
        .unwrap();

    // Create an identical time event for the lookup (TimeEvent doesn't impl Clone)
    let time_event_for_lookup = unvalidated::Builder::time()
        .with_id(init_cid)
        .with_tx(chain_id.to_string(), tx_hash_cid, "f(bytes32)".to_string())
        .with_prev(prev_cid)
        .build()
        .expect("test time event should build");

    // Now call discover_chain_proof directly on the EventService
    let discovered = service.discover_chain_proof(&time_event_for_lookup).await;

    assert!(
        discovered.is_ok(),
        "discover_chain_proof should succeed for self-anchored event: {:?}",
        discovered.err()
    );

    let proof = discovered.unwrap();
    assert_eq!(proof.chain_id, chain_id);
    assert_eq!(proof.transaction_hash, tx_hash);
    assert_eq!(proof.transaction_input, tx_input);
    assert_eq!(proof.block_hash, block_hash);
    assert_eq!(proof.timestamp, timestamp as i64);
}

/// Test that discover_chain_proof falls back to RPC when proof not in DB,
/// and returns NoChainProvider error when no RPC provider is configured.
#[tokio::test]
async fn test_discover_chain_proof_fallback_no_provider() {
    use ceramic_event::unvalidated;

    use crate::{EventService, UndeliveredEventReview};

    let pool = SqlitePool::connect_in_memory().await.unwrap();
    // Service created with no RPC providers
    let service = EventService::try_new(pool.clone(), UndeliveredEventReview::Skip, false, vec![])
        .await
        .unwrap();

    // Create a time event for a non-existent chain proof
    let tx_hash_cid =
        tx_hash_to_cid("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");
    let init_cid = super::deterministic_cid(b"discover not found test init");
    let prev_cid = super::deterministic_cid(b"discover not found test prev");

    let time_event = unvalidated::Builder::time()
        .with_id(init_cid)
        .with_tx(
            "eip155:100".to_string(),
            tx_hash_cid,
            "f(bytes32)".to_string(),
        )
        .with_prev(prev_cid)
        .build()
        .expect("test time event should build");

    // Call discover_chain_proof - should fail because proof not in DB and no RPC provider
    let result = service.discover_chain_proof(&time_event).await;

    assert!(
        result.is_err(),
        "discover_chain_proof should fail when no RPC provider configured"
    );

    // Verify it's a NoChainProvider error (RPC fallback fails without provider)
    match result.unwrap_err() {
        crate::eth_rpc::Error::NoChainProvider(chain_id) => {
            assert_eq!(chain_id.to_string(), "eip155:100");
        }
        other => panic!("Expected NoChainProvider error, got: {:?}", other),
    }
}

/// Test that self-anchored events produce correct ConclusionTime events via ConclusionFeed.
///
/// This is the critical end-to-end test that verifies:
/// 1. Self-anchored TimeEvent is inserted via Store::insert_many with chain_inclusion
/// 2. Chain proof is persisted correctly
/// 3. ConclusionFeed's conclusion_events_since returns the event with correct TimeProof
/// 4. The TimeProof contains the correct before timestamp and chain_id
///
/// This exercises the production code path used by the pipeline/concluder.
#[tokio::test]
async fn test_self_anchored_events_in_conclusion_feed() {
    use ceramic_anchor_service::{Store, TimeEventInsertable};
    use ceramic_core::{EventId, Network, NodeKey};
    use ceramic_event::unvalidated;
    use ceramic_pipeline::ConclusionFeed;

    use crate::{EventService, UndeliveredEventReview};

    let pool = SqlitePool::connect_in_memory().await.unwrap();

    // Create EventService WITHOUT chain providers (self-anchoring doesn't use RPC)
    let service = EventService::try_new(pool.clone(), UndeliveredEventReview::Skip, false, vec![])
        .await
        .unwrap();

    // Set up test data
    let tx_hash = "0xfacefacefacefacefacefacefacefacefacefacefacefacefacefacefaceface";
    let chain_id = "eip155:100";
    let timestamp = 1704067200u64; // 2024-01-01 00:00:00 UTC

    // Create an init event first
    // Use the test helpers which generate properly typed events
    // Get init + data events using the helper (get_events returns init + 3 data events)
    let events = super::get_events().await;
    let init_event_id = events[0].key.clone();
    let init_cid = init_event_id.cid().unwrap();
    let data_event_id = events[1].key.clone();
    let data_cid = data_event_id.cid().unwrap();

    // Insert init and data events via recon Store (simulating normal event flow)
    recon::Store::insert_many(&service, &events[..2], NodeKey::random().id())
        .await
        .unwrap();

    // Now create a TimeEvent via the self-anchoring path
    let tx_hash_cid = tx_hash_to_cid(tx_hash);

    let time_event = unvalidated::Builder::time()
        .with_id(init_cid)
        .with_tx(chain_id.to_string(), tx_hash_cid, "f(bytes32)".to_string())
        .with_prev(data_cid)
        .build()
        .expect("test time event should build");

    let time_event_cid = time_event.to_cid().expect("time event should encode");

    // Build EventId using the same sep key bytes from the original event
    // (the events from get_events() use "model" sep with the stream's model CID)
    let time_event_id = EventId::builder()
        .with_network(&Network::DevUnstable)
        .with_sep(
            "model",
            init_event_id.separator().expect("events have separator"),
        )
        .with_controller(super::CONTROLLER)
        .with_init(&init_cid)
        .with_event(&time_event_cid)
        .build();

    // Create ChainInclusionData (simulating what anchor-evm produces)
    let chain_data = ChainInclusionData {
        chain_id: chain_id.to_string(),
        transaction_hash: tx_hash.to_string(),
        transaction_input: "0x97ad09eb".to_string() + &"fa".repeat(32),
        block_hash: "0x".to_string() + &"ce".repeat(32),
        timestamp,
    };

    let insertable = TimeEventInsertable {
        event_id: time_event_id,
        cid: time_event_cid,
        event: time_event,
        chain_inclusion: Some(chain_data),
    };

    // Insert via Store::insert_many (the self-anchoring path)
    Store::insert_many(&service, vec![insertable], NodeKey::random().id())
        .await
        .unwrap();

    // Now verify via ConclusionFeed
    let conclusion_events = service.conclusion_events_since(0, 10).await.unwrap();

    // Find the Time event in the results
    let time_conclusion = conclusion_events
        .iter()
        .find_map(|e| match e {
            ceramic_pipeline::ConclusionEvent::Time(t) => Some(t),
            _ => None,
        })
        .expect("Should have a Time conclusion event");

    // Verify the TimeProof has correct values from our ChainInclusionData
    assert_eq!(
        time_conclusion.time_proof.chain_id, chain_id,
        "TimeProof chain_id should match self-anchored chain_id"
    );
    assert_eq!(
        time_conclusion.time_proof.before, timestamp,
        "TimeProof.before timestamp should match self-anchored timestamp"
    );

    // Verify the event CID matches
    assert_eq!(
        time_conclusion.event_cid, time_event_cid,
        "Conclusion event CID should match the inserted time event"
    );

    // Verify the stream info is correct
    assert_eq!(
        time_conclusion.init.stream_cid, init_cid,
        "Stream CID should match the init event"
    );
}
