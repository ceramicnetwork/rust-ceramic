// Tests for server.rs api implementations

use std::{ops::Range, str::FromStr, sync::Arc};

use crate::server::{decode_multibase_data, BuildResponse, Server};
use crate::{
    ApiItem, EventDataResult, EventInsertResult, EventService, IncludeEventData, InterestService,
};

use anyhow::Result;
use async_trait::async_trait;
use ceramic_api_server::Api;
use ceramic_api_server::{
    models::{self},
    EventsEventIdGetResponse, EventsPostResponse, ExperimentalEventsSepSepValueGetResponse,
    InterestsPostResponse, InterestsSortKeySortValuePostResponse,
};
use ceramic_core::{Cid, Interest};
use ceramic_core::{EventId, Network, PeerId, StreamId};
use expect_test::expect;
use mockall::{mock, predicate};
use multibase::Base;
use recon::Key;
use test_log::test;

struct Context;

pub const SIGNED_INIT_EVENT_CID: &str =
    "bagcqcerar2aga7747dm6fota3iipogz4q55gkaamcx2weebs6emvtvie2oha";
pub const SIGNED_INIT_EVENT_PAYLOAD_CID: &str =
    "bafyreiaroclcgqih242byss6pneufencrulmeex2ttfdzefst67agwq3im";
pub const SIGNED_INIT_EVENT_CAR: &str = "
        uO6Jlcm9vdHOB2CpYJgABhQESII6AYH_8-NniumDaEPcbPId6ZQAMFfViEDLxGVnVBNOOZ3ZlcnNpb24
        B0QEBcRIgEXCWI0EH1zQcSl57SUKRoo0WwhL6nMo8kLKfvgNaG0OiZGRhdGGhZXN0ZXBoGQFNZmhlYWR
        lcqRjc2VwZW1vZGVsZW1vZGVsWCjOAQIBhQESIKDoMqM144vTQLQ6DwKZvzxRWg_DPeTNeRCkPouTHo1
        YZnVuaXF1ZUxEpvE6skELu2qFaN5rY29udHJvbGxlcnOBeDhkaWQ6a2V5Ono2TWt0QnluQVBMckV5ZVM
        3cFZ0aGJpeVNjbWZ1OG41Vjdib1hneHlvNXEzU1pSUroCAYUBEiCOgGB__PjZ4rpg2hD3GzyHemUADBX
        1YhAy8RlZ1QTTjqJncGF5bG9hZFgkAXESIBFwliNBB9c0HEpee0lCkaKNFsIS-pzKPJCyn74DWhtDanN
        pZ25hdHVyZXOBomlwcm90ZWN0ZWRYgXsiYWxnIjoiRWREU0EiLCJraWQiOiJkaWQ6a2V5Ono2TWt0Qnl
        uQVBMckV5ZVM3cFZ0aGJpeVNjbWZ1OG41Vjdib1hneHlvNXEzU1pSUiN6Nk1rdEJ5bkFQTHJFeWVTN3B
        WdGhiaXlTY21mdThuNVY3Ym9YZ3h5bzVxM1NaUlIifWlzaWduYXR1cmVYQCQDjlx8fT8rbTR4088HtOE
        27LJMc38DSuf1_XtK14hDp1Q6vhHqnuiobqp5EqNOp0vNFCCzwgG-Dsjmes9jJww";

pub const SIGNED_INIT_EVENT: &str = "
        uomdwYXlsb2FkWCQBcRIgEXCWI0EH1zQcSl57SUKRoo0WwhL6nMo8kLKfvgNaG0Nqc2lnbmF0dXJlc4G
        iaXByb3RlY3RlZFiBeyJhbGciOiJFZERTQSIsImtpZCI6ImRpZDprZXk6ejZNa3RCeW5BUExyRXllUzd
        wVnRoYml5U2NtZnU4bjVWN2JvWGd4eW81cTNTWlJSI3o2TWt0QnluQVBMckV5ZVM3cFZ0aGJpeVNjbWZ
        1OG41Vjdib1hneHlvNXEzU1pSUiJ9aXNpZ25hdHVyZVhAJAOOXHx9PyttNHjTzwe04TbsskxzfwNK5_X
        9e0rXiEOnVDq-Eeqe6KhuqnkSo06nS80UILPCAb4OyOZ6z2MnDA";
pub const SIGNED_INIT_EVENT_PAYLOAD: &str = "uomRkYXRhoWVzdGVwaBkBTWZoZWFkZXKkY3NlcGVtb2RlbGVtb2RlbFgozgECAYUBEiCg6DKjNeOL00C0Og8Cmb88UVoPwz3kzXkQpD6Lkx6NWGZ1bmlxdWVMRKbxOrJBC7tqhWjea2NvbnRyb2xsZXJzgXg4ZGlkOmtleTp6Nk1rdEJ5bkFQTHJFeWVTN3BWdGhiaXlTY21mdThuNVY3Ym9YZ3h5bzVxM1NaUlI";

pub const UNSIGNED_INIT_EVENT_CID: &str =
    "bafyreiakimdaub7m6inx2nljypdhvhu5vozjhylqukif4hjxt65qnkv6my";

pub const UNSIGNED_INIT_EVENT_CAR: &str = "
        uOqJlcm9vdHOB2CpYJQABcRIgCkMGCgfs8ht9NWnDxnqenauyk-FwopBeHTefuwaqvmZndmVyc2lvbgHDAQFxEiAKQwYKB-zyG301acPGep6dq7KT4XCikF4dN5-7Bqq-ZqJkZGF0YfZmaGVhZGVypGNzZXBlbW9kZWxlbW9kZWxYKM4BAgGFARIghHTHRYxxeQXgc9Q6LUJVelzW5bnrw9TWgoBJlBIOVtdmdW5pcXVlR2Zvb3xiYXJrY29udHJvbGxlcnOBeDhkaWQ6a2V5Ono2TWt0Q0ZSY3dMUkZRQTlXYmVEUk03VzdrYkJkWlRIUTJ4blBneXhaTHExZ0NwSw";

pub const UNSIGNED_INIT_EVENT_PAYLOAD: &str = "uomRkYXRh9mZoZWFkZXKkY3NlcGVtb2RlbGVtb2RlbFgozgECAYUBEiCEdMdFjHF5BeBz1DotQlV6XNbluevD1NaCgEmUEg5W12Z1bmlxdWVHZm9vfGJhcmtjb250cm9sbGVyc4F4OGRpZDprZXk6ejZNa3RDRlJjd0xSRlFBOVdiZURSTTdXN2tiQmRaVEhRMnhuUGd5eFpMcTFnQ3BL";

// Data Event for a stream with a signed init event
pub const DATA_EVENT_CAR: &str = "
        uO6Jlcm9vdHOB2CpYJgABhQESICddBxl5Sk2e7I20pzX9kDLf0jj6WvIQ1KqbM3WQiClDZ3ZlcnNpb24
        BqAEBcRIgdtssXEgR7sXQQQA1doBpxUpTn4pcAaVFZfQjyo-03SGjYmlk2CpYJgABhQESII6AYH_8-Nn
        iumDaEPcbPId6ZQAMFfViEDLxGVnVBNOOZGRhdGGBo2JvcGdyZXBsYWNlZHBhdGhmL3N0ZXBoZXZhbHV
        lGQFOZHByZXbYKlgmAAGFARIgjoBgf_z42eK6YNoQ9xs8h3plAAwV9WIQMvEZWdUE0466AgGFARIgJ10
        HGXlKTZ7sjbSnNf2QMt_SOPpa8hDUqpszdZCIKUOiZ3BheWxvYWRYJAFxEiB22yxcSBHuxdBBADV2gGn
        FSlOfilwBpUVl9CPKj7TdIWpzaWduYXR1cmVzgaJpcHJvdGVjdGVkWIF7ImFsZyI6IkVkRFNBIiwia2l
        kIjoiZGlkOmtleTp6Nk1rdEJ5bkFQTHJFeWVTN3BWdGhiaXlTY21mdThuNVY3Ym9YZ3h5bzVxM1NaUlI
        jejZNa3RCeW5BUExyRXllUzdwVnRoYml5U2NtZnU4bjVWN2JvWGd4eW81cTNTWlJSIn1pc2lnbmF0dXJ
        lWECym-Kwb5ti-T5dCygt4zf8Lr6MescAbkk_DILoy3fFjYG8fZVUCGKDQiTTHbNbzOk1yze7-2hA3AK
        dBfzJY1kA";

// Assumes Mainnet network
pub const DATA_EVENT_ID: &str =
    "ce010500aa5773c7d75777e1deb6cb4af0e69eebd504d38e0185011220275d0719794a4d9eec8db4a735fd9032dfd238fa5af210d4aa9b337590882943";

// Data Event for a stream with an unsigned init event
pub const DATA_EVENT_CAR_UNSIGNED_INIT: &str = "
        uO6Jlcm9vdHOB2CpYJgABhQESIAlT-MndVmni9jiwS6JPtXtvYAa1-4tjruqLftM6BxvTZ3ZlcnNpb24B-gEBcRIguZ-ORAzcRLjL2LKcFJX2lC3Cv_4bywuG4Q8gEc5dbYajYmlk2CpYJQABcRIgCkMGCgfs8ht9NWnDxnqenauyk-FwopBeHTefuwaqvmZkZGF0YYSjYm9wY2FkZGRwYXRoZC9vbmVldmFsdWVjZm9vo2JvcGNhZGRkcGF0aGQvdHdvZXZhbHVlY2JhcqNib3BjYWRkZHBhdGhmL3RocmVlZXZhbHVlZmZvb2JhcqNib3BjYWRkZHBhdGhnL215RGF0YWV2YWx1ZQFkcHJldtgqWCUAAXESIApDBgoH7PIbfTVpw8Z6np2rspPhcKKQXh03n7sGqr5mugIBhQESIAlT-MndVmni9jiwS6JPtXtvYAa1-4tjruqLftM6BxvTomdwYXlsb2FkWCQBcRIguZ-ORAzcRLjL2LKcFJX2lC3Cv_4bywuG4Q8gEc5dbYZqc2lnbmF0dXJlc4GiaXByb3RlY3RlZFiBeyJhbGciOiJFZERTQSIsImtpZCI6ImRpZDprZXk6ejZNa3RDRlJjd0xSRlFBOVdiZURSTTdXN2tiQmRaVEhRMnhuUGd5eFpMcTFnQ3BLI3o2TWt0Q0ZSY3dMUkZRQTlXYmVEUk03VzdrYkJkWlRIUTJ4blBneXhaTHExZ0NwSyJ9aXNpZ25hdHVyZVhAZSJEw5QkFrYhbLYdLgnBn5SIbGAgm5i2jHhntWwe8nDkyKcCu4OvLMvFyGpjPloYVOr0JKwXlQfbgccHtbJpDw";

pub const TIME_EVENT_CAR:&str="uOqJlcm9vdHOB2CpYJQABcRIgcmqgb7eHSgQ32hS1NGVKZruLJGcKDI1f4lqOyNYn3eVndmVyc2lvbgG3AQFxEiByaqBvt4dKBDfaFLU0ZUpmu4skZwoMjV_iWo7I1ifd5aRiaWTYKlgmAAGFARIgjoBgf_z42eK6YNoQ9xs8h3plAAwV9WIQMvEZWdUE045kcGF0aGEwZHByZXbYKlgmAAGFARIgJ10HGXlKTZ7sjbSnNf2QMt_SOPpa8hDUqpszdZCIKUNlcHJvb2bYKlglAAFxEiAFKLx3fi7-yD1aPNyqnblI_r_5XllReVz55jBMvMxs9q4BAXESIAUovHd-Lv7IPVo83KqduUj-v_leWVF5XPnmMEy8zGz2pGRyb2902CpYJQABcRIgfWtbF-FQN6GN6ZL8OtHvp2YrGlmLbZwkOl6UY-3AUNFmdHhIYXNo2CpYJgABkwEbIBv-WU6fLnsyo5_lDSTC_T-xUlW95brOAUDByGHJzbCRZnR4VHlwZWpmKGJ5dGVzMzIpZ2NoYWluSWRvZWlwMTU1OjExMTU1MTExeQFxEiB9a1sX4VA3oY3pkvw60e-nZisaWYttnCQ6XpRj7cBQ0YPYKlgmAAGFARIgJ10HGXlKTZ7sjbSnNf2QMt_SOPpa8hDUqpszdZCIKUP22CpYJQABcRIgqVOMo-IVjo08Mk0cim3Z8flNyHY7c9g7uGMqeS0PFHA";

/// multibase-decodes a string after stripping all whitespace characters.
pub fn decode_multibase_str(encoded: &str) -> Vec<u8> {
    let (_, bytes) = multibase::decode(
        encoded
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect::<String>(),
    )
    .unwrap();
    bytes
}

mock! {
    pub AccessInterestStoreTest {}
    #[async_trait]
    impl InterestService for AccessInterestStoreTest {
        async fn insert(&self, key: Interest) -> Result<bool>;
        async fn range(
            &self,
            start: &Interest,
            end: &Interest,
            offset: usize,
            limit: usize,
        ) -> Result<Vec<Interest>>;
    }
}

mock! {
    pub EventStoreTest {}
    #[async_trait]
    impl EventService for EventStoreTest {
        async fn insert_many(&self, items: Vec<ApiItem>) -> Result<Vec<EventInsertResult>>;
        async fn range_with_values(
            &self,
            range: Range<EventId>,
            offset: usize,
            limit: usize,
        ) -> Result<Vec<(Cid, Vec<u8>)>>;
        async fn value_for_order_key(&self, key: &EventId) -> Result<Option<Vec<u8>>>;
        async fn value_for_cid(&self, key: &Cid) -> Result<Option<Vec<u8>>>;
        async fn events_since_highwater_mark(
            &self,
            highwater: i64,
            limit: i64,
            include_data: IncludeEventData,
        ) -> Result<(i64, Vec<EventDataResult>)>;
        async fn highwater_mark(
            &self,
        ) -> Result<i64>;
        async fn get_block(& self, cid: &Cid) -> Result<Option<Vec<u8>>>;
    }
}

/// Given a mock of the EventStore, prepare it to expect calls to load the init event.
pub fn mock_get_init_event(mock_store: &mut MockEventStoreTest) {
    // Expect two get_block calls

    // Call to get the init event envelope
    mock_store
        .expect_get_block()
        .once()
        .with(predicate::eq(Cid::from_str(SIGNED_INIT_EVENT_CID).unwrap()))
        .return_once(move |_| Ok(Some(decode_multibase_str(SIGNED_INIT_EVENT))));

    // Call to get the init event payload
    mock_store
        .expect_get_block()
        .once()
        .with(predicate::eq(
            Cid::from_str(SIGNED_INIT_EVENT_PAYLOAD_CID).unwrap(),
        ))
        .return_once(move |_| Ok(Some(decode_multibase_str(SIGNED_INIT_EVENT_PAYLOAD))));
}

/// Given a mock of the EventStore, prepare it to expect calls to load the unsigned init event.
pub fn mock_get_unsigned_init_event(mock_store: &mut MockEventStoreTest) {
    // Call to get the init event payload
    mock_store
        .expect_get_block()
        .once()
        .with(predicate::eq(
            Cid::from_str(UNSIGNED_INIT_EVENT_CID).unwrap(),
        ))
        .return_once(move |_| Ok(Some(decode_multibase_str(UNSIGNED_INIT_EVENT_PAYLOAD))));
}

#[test(tokio::test)]
async fn create_event() {
    let peer_id = PeerId::random();
    let network = Network::Mainnet;
    let expected_event_id = EventId::try_from(hex::decode(DATA_EVENT_ID).unwrap()).unwrap();

    // Remove whitespace from event CAR file
    let event_data = DATA_EVENT_CAR
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect::<String>();
    let mock_interest = MockAccessInterestStoreTest::new();
    let mut mock_event_store = MockEventStoreTest::new();
    mock_get_init_event(&mut mock_event_store);
    let args = vec![ApiItem::new(
        expected_event_id,
        decode_multibase_data(&event_data).unwrap(),
    )];

    mock_event_store
        .expect_insert_many()
        .with(predicate::eq(args))
        .times(1)
        .returning(|input| {
            Ok(input
                .into_iter()
                .map(|v| EventInsertResult::new_ok(v.key.clone()))
                .collect())
        });
    let server = Server::new(peer_id, network, mock_interest, Arc::new(mock_event_store));
    let resp = server
        .events_post(
            models::EventData {
                data: event_data.to_string(),
            },
            &Context,
        )
        .await
        .unwrap();
    assert!(matches!(resp, EventsPostResponse::Success));
}
#[test(tokio::test)]
async fn create_event_fails() {
    let peer_id = PeerId::random();
    let network = Network::Mainnet;
    let expected_event_id = EventId::try_from(hex::decode(DATA_EVENT_ID).unwrap()).unwrap();

    // Remove whitespace from event CAR file
    let event_data = DATA_EVENT_CAR
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect::<String>();
    let mock_interest = MockAccessInterestStoreTest::new();
    let mut mock_event_store = MockEventStoreTest::new();
    mock_get_init_event(&mut mock_event_store);
    let args = vec![ApiItem::new(
        expected_event_id.clone(),
        decode_multibase_data(&event_data).unwrap(),
    )];

    mock_event_store
        .expect_insert_many()
        .with(predicate::eq(args))
        .times(1)
        .returning(|input| {
            Ok(input
                .iter()
                .map(|i| {
                    EventInsertResult::new_failed(
                        i.key.clone(),
                        "Event is missing prev".to_string(),
                    )
                })
                .collect())
        });
    let server = Server::new(peer_id, network, mock_interest, Arc::new(mock_event_store));
    let resp = server
        .events_post(
            models::EventData {
                data: event_data.to_string(),
            },
            &Context,
        )
        .await
        .unwrap();
    assert!(matches!(resp, EventsPostResponse::BadRequest(_)));
}

#[test(tokio::test)]
async fn register_interest_sort_value() {
    let peer_id = PeerId::random();
    let network = Network::InMemory;
    let model = "k2t6wz4ylx0qr6v7dvbczbxqy7pqjb0879qx930c1e27gacg3r8sllonqt4xx9"; // cspell:disable-line

    // Construct start and end event ids
    let start = EventId::builder()
        .with_network(&network)
        .with_sep("model", &decode_multibase_data(model).unwrap())
        .with_min_controller()
        .with_min_init()
        .with_min_event()
        .build_fencepost();
    let end = EventId::builder()
        .with_network(&network)
        .with_sep("model", &decode_multibase_data(model).unwrap())
        .with_max_controller()
        .with_max_init()
        .with_max_event()
        .build_fencepost();

    // Setup mock expectations
    let mut mock_interest = MockAccessInterestStoreTest::new();
    mock_interest
        .expect_insert()
        .with(predicate::eq(
            Interest::builder()
                .with_sep_key("model")
                .with_peer_id(&peer_id)
                .with_range((start.as_slice(), end.as_slice()))
                .with_not_after(0)
                .build(),
        ))
        .times(1)
        .returning(|_| Ok(true));
    let mock_event_store = MockEventStoreTest::new();
    let server = Server::new(peer_id, network, mock_interest, Arc::new(mock_event_store));
    let interest = models::Interest {
        sep: "model".to_string(),
        sep_value: model.to_owned(),
        controller: None,
        stream_id: None,
    };
    let resp = server.interests_post(interest, &Context).await.unwrap();
    assert_eq!(resp, InterestsPostResponse::Success);
}

#[test(tokio::test)]

async fn register_interest_sort_value_bad_request() {
    let peer_id = PeerId::random();
    let network = Network::InMemory;

    let model = "2t6wz4ylx0qr6v7dvbczbxqy7pqjb0879qx930c1e27gacg3r8sllonqt4xx9"; //missing 'k' cspell:disable-line

    // Setup mock expectations
    let mock_interest = MockAccessInterestStoreTest::new();
    let mock_event_store = MockEventStoreTest::new();
    let server = Server::new(peer_id, network, mock_interest, Arc::new(mock_event_store));
    let interest = models::Interest {
        sep: "model".to_string(),
        sep_value: model.to_owned(),
        controller: None,
        stream_id: None,
    };
    let resp = server.interests_post(interest, &Context).await.unwrap();
    assert!(matches!(resp, InterestsPostResponse::BadRequest(_)));
}

#[test(tokio::test)]

async fn register_interest_sort_value_controller() {
    let peer_id = PeerId::random();
    let network = Network::InMemory;
    let model = "z3KWHw5Efh2qLou2FEdz3wB8ZvLgURJP94HeijLVurxtF1Ntv6fkg2G"; // base58 encoded should work cspell:disable-line
                                                                           // we convert to base36 before storing
    let model_base36 = "k2t6wz4ylx0qr6v7dvbczbxqy7pqjb0879qx930c1e27gacg3r8sllonqt4xx9"; // cspell:disable-line
    let controller = "did:key:zGs1Det7LHNeu7DXT4nvoYrPfj3n6g7d6bj2K4AMXEvg1";
    let start = EventId::builder()
        .with_network(&network)
        .with_sep("model", &decode_multibase_data(model_base36).unwrap())
        .with_controller(controller)
        .with_min_init()
        .with_min_event()
        .build_fencepost();
    let end = EventId::builder()
        .with_network(&network)
        .with_sep("model", &decode_multibase_data(model_base36).unwrap())
        .with_controller(controller)
        .with_max_init()
        .with_max_event()
        .build_fencepost();
    let mut mock_interest = MockAccessInterestStoreTest::new();
    mock_interest
        .expect_insert()
        .with(predicate::eq(
            Interest::builder()
                .with_sep_key("model")
                .with_peer_id(&peer_id)
                .with_range((start.as_slice(), end.as_slice()))
                .with_not_after(0)
                .build(),
        ))
        .times(1)
        .returning(|__| Ok(true));
    let mock_event_store = MockEventStoreTest::new();
    let server = Server::new(peer_id, network, mock_interest, Arc::new(mock_event_store));
    let resp = server
        .interests_sort_key_sort_value_post(
            "model".to_string(),
            model.to_owned(),
            Some(controller.to_owned()),
            None,
            &Context,
        )
        .await
        .unwrap();
    assert_eq!(resp, InterestsSortKeySortValuePostResponse::Success);
}

#[test(tokio::test)]

async fn register_interest_value_controller_stream() {
    let peer_id = PeerId::random();
    let network = Network::InMemory;
    let model = "k2t6wz4ylx0qr6v7dvbczbxqy7pqjb0879qx930c1e27gacg3r8sllonqt4xx9"; // cspell:disable-line
    let controller = "did:key:zGs1Det7LHNeu7DXT4nvoYrPfj3n6g7d6bj2K4AMXEvg1";
    let stream =
        StreamId::from_str("k2t6wz4ylx0qs435j9oi1s6469uekyk6qkxfcb21ikm5ag2g1cook14ole90aw") // cspell:disable-line
            .unwrap();
    let start = EventId::builder()
        .with_network(&network)
        .with_sep("model", &decode_multibase_data(model).unwrap())
        .with_controller(controller)
        .with_init(&stream.cid)
        .with_min_event()
        .build_fencepost();
    let end = EventId::builder()
        .with_network(&network)
        .with_sep("model", &decode_multibase_data(model).unwrap())
        .with_controller(controller)
        .with_init(&stream.cid)
        .with_max_event()
        .build_fencepost();
    let mut mock_interest = MockAccessInterestStoreTest::new();
    mock_interest
        .expect_insert()
        .with(predicate::eq(
            Interest::builder()
                .with_sep_key("model")
                .with_peer_id(&peer_id)
                .with_range((start.as_slice(), end.as_slice()))
                .with_not_after(0)
                .build(),
        ))
        .times(1)
        .returning(|__| Ok(true));
    let mock_event_store = MockEventStoreTest::new();
    let server = Server::new(peer_id, network, mock_interest, Arc::new(mock_event_store));
    let resp = server
        .interests_sort_key_sort_value_post(
            "model".to_string(),
            model.to_owned(),
            Some(controller.to_owned()),
            Some(stream.to_string()),
            &Context,
        )
        .await
        .unwrap();
    assert_eq!(resp, InterestsSortKeySortValuePostResponse::Success);
}

#[test(tokio::test)]
async fn get_interests() {
    let peer_id = PeerId::from_str("1AaNXU5G2SJQSzCCP23V2TEDierSRBBGLA7aSCYScUTke9").unwrap();
    let network = Network::InMemory;
    let model = "k2t6wz4ylx0qr6v7dvbczbxqy7pqjb0879qx930c1e27gacg3r8sllonqt4xx9"; // cspell:disable-line
    let controller = "did:key:zGs1Det7LHNeu7DXT4nvoYrPfj3n6g7d6bj2K4AMXEvg1";
    let stream =
        StreamId::from_str("k2t6wz4ylx0qs435j9oi1s6469uekyk6qkxfcb21ikm5ag2g1cook14ole90aw") // cspell:disable-line
            .unwrap();
    let start = EventId::builder()
        .with_network(&network)
        .with_sep("model", &decode_multibase_data(model).unwrap())
        .with_controller(controller)
        .with_init(&stream.cid)
        .with_min_event()
        .build_fencepost();
    let end = EventId::builder()
        .with_network(&network)
        .with_sep("model", &decode_multibase_data(model).unwrap())
        .with_controller(controller)
        .with_init(&stream.cid)
        .with_max_event()
        .build_fencepost();
    let mut mock_interest = MockAccessInterestStoreTest::new();
    mock_interest
        .expect_range()
        .with(
            predicate::eq(Interest::min_value()),
            predicate::eq(Interest::max_value()),
            predicate::eq(0),
            predicate::eq(usize::MAX),
        )
        .once()
        .return_once(move |_, _, _, _| {
            Ok(vec![
                Interest::builder()
                    .with_sep_key("model")
                    .with_peer_id(&peer_id)
                    .with_range((start.as_slice(), end.as_slice()))
                    .with_not_after(0)
                    .build(),
                Interest::builder()
                    .with_sep_key("model")
                    .with_peer_id(&peer_id)
                    .with_range((start.as_slice(), end.as_slice()))
                    .with_not_after(1)
                    .build(),
            ])
        });

    let mock_event_store = MockEventStoreTest::new();
    let server = Server::new(peer_id, network, mock_interest, Arc::new(mock_event_store));
    let resp = server
        .experimental_interests_get(None, &Context)
        .await
        .unwrap();
    expect![[r#"
        Success(
            InterestsGet {
                interests: [
                    InterestsGetInterestsInner {
                        data: "zwZSodouDdQxpoGdqDjfm1n8r3v18Eoo4si4AFo1UbGArVey2XHwDBwDshSiPN36DDWaE7MprPpBmNrZkDhrFugryq9nnAVyP6M9oTns8fjB4RqR7oCNEX8HDBZAbVVrpXY2QcWHu5Dy",
                    },
                    InterestsGetInterestsInner {
                        data: "zwZSodouDdQxpoGdqDjfm1n8r3v18Eoo4si4AFo1UbGArVey2XHwDBwDshSiPN36DDWaE7MprPpBmNrZkDhrFugryq9nnAVyP6M9oTns8fjB4RqR7oCNEX8HDBZAbVVrpXY2QcWHu5Dz",
                    },
                ],
            },
        )
    "#]].assert_debug_eq(&resp);
}
#[test(tokio::test)]
async fn get_interests_for_peer() {
    let peer_id_a = PeerId::from_str("1AaNXU5G2SJQSzCCP23V2TEDierSRBBGLA7aSCYScUTke9").unwrap();
    let peer_id_b = PeerId::from_str("1AcJjoLqKWAPBRQYsff8ZQPjwinCAFUdTEgZrAZgeZkCu7").unwrap();
    let network = Network::InMemory;
    let model = "k2t6wz4ylx0qr6v7dvbczbxqy7pqjb0879qx930c1e27gacg3r8sllonqt4xx9"; // cspell:disable-line
    let controller = "did:key:zGs1Det7LHNeu7DXT4nvoYrPfj3n6g7d6bj2K4AMXEvg1";
    let stream =
        StreamId::from_str("k2t6wz4ylx0qs435j9oi1s6469uekyk6qkxfcb21ikm5ag2g1cook14ole90aw") // cspell:disable-line
            .unwrap();
    let start = EventId::builder()
        .with_network(&network)
        .with_sep("model", &decode_multibase_data(model).unwrap())
        .with_controller(controller)
        .with_init(&stream.cid)
        .with_min_event()
        .build_fencepost();
    let end = EventId::builder()
        .with_network(&network)
        .with_sep("model", &decode_multibase_data(model).unwrap())
        .with_controller(controller)
        .with_init(&stream.cid)
        .with_max_event()
        .build_fencepost();
    let mut mock_interest = MockAccessInterestStoreTest::new();
    mock_interest
        .expect_range()
        .with(
            predicate::eq(Interest::min_value()),
            predicate::eq(Interest::max_value()),
            predicate::eq(0),
            predicate::eq(usize::MAX),
        )
        .once()
        .return_once(move |_, _, _, _| {
            Ok(vec![
                Interest::builder()
                    .with_sep_key("model")
                    .with_peer_id(&peer_id_a)
                    .with_range((start.as_slice(), end.as_slice()))
                    .with_not_after(0)
                    .build(),
                Interest::builder()
                    .with_sep_key("model")
                    .with_peer_id(&peer_id_a)
                    .with_range((start.as_slice(), end.as_slice()))
                    .with_not_after(1)
                    .build(),
                Interest::builder()
                    .with_sep_key("model")
                    .with_peer_id(&peer_id_b)
                    .with_range((start.as_slice(), end.as_slice()))
                    .with_not_after(0)
                    .build(),
                Interest::builder()
                    .with_sep_key("model")
                    .with_peer_id(&peer_id_b)
                    .with_range((start.as_slice(), end.as_slice()))
                    .with_not_after(1)
                    .build(),
            ])
        });

    let mock_event_store = MockEventStoreTest::new();
    let server = Server::new(
        peer_id_b,
        network,
        mock_interest,
        Arc::new(mock_event_store),
    );
    let resp = server
        .experimental_interests_get(Some(peer_id_a.to_string()), &Context)
        .await
        .unwrap();
    expect![[r#"
        Success(
            InterestsGet {
                interests: [
                    InterestsGetInterestsInner {
                        data: "zwZSodouDdQxpoGdqDjfm1n8r3v18Eoo4si4AFo1UbGArVey2XHwDBwDshSiPN36DDWaE7MprPpBmNrZkDhrFugryq9nnAVyP6M9oTns8fjB4RqR7oCNEX8HDBZAbVVrpXY2QcWHu5Dy",
                    },
                    InterestsGetInterestsInner {
                        data: "zwZSodouDdQxpoGdqDjfm1n8r3v18Eoo4si4AFo1UbGArVey2XHwDBwDshSiPN36DDWaE7MprPpBmNrZkDhrFugryq9nnAVyP6M9oTns8fjB4RqR7oCNEX8HDBZAbVVrpXY2QcWHu5Dz",
                    },
                ],
            },
        )
    "#]].assert_debug_eq(&resp);
}

#[test(tokio::test)]
async fn get_events_for_interest_range() {
    let peer_id = PeerId::random();
    let network = Network::InMemory;
    let model = "k2t6wz4ylx0qr6v7dvbczbxqy7pqjb0879qx930c1e27gacg3r8sllonqt4xx9"; // cspell:disable-line
    let controller = "did:key:zGs1Det7LHNeu7DXT4nvoYrPfj3n6g7d6bj2K4AMXEvg1";
    let stream =
        StreamId::from_str("k2t6wz4ylx0qs435j9oi1s6469uekyk6qkxfcb21ikm5ag2g1cook14ole90aw") // cspell:disable-line
            .unwrap();
    let cid = stream.cid;
    let start = EventId::builder()
        .with_network(&network)
        .with_sep("model", &decode_multibase_data(model).unwrap())
        .with_controller(controller)
        .with_init(&cid)
        .with_min_event()
        .build_fencepost();
    let end = EventId::builder()
        .with_network(&network)
        .with_sep("model", &decode_multibase_data(model).unwrap())
        .with_controller(controller)
        .with_init(&stream.cid)
        .with_max_event()
        .build_fencepost();
    /*
    l: Success(EventsGet { events: [Event { id: "fce0105ff012616e0f0c1e987ef0f772afbe2c7f05c50102bc800", data: "" }, Event { id: "fce0105ff012616e0f0c1e987ef0f772afbe2c7f05c50102bc8ff", data: "" }], resume_offset: 2, is_complete: false })
    r: Success(EventsGet { events: [Event { id: "fce0105ff012616e0f0c1e987ef0f772afbe2c7f05c50102bc800", data: "" }], resume_offset: 1, is_complete: false })
            */
    let mock_interest = MockAccessInterestStoreTest::new();
    let expected = BuildResponse::event(cid, None);
    let mut mock_event_store = MockEventStoreTest::new();
    mock_event_store
        .expect_range_with_values()
        .with(
            predicate::eq(start..end),
            predicate::eq(0),
            predicate::eq(1),
        )
        .times(1)
        .returning(move |_, _, _| Ok(vec![(cid, vec![])]));
    let server = Server::new(peer_id, network, mock_interest, Arc::new(mock_event_store));
    let resp = server
        .experimental_events_sep_sep_value_get(
            "model".to_string(),
            model.to_owned(),
            Some(controller.to_owned()),
            Some(stream.to_string()),
            None,
            Some(1),
            &Context,
        )
        .await
        .unwrap();
    assert_eq!(
        resp,
        ExperimentalEventsSepSepValueGetResponse::Success(models::EventsGet {
            resume_offset: 1,
            events: vec![expected],
            is_complete: false,
        })
    );
}

#[test(tokio::test)]
async fn test_events_event_id_get_by_event_id_success() {
    let peer_id = PeerId::random();
    let network = Network::InMemory;
    let event_cid =
        Cid::from_str("baejbeicqtpe5si4qvbffs2s7vtbk5ccbsfg6owmpidfj3zeluqz4hlnz6m").unwrap(); // cspell:disable-line

    let event_id = EventId::new(
        &network,
        "model",
        &decode_multibase_data("k2t6wz4ylx0qr6v7dvbczbxqy7pqjb0879qx930c1e27gacg3r8sllonqt4xx9")
            .unwrap(), // cspell:disable-line
        "did:key:zGs1Det7LHNeu7DXT4nvoYrPfj3n6g7d6bj2K4AMXEvg1", // cspell:disable-line
        &Cid::from_str("baejbeihyr3kf77etqdccjfoc33dmko2ijyugn6qk6yucfkioasjssz3bbu").unwrap(), // cspell:disable-line
        &event_cid,
    );
    let event_id_str = multibase::encode(Base::Base16Lower, event_id.to_bytes());
    let event_data = b"event data".to_vec();
    let event_data_base64 = multibase::encode(multibase::Base::Base64, &event_data);
    let mut mock_event_store = MockEventStoreTest::new();
    mock_event_store
        .expect_value_for_order_key()
        .with(predicate::eq(event_id))
        .times(1)
        .returning(move |_| Ok(Some(event_data.clone())));
    let mock_interest = MockAccessInterestStoreTest::new();
    let server = Server::new(peer_id, network, mock_interest, Arc::new(mock_event_store));
    let result = server.events_event_id_get(event_id_str, &Context).await;
    let EventsEventIdGetResponse::Success(event) = result.unwrap() else {
        panic!("Expected EventsEventIdGetResponse::Success but got another variant");
    };
    assert_eq!(
        event.id,
        multibase::encode(multibase::Base::Base32Lower, event_cid.to_bytes())
    );
    assert_eq!(event.data.unwrap(), event_data_base64);
}

#[test(tokio::test)]

async fn test_events_event_id_get_by_cid_success() {
    let peer_id = PeerId::random();
    let network = Network::InMemory;
    let event_cid =
        Cid::from_str("baejbeihyr3kf77etqdccjfoc33dmko2ijyugn6qk6yucfkioasjssz3bbu").unwrap(); // cspell:disable-line
    let event_data = b"event data".to_vec();
    let event_data_base64 = multibase::encode(multibase::Base::Base64, &event_data);
    let mut mock_event_store = MockEventStoreTest::new();
    mock_event_store
        .expect_value_for_cid()
        .with(predicate::eq(event_cid))
        .times(1)
        .returning(move |_| Ok(Some(event_data.clone())));
    let mock_interest = MockAccessInterestStoreTest::new();
    let server = Server::new(peer_id, network, mock_interest, Arc::new(mock_event_store));
    let result = server
        .events_event_id_get(event_cid.to_string(), &Context)
        .await;
    let EventsEventIdGetResponse::Success(event) = result.unwrap() else {
        panic!("Expected EventsEventIdGetResponse::Success but got another variant");
    };
    assert_eq!(
        event.id,
        multibase::encode(multibase::Base::Base32Lower, event_cid.to_bytes())
    );
    assert_eq!(event.data.unwrap(), event_data_base64);
}
