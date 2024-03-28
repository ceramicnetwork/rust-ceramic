// Tests for server.rs api implementations

use crate::server::decode_multibase_data;
use crate::server::BuildResponse;
use crate::server::Server;
use crate::{AccessInterestStore, AccessModelStore};
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
use mockall::{mock, predicate};
use multibase::Base;
use recon::Key;
use std::str::FromStr;
use std::sync::Arc;
use tracing_test::traced_test;

struct Context;
mock! {
    pub ReconInterestTest {
        fn insert(&self, key: Interest, value: Option<Vec<u8>>) -> Result<bool>;
        fn range_with_values(
            &self,
            start: &Interest,
            end: &Interest,
            offset: usize,
            limit: usize,
        ) -> Result<Vec<(Interest, Vec<u8>)>>;
    }
    impl Clone for ReconInterestTest {
        fn clone(&self) -> Self;
    }
}
#[async_trait]
impl AccessInterestStore for MockReconInterestTest {
    async fn insert(&self, key: Interest) -> Result<bool> {
        self.insert(key, None)
    }
    async fn range(
        &self,
        start: &Interest,
        end: &Interest,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Interest>> {
        let res = self.range_with_values(start, end, offset, limit)?;
        Ok(res.into_iter().map(|(k, _)| k).collect())
    }
}
mock! {
    pub ReconModelTest {
        fn insert_many(&self, items: &[(EventId, Option<Vec<u8>>)]) -> Result<(Vec<bool>, usize)>;
        fn range_with_values(
            &self,
            start: &EventId,
            end: &EventId,
            offset: usize,
            limit: usize,
        ) -> Result<Vec<(EventId,Vec<u8>)>>;
        fn value_for_key(&self, key: &EventId) -> Result<Option<Vec<u8>>>;
    }
    impl Clone for ReconModelTest {
        fn clone(&self) -> Self;
    }
}
#[async_trait]
impl AccessModelStore for MockReconModelTest {
    async fn insert_many(
        &self,
        items: &[(EventId, Option<Vec<u8>>)],
    ) -> Result<(Vec<bool>, usize)> {
        self.insert_many(items)
    }
    async fn range_with_values(
        &self,
        start: &EventId,
        end: &EventId,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<(EventId, Vec<u8>)>> {
        self.range_with_values(start, end, offset, limit)
    }
    async fn value_for_key(&self, key: &EventId) -> Result<Option<Vec<u8>>> {
        self.value_for_key(key)
    }
    async fn keys_since_highwater_mark(
        &self,
        _highwater: i64,
        _limit: i64,
    ) -> anyhow::Result<(i64, Vec<EventId>)> {
        Ok((0, vec![]))
    }
}
#[tokio::test]
async fn create_event() {
    let peer_id = PeerId::random();
    let network = Network::InMemory;
    let event_id = EventId::new(
        &network,
        "model",
        &decode_multibase_data("k2t6wz4ylx0qr6v7dvbczbxqy7pqjb0879qx930c1e27gacg3r8sllonqt4xx9")
            .unwrap(), // cspell:disable-line
        "did:key:zGs1Det7LHNeu7DXT4nvoYrPfj3n6g7d6bj2K4AMXEvg1", // cspell:disable-line
        &Cid::from_str("baejbeihyr3kf77etqdccjfoc33dmko2ijyugn6qk6yucfkioasjssz3bbu").unwrap(), // cspell:disable-line
        &Cid::from_str("baejbeicqtpe5si4qvbffs2s7vtbk5ccbsfg6owmpidfj3zeluqz4hlnz6m").unwrap(), // cspell:disable-line
    );
    let event_id_str = multibase::encode(Base::Base16Lower, event_id.to_bytes());
    let event_data = "f".to_string();
    let mock_interest = MockReconInterestTest::new();
    let mut mock_model = MockReconModelTest::new();
    let args = vec![(
        event_id.clone(),
        Some(decode_multibase_data(event_data.as_str()).unwrap()),
    )];
    mock_model
        .expect_insert_many()
        .with(predicate::eq(args))
        .times(1)
        .returning(|_| Ok((vec![true], 1)));
    let server = Server::new(peer_id, network, mock_interest, Arc::new(mock_model));
    let resp = server
        .events_post(
            models::Event {
                id: event_id_str,
                data: event_data,
            },
            &Context,
        )
        .await
        .unwrap();
    assert!(matches!(resp, EventsPostResponse::Success));
}
#[tokio::test]
#[traced_test]
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
    let mut mock_interest = MockReconInterestTest::new();
    mock_interest
        .expect_insert()
        .with(
            predicate::eq(
                Interest::builder()
                    .with_sep_key("model")
                    .with_peer_id(&peer_id)
                    .with_range((start.as_slice(), end.as_slice()))
                    .with_not_after(0)
                    .build(),
            ),
            predicate::eq(None),
        )
        .times(1)
        .returning(|_, _| Ok(true));
    let mock_model = MockReconModelTest::new();
    let server = Server::new(peer_id, network, mock_interest, Arc::new(mock_model));
    let interest = models::Interest {
        sep: "model".to_string(),
        sep_value: model.to_owned(),
        controller: None,
        stream_id: None,
    };
    let resp = server.interests_post(interest, &Context).await.unwrap();
    assert_eq!(resp, InterestsPostResponse::Success);
}

#[tokio::test]
#[traced_test]
async fn register_interest_sort_value_bad_request() {
    let peer_id = PeerId::random();
    let network = Network::InMemory;

    let model = "2t6wz4ylx0qr6v7dvbczbxqy7pqjb0879qx930c1e27gacg3r8sllonqt4xx9"; //missing 'k' cspell:disable-line

    // Setup mock expectations
    let mock_interest = MockReconInterestTest::new();
    let mock_model = MockReconModelTest::new();
    let server = Server::new(peer_id, network, mock_interest, Arc::new(mock_model));
    let interest = models::Interest {
        sep: "model".to_string(),
        sep_value: model.to_owned(),
        controller: None,
        stream_id: None,
    };
    let resp = server.interests_post(interest, &Context).await.unwrap();
    assert!(matches!(resp, InterestsPostResponse::BadRequest(_)));
}

#[tokio::test]
#[traced_test]
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
    let mut mock_interest = MockReconInterestTest::new();
    mock_interest
        .expect_insert()
        .with(
            predicate::eq(
                Interest::builder()
                    .with_sep_key("model")
                    .with_peer_id(&peer_id)
                    .with_range((start.as_slice(), end.as_slice()))
                    .with_not_after(0)
                    .build(),
            ),
            predicate::eq(None),
        )
        .times(1)
        .returning(|_, _| Ok(true));
    let mock_model = MockReconModelTest::new();
    let server = Server::new(peer_id, network, mock_interest, Arc::new(mock_model));
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

#[tokio::test]
#[traced_test]
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
    let mut mock_interest = MockReconInterestTest::new();
    mock_interest
        .expect_insert()
        .with(
            predicate::eq(
                Interest::builder()
                    .with_sep_key("model")
                    .with_peer_id(&peer_id)
                    .with_range((start.as_slice(), end.as_slice()))
                    .with_not_after(0)
                    .build(),
            ),
            predicate::eq(None),
        )
        .times(1)
        .returning(|_, _| Ok(true));
    let mock_model = MockReconModelTest::new();
    let server = Server::new(peer_id, network, mock_interest, Arc::new(mock_model));
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
#[tokio::test]
#[traced_test]
async fn get_events_for_interest_range() {
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
    /*
    l: Success(EventsGet { events: [Event { id: "fce0105ff012616e0f0c1e987ef0f772afbe2c7f05c50102bc800", data: "" }, Event { id: "fce0105ff012616e0f0c1e987ef0f772afbe2c7f05c50102bc8ff", data: "" }], resume_offset: 2, is_complete: false })
    r: Success(EventsGet { events: [Event { id: "fce0105ff012616e0f0c1e987ef0f772afbe2c7f05c50102bc800", data: "" }], resume_offset: 1, is_complete: false })
            */
    let mock_interest = MockReconInterestTest::new();
    let expected = BuildResponse::event(start.clone(), vec![]);
    let mut mock_model = MockReconModelTest::new();
    mock_model
        .expect_range_with_values()
        .with(
            predicate::eq(start),
            predicate::eq(end),
            predicate::eq(0),
            predicate::eq(1),
        )
        .times(1)
        .returning(|s, _, _, _| Ok(vec![(s.clone(), vec![])]));
    let server = Server::new(peer_id, network, mock_interest, Arc::new(mock_model));
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
#[tokio::test]
#[traced_test]
async fn test_events_event_id_get_success() {
    let peer_id = PeerId::random();
    let network = Network::InMemory;
    let event_id = EventId::new(
        &network,
        "model",
        &decode_multibase_data("k2t6wz4ylx0qr6v7dvbczbxqy7pqjb0879qx930c1e27gacg3r8sllonqt4xx9")
            .unwrap(), // cspell:disable-line
        "did:key:zGs1Det7LHNeu7DXT4nvoYrPfj3n6g7d6bj2K4AMXEvg1", // cspell:disable-line
        &Cid::from_str("baejbeihyr3kf77etqdccjfoc33dmko2ijyugn6qk6yucfkioasjssz3bbu").unwrap(), // cspell:disable-line
        &Cid::from_str("baejbeicqtpe5si4qvbffs2s7vtbk5ccbsfg6owmpidfj3zeluqz4hlnz6m").unwrap(), // cspell:disable-line
    );
    let event_id_str = multibase::encode(Base::Base16Lower, event_id.to_bytes());
    let event_data = b"event data".to_vec();
    let event_data_base64 = multibase::encode(multibase::Base::Base64, &event_data);
    let mut mock_model = MockReconModelTest::new();
    mock_model
        .expect_value_for_key()
        .with(predicate::eq(event_id.clone()))
        .times(1)
        .returning(move |_| Ok(Some(event_data.clone())));
    let mock_interest = MockReconInterestTest::new();
    let server = Server::new(peer_id, network, mock_interest, Arc::new(mock_model));
    let result = server.events_event_id_get(event_id_str, &Context).await;
    let EventsEventIdGetResponse::Success(event) = result.unwrap() else {
        panic!("Expected EventsEventIdGetResponse::Success but got another variant");
    };
    assert_eq!(
        event.id,
        multibase::encode(multibase::Base::Base16Lower, event_id.as_bytes())
    );
    assert_eq!(event.data, event_data_base64);
}
