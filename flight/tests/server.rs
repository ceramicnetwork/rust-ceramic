use std::{net::SocketAddr, str::FromStr as _, sync::Arc, time::Duration};

use anyhow::{bail, Context as _, Result};
use arrow::{compute::concat_batches, util::pretty::pretty_format_batches};
use arrow_array::RecordBatch;
use arrow_flight::{sql::client::FlightSqlServiceClient, FlightInfo};
use arrow_schema::Schema;
use ceramic_arrow_test::pretty_conclusion_events_from_batch;
use ceramic_flight::server::new_server;
use ceramic_pipeline::{
    ConclusionData, ConclusionEvent, ConclusionFeed, ConclusionInit, ConclusionTime,
};
use cid::Cid;
use expect_test::expect;
use futures::TryStreamExt as _;
use http::Uri;
use mockall::{mock, predicate};
use test_log::test;
use tokio::net::TcpListener;
use tonic::{async_trait, transport::Channel};

async fn channel(addr: &SocketAddr) -> Channel {
    let url = format!("http://{addr}");
    let uri: Uri = url.parse().expect("Valid URI");
    Channel::builder(uri)
        .timeout(Duration::from_secs(30))
        .connect()
        .await
        .expect("error connecting to server")
}

async fn start_server(feed: MockFeed) -> FlightSqlServiceClient<Channel> {
    let ctx = ceramic_pipeline::session_from_config(ceramic_pipeline::Config {
        conclusion_feed: feed.into(),
        object_store: Arc::new(object_store::memory::InMemory::new()),
    })
    .await
    .unwrap();
    let server = new_server(ctx).unwrap();
    // let OS choose a free port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_future =
        server.serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener));
    tokio::spawn(server_future);
    FlightSqlServiceClient::new(channel(&addr).await)
}

async fn execute_flight(
    client: &mut FlightSqlServiceClient<Channel>,
    info: FlightInfo,
) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::try_from(info.clone()).context("valid schema")?);
    let mut batches = Vec::with_capacity(info.endpoint.len() + 1);
    // Always push an empty batch so the test output always contains a header.
    batches.push(RecordBatch::new_empty(schema.clone()));

    for endpoint in info.endpoint {
        let Some(ticket) = &endpoint.ticket else {
            bail!("did not get ticket");
        };

        let mut flight_data = client.do_get(ticket.clone()).await.context("do get")?;

        let mut endpoint_batches: Vec<_> = (&mut flight_data)
            .try_collect()
            .await
            .context("collect data stream")?;
        batches.append(&mut endpoint_batches);
    }

    concat_batches(&schema, &batches).context("concat_batches for output")
}

mock! {
    #[derive(Debug)]
    pub Feed {}
    #[async_trait]
    impl ConclusionFeed for Feed {
        async fn conclusion_events_since(
            &self,
            highwater_mark: i64,
            limit: i64,
        ) -> anyhow::Result<Vec<ConclusionEvent>>;
    }
}

fn events(start_index: u64) -> Vec<ConclusionEvent> {
    vec![
        ConclusionEvent::Data(ConclusionData {
            index: start_index,
            event_cid: Cid::from_str("baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi")
                .unwrap(),
            init: ConclusionInit {
                stream_cid: Cid::from_str(
                    "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                )
                .unwrap(),
                stream_type: 3,
                controller: "did:key:bob".to_string(),
                dimensions: vec![
                    ("controller".to_string(), b"did:key:bob".to_vec()),
                    ("model".to_string(), b"model".to_vec()),
                ],
            },
            previous: vec![],
            data: r#"{"a":0}"#.bytes().collect(),
        }),
        ConclusionEvent::Time(ConclusionTime {
            index: start_index + 1,
            event_cid: Cid::from_str("baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq")
                .unwrap(),
            init: ConclusionInit {
                stream_cid: Cid::from_str(
                    "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                )
                .unwrap(),
                stream_type: 3,
                controller: "did:key:bob".to_string(),
                dimensions: vec![
                    ("controller".to_string(), b"did:key:bob".to_vec()),
                    ("model".to_string(), b"model".to_vec()),
                ],
            },
            previous: vec![Cid::from_str(
                "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
            )
            .unwrap()],
        }),
        ConclusionEvent::Data(ConclusionData {
            index: start_index + 2,
            event_cid: Cid::from_str("baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du")
                .unwrap(),
            init: ConclusionInit {
                stream_cid: Cid::from_str(
                    "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                )
                .unwrap(),
                stream_type: 3,
                controller: "did:key:bob".to_string(),
                dimensions: vec![
                    ("controller".to_string(), b"did:key:bob".to_vec()),
                    ("model".to_string(), b"model".to_vec()),
                ],
            },
            previous: vec![
                Cid::from_str("baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq")
                    .unwrap(),
                Cid::from_str("baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi")
                    .unwrap(),
            ],
            data: r#"[{"op":"replace", "path": "/a", "value":1}]"#.bytes().collect(),
        }),
    ]
}

#[test(tokio::test)]
async fn test_simple() -> Result<()> {
    let mut feed = MockFeed::new();
    feed.expect_conclusion_events_since()
        .once()
        .return_once(|_, _| Ok(events(0)));
    let mut client = start_server(feed).await;

    let info = client
        .execute("SELECT * FROM conclusion_events".to_string(), None)
        .await?;
    let batch = execute_flight(&mut client, info).await?;
    let batches = pretty_conclusion_events_from_batch(batch).await;
    let formatted = pretty_format_batches(&batches).unwrap().to_string();
    expect![[r#"
        +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+---------------------------------------------+----------------------------------------------------------------------------------------------------------------------------+
        | index | stream_cid                                                  | stream_type | controller  | dimensions                                              | event_cid                                                   | event_type | data                                        | previous                                                                                                                   |
        +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+---------------------------------------------+----------------------------------------------------------------------------------------------------------------------------+
        | 0     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 3           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"a":0}                                     |                                                                                                                            |
        | 1     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 3           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          |                                             | [baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi]                                                              |
        | 2     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 3           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du | 0          | [{"op":"replace", "path": "/a", "value":1}] | [baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq, baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi] |
        +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+---------------------------------------------+----------------------------------------------------------------------------------------------------------------------------+"#]].assert_eq(&formatted);
    Ok(())
}

#[test(tokio::test)]
async fn test_push_down_predicate() -> Result<()> {
    let mut feed = MockFeed::new();
    feed.expect_conclusion_events_since()
        .once()
        .with(predicate::eq(42), predicate::eq(2))
        .return_once(|h, l| Ok(events(h as u64).into_iter().take(l as usize).collect()));
    let mut client = start_server(feed).await;

    let info = client
        .execute(
            "SELECT * FROM conclusion_events WHERE index > 42 LIMIT 2".to_string(),
            None,
        )
        .await?;
    let batch = execute_flight(&mut client, info).await?;
    let batches = pretty_conclusion_events_from_batch(batch).await;
    let formatted = pretty_format_batches(&batches).unwrap().to_string();
    expect![[r#"
        +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+---------+---------------------------------------------------------------+
        | index | stream_cid                                                  | stream_type | controller  | dimensions                                              | event_cid                                                   | event_type | data    | previous                                                      |
        +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+---------+---------------------------------------------------------------+
        | 42    | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 3           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"a":0} |                                                               |
        | 43    | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 3           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          |         | [baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi] |
        +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+---------+---------------------------------------------------------------+"#]].assert_eq(&formatted);
    Ok(())
}
