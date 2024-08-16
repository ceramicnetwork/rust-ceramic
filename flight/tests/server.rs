use std::{net::SocketAddr, str::FromStr as _, sync::Arc, time::Duration};

use anyhow::{bail, Context as _, Result};
use arrow::{compute::concat_batches, util::pretty::pretty_format_batches};
use arrow_array::RecordBatch;
use arrow_flight::{sql::client::FlightSqlServiceClient, FlightInfo};
use arrow_schema::Schema;
use ceramic_arrow_test::pretty_feed_from_batch;
use ceramic_flight::{
    server::{new_server, ConclusionFeed},
    ConclusionData, ConclusionEvent, ConclusionInit, ConclusionTime,
};
use cid::Cid;
use expect_test::expect;
use futures::TryStreamExt as _;
use http::Uri;
use mockall::{mock, predicate};
use tokio::net::TcpListener;
use tonic::{async_trait, transport::Channel};

/// Return a [`Channel`] connected to the TestServer
#[allow(dead_code)]
pub async fn channel(addr: &SocketAddr) -> Channel {
    let url = format!("http://{addr}");
    let uri: Uri = url.parse().expect("Valid URI");
    Channel::builder(uri)
        .timeout(Duration::from_secs(30))
        .connect()
        .await
        .expect("error connecting to server")
}

async fn start_server(feed: MockFeed) -> FlightSqlServiceClient<Channel> {
    let server = new_server(Arc::new(feed)).unwrap();
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

    Ok(concat_batches(&schema, &batches)?)
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
            index: start_index + 0,
            event_cid: Cid::from_str("baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi")
                .unwrap(),
            init: ConclusionInit {
                stream_cid: Cid::from_str(
                    "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                )
                .unwrap(),
                controller: "did:key:bob".to_string(),
                dimensions: vec![],
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
                controller: "did:key:bob".to_string(),
                dimensions: vec![],
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
                controller: "did:key:bob".to_string(),
                dimensions: vec![],
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

#[tokio::test]
async fn test_simple() -> Result<()> {
    let mut feed = MockFeed::new();
    feed.expect_conclusion_events_since()
        .once()
        .return_once(|_, _| Ok(events(0)));
    let mut client = start_server(feed).await;

    let info = client
        .execute("SELECT * FROM conclusion_feed".to_string(), None)
        .await?;
    let batch = execute_flight(&mut client, info).await?;
    let batches = pretty_feed_from_batch(batch).await;
    let formatted = pretty_format_batches(&batches).unwrap().to_string();
    expect![[r#"
        +-------+------------+-------------------------------------------------------------+-------------+-------------------------------------------------------------+---------------------------------------------+----------------------------------------------------------------------------------------------------------------------------+
        | index | event_type | stream_cid                                                  | controller  | event_cid                                                   | data                                        | previous                                                                                                                   |
        +-------+------------+-------------------------------------------------------------+-------------+-------------------------------------------------------------+---------------------------------------------+----------------------------------------------------------------------------------------------------------------------------+
        | 0     | 0          | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | did:key:bob | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | {"a":0}                                     | []                                                                                                                         |
        | 1     | 1          | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | did:key:bob | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq |                                             | [baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi]                                                              |
        | 2     | 0          | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | did:key:bob | baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du | [{"op":"replace", "path": "/a", "value":1}] | [baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq, baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi] |
        +-------+------------+-------------------------------------------------------------+-------------+-------------------------------------------------------------+---------------------------------------------+----------------------------------------------------------------------------------------------------------------------------+"#]].assert_eq(&formatted);
    Ok(())
}

#[tokio::test]
async fn test_push_down_predicate() -> Result<()> {
    let mut feed = MockFeed::new();
    feed.expect_conclusion_events_since()
        .once()
        .with(predicate::eq(42), predicate::eq(2))
        .return_once(|h, l| Ok(events(h as u64).into_iter().take(l as usize).collect()));
    let mut client = start_server(feed).await;

    let info = client
        .execute(
            "SELECT * FROM conclusion_feed WHERE index > 42 LIMIT 2".to_string(),
            None,
        )
        .await?;
    let batch = execute_flight(&mut client, info).await?;
    let batches = pretty_feed_from_batch(batch).await;
    let formatted = pretty_format_batches(&batches).unwrap().to_string();
    expect![[r#"
        +-------+------------+-------------------------------------------------------------+-------------+-------------------------------------------------------------+---------+---------------------------------------------------------------+
        | index | event_type | stream_cid                                                  | controller  | event_cid                                                   | data    | previous                                                      |
        +-------+------------+-------------------------------------------------------------+-------------+-------------------------------------------------------------+---------+---------------------------------------------------------------+
        | 42    | 0          | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | did:key:bob | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | {"a":0} | []                                                            |
        | 43    | 1          | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | did:key:bob | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq |         | [baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi] |
        +-------+------------+-------------------------------------------------------------+-------------+-------------------------------------------------------------+---------+---------------------------------------------------------------+"#]].assert_eq(&formatted);
    Ok(())
}
