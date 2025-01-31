use std::{net::SocketAddr, str::FromStr as _, sync::Arc, time::Duration};

use anyhow::{bail, Context as _, Result};
use arrow::{compute::concat_batches, util::pretty::pretty_format_batches};
use arrow_array::RecordBatch;
use arrow_flight::{sql::client::FlightSqlServiceClient, FlightInfo};
use arrow_schema::Schema;
use ceramic_core::{StreamId, StreamIdType, METAMODEL_STREAM_ID};
use ceramic_flight::server::new_server;
use ceramic_pipeline::{
    aggregator::{ModelAccountRelationV2, ModelDefinition, SubscribeSinceMsg},
    ConclusionData, ConclusionEvent, ConclusionFeed, ConclusionInit, ConclusionTime,
    PipelineHandle,
};
use cid::Cid;
use expect_test::expect;
use futures::TryStreamExt as _;
use http::Uri;
use mockall::{mock, predicate};
use prometheus_client::registry::Registry;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use shutdown::Shutdown;
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

async fn start_server(feed: MockFeed) -> (FlightSqlServiceClient<Channel>, PipelineHandle) {
    let shutdown = Shutdown::new();
    let metrics = ceramic_pipeline::Metrics::register(&mut Registry::default());
    let (ctx, _) = ceramic_pipeline::spawn_actors(ceramic_pipeline::Config {
        aggregator: true,
        conclusion_feed: feed.into(),
        object_store: Arc::new(object_store::memory::InMemory::new()),
        metrics,
        shutdown,
    })
    .await
    .unwrap()
    .into_parts();
    let server = new_server(ctx.pipeline_ctx().session()).unwrap();
    // let OS choose a free port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_future =
        server.serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener));
    tokio::spawn(server_future);
    (FlightSqlServiceClient::new(channel(&addr).await), ctx)
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
        async fn max_highwater_mark(&self) -> anyhow::Result<Option<u64>>;
        async fn conclusion_events_since(
            &self,
            highwater_mark: i64,
            limit: i64,
        ) -> anyhow::Result<Vec<ConclusionEvent>>;
    }
}

#[derive(Deserialize, Serialize, JsonSchema)]
#[schemars(rename_all = "camelCase", deny_unknown_fields)]
pub struct Color {
    creator: String,
    red: i32,
    green: i32,
    blue: i32,
}

fn events(start_index: u64, highwater_mark: u64, limit: usize) -> Vec<ConclusionEvent> {
    let model_def = ModelDefinition::new_v2::<Color>(
        "TestColorModel".to_owned(),
        None,
        false,
        None,
        None,
        ModelAccountRelationV2::List,
    )
    .unwrap();

    // NOTE: These CIDs and StreamIDs are fake and do not represent the actual hash of the data.
    // This makes testing easier as changing the contents does not mean you need to update all of
    // the cids.
    let model_stream_id =
        StreamId::from_str("k2t6wz4yhfp1pc9l42mm6vh20xmhm9ac7cznnpu4xcxe4jds13l9sjknm1accd")
            .unwrap();
    let instance_stream_id =
        StreamId::from_str("k2t6wzhjp5kk11gmt3clbfsplu6dnug4gxix1mj86w9p0ddwv3hdehc9mjawx7")
            .unwrap();
    let stream_init = ConclusionInit {
        stream_cid: instance_stream_id.cid,
        stream_type: StreamIdType::ModelInstanceDocument as u8,
        controller: "did:key:alice".to_owned(),
        dimensions: vec![
            ("controller".to_owned(), b"did:key:alice".to_vec()),
            ("model".to_owned(), model_stream_id.to_vec()),
            ("unique".to_owned(), b"wgk53".to_vec()),
        ],
    };
    vec![
        ConclusionEvent::Data(ConclusionData {
            index: start_index + 0,
            event_cid: model_stream_id.cid,
            init: ConclusionInit {
                stream_cid: model_stream_id.cid,
                stream_type: StreamIdType::Model as u8,
                controller: "did:key:bob".to_owned(),
                dimensions: vec![
                    ("controller".to_owned(), b"did:key:bob".to_vec()),
                    ("model".to_owned(), METAMODEL_STREAM_ID.to_vec()),
                ],
            },
            previous: vec![],
            data: serde_json::to_string(&json!({
                "metadata":{
                    "foo":1,
                    "shouldIndex":true
                },
                "content": model_def,
            }))
            .unwrap()
            .into(),
        }),
        ConclusionEvent::Data(ConclusionData {
            index: start_index + 1,
            event_cid: Cid::from_str("baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi")
                .unwrap(),
            init: stream_init.clone(),
            previous: vec![],
            data: serde_json::to_string(&json!({
            "metadata":{
                "foo":1,
                "shouldIndex":true
            },
            "content":{
                "creator":"alice",
                "red":255,
                "green":255,
                "blue":255
            }}))
            .unwrap()
            .into(),
        }),
        ConclusionEvent::Time(ConclusionTime {
            index: start_index + 2,
            event_cid: Cid::from_str("baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq")
                .unwrap(),
            init: stream_init.clone(),
            previous: vec![Cid::from_str(
                "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
            )
            .unwrap()],
        }),
        ConclusionEvent::Data(ConclusionData {
            index: start_index + 3,
            event_cid: Cid::from_str("baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em")
                .unwrap(),
            init: stream_init.clone(),
            previous: vec![Cid::from_str(
                "baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq",
            )
            .unwrap()],
            data: serde_json::to_string(&json!({
            "metadata":{"foo":2},
            "content":[{
                "op":"replace",
                "path": "/red",
                "value":0
            }]}))
            .unwrap()
            .into(),
        }),
    ]
    .into_iter()
    .filter(|e| e.index() > highwater_mark)
    .take(limit)
    .collect()
}

#[test(tokio::test)]
async fn conclusion_events() -> Result<()> {
    let mut feed = MockFeed::new();
    feed.expect_max_highwater_mark()
        .once()
        .return_once(|| Ok(None));
    feed.expect_conclusion_events_since()
        .returning(|h, l| Ok(events(1, h as u64, l as usize)));

    let (mut client, _ctx) = start_server(feed).await;

    let info = client
        .execute(
            r#"
            SELECT
                "index",
                cid_string(stream_cid) as stream_cid,
                stream_type,
                controller,
                dimensions,
                cid_string(event_cid) as event_cid,
                event_type,
                data::varchar as data,
                array_cid_string(previous) as previous
            FROM conclusion_events"#
                .to_string(),
            None,
        )
        .await?;
    let batch = execute_flight(&mut client, info).await?;
    let formatted = pretty_format_batches(&[batch]).unwrap().to_string();
    expect![[r#"
        +-------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------+
        | index | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                          | event_cid                                                   | event_type | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | previous                                                      |
        +-------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------+
        | 1     | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob   | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631}                                                                   | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | {"content":{"accountRelation":{"type":"list"},"implements":[],"interface":false,"name":"TestColorModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"Color","type":"object"},"version":"2.0","views":{}},"metadata":{"foo":1,"shouldIndex":true}} |                                                               |
        | 2     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                             |                                                               |
        | 3     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | [baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi] |
        | 4     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em | 0          | {"content":[{"op":"replace","path":"/red","value":0}],"metadata":{"foo":2}}                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | [baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq] |
        +-------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------+"#]].assert_eq(&formatted);
    Ok(())
}

#[test(tokio::test)]
async fn conclusion_push_down_predicate() -> Result<()> {
    let mut feed = MockFeed::new();
    feed.expect_max_highwater_mark()
        .once()
        .return_once(|| Ok(None));
    feed.expect_conclusion_events_since()
        .once()
        .with(predicate::eq(42), predicate::eq(2))
        .return_once(|h, l| Ok(events(h as u64, h as u64, l as usize)));
    let (mut client, _ctx) = start_server(feed).await;

    let info = client
        .execute(
            r#"
            SELECT
                "index",
                cid_string(stream_cid) as stream_cid,
                stream_type,
                controller,
                dimensions,
                cid_string(event_cid) as event_cid,
                event_type,
                data::varchar as data,
                array_cid_string(previous) as previous
            FROM conclusion_events
            WHERE index > 42 LIMIT 2"#
                .to_string(),
            None,
        )
        .await?;
    let batch = execute_flight(&mut client, info).await?;
    let formatted = pretty_format_batches(&[batch]).unwrap().to_string();
    expect![[r#"
        +-------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+----------------------------------------------------------------------------------------------------------+---------------------------------------------------------------+
        | index | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                          | event_cid                                                   | event_type | data                                                                                                     | previous                                                      |
        +-------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+----------------------------------------------------------------------------------------------------------+---------------------------------------------------------------+
        | 43    | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}} |                                                               |
        | 44    | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          |                                                                                                          | [baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi] |
        +-------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+----------------------------------------------------------------------------------------------------------+---------------------------------------------------------------+"#]].assert_eq(&formatted);
    Ok(())
}

#[test(tokio::test)]
async fn conclusion_events_feed() -> Result<()> {
    let mut feed = MockFeed::new();
    feed.expect_max_highwater_mark()
        .once()
        .return_once(|| Ok(None));
    feed.expect_conclusion_events_since()
        .returning(|h, l| Ok(events(1, h as u64, l as usize)));

    let (mut client, _ctx) = start_server(feed).await;

    let info = client
        .execute(
            r#"
            SELECT
                "index",
                cid_string(stream_cid) as stream_cid,
                stream_type,
                controller,
                dimensions,
                cid_string(event_cid) as event_cid,
                event_type,
                data::varchar as data,
                array_cid_string(previous) as previous
            FROM conclusion_events_feed LIMIT 4"#
                .to_string(),
            None,
        )
        .await?;
    let batch = execute_flight(&mut client, info).await?;
    let formatted = pretty_format_batches(&[batch]).unwrap().to_string();
    expect![[r#"
        +-------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------+
        | index | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                          | event_cid                                                   | event_type | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | previous                                                      |
        +-------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------+
        | 1     | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob   | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631}                                                                   | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | {"content":{"accountRelation":{"type":"list"},"implements":[],"interface":false,"name":"TestColorModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"Color","type":"object"},"version":"2.0","views":{}},"metadata":{"foo":1,"shouldIndex":true}} |                                                               |
        | 2     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                             |                                                               |
        | 3     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | [baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi] |
        | 4     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em | 0          | {"content":[{"op":"replace","path":"/red","value":0}],"metadata":{"foo":2}}                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | [baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq] |
        +-------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------+"#]].assert_eq(&formatted);
    Ok(())
}

#[test(tokio::test)]
async fn event_states_simple() -> Result<()> {
    let mut feed = MockFeed::new();
    feed.expect_max_highwater_mark()
        .once()
        .return_once(|| Ok(None));
    feed.expect_conclusion_events_since()
        .returning(|h, l| Ok(events(1, h as u64, l as usize)));

    let (mut client, ctx) = start_server(feed).await;

    // Wait until we have processed all events before we try to query.
    // This way the test can be both fast and robust.
    if let Some(aggregator) = ctx.aggregator() {
        let mut sub = aggregator
            .send(SubscribeSinceMsg {
                projection: None,
                offset: None,
                // We know there are only four events, query all of them
                limit: Some(4),
            })
            .await??;
        while sub.try_next().await?.is_some() {}
    }

    let info = client
        .execute(
            r#"
            SELECT
                "index",
                cid_string(stream_cid) as stream_cid,
                stream_type,
                controller,
                dimensions,
                cid_string(event_cid) as event_cid,
                event_type,
                data::varchar as data
            FROM event_states LIMIT 4"#
                .to_string(),
            None,
        )
        .await?;
    let batch = execute_flight(&mut client, info).await?;
    let formatted = pretty_format_batches(&[batch]).unwrap().to_string();
    expect![[r#"
        +-------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        | index | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                          | event_cid                                                   | event_type | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
        +-------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        | 1     | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob   | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631}                                                                   | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | {"content":{"accountRelation":{"type":"list"},"implements":[],"interface":false,"name":"TestColorModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"Color","type":"object"},"version":"2.0","views":{}},"metadata":{"foo":1,"shouldIndex":true}} |
        | 2     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                             |
        | 3     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                             |
        | 4     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em | 0          | {"metadata":{"foo":2,"shouldIndex":true},"content":{"blue":255,"creator":"alice","green":255,"red":0}}                                                                                                                                                                                                                                                                                                                                                                                                                                               |
        +-------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+"#]].assert_eq(&formatted);
    Ok(())
}
#[test(tokio::test)]
async fn event_states_feed() -> Result<()> {
    let mut feed = MockFeed::new();
    feed.expect_max_highwater_mark()
        .once()
        .return_once(|| Ok(None));
    feed.expect_conclusion_events_since()
        .returning(|h, l| Ok(events(1, h as u64, l as usize)));

    let (mut client, _ctx) = start_server(feed).await;

    let info = client
        .execute(
            r#"
            SELECT
                "index",
                cid_string(stream_cid) as stream_cid,
                stream_type,
                controller,
                dimensions,
                cid_string(event_cid) as event_cid,
                event_type,
                data::varchar as data
            FROM event_states_feed LIMIT 4"#
                .to_string(),
            None,
        )
        .await?;
    let batch = execute_flight(&mut client, info).await?;
    let formatted = pretty_format_batches(&[batch]).unwrap().to_string();
    expect![[r#"
        +-------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        | index | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                          | event_cid                                                   | event_type | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
        +-------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        | 1     | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob   | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631}                                                                   | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | {"content":{"accountRelation":{"type":"list"},"implements":[],"interface":false,"name":"TestColorModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"Color","type":"object"},"version":"2.0","views":{}},"metadata":{"foo":1,"shouldIndex":true}} |
        | 2     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                             |
        | 3     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                             |
        | 4     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em | 0          | {"metadata":{"foo":2,"shouldIndex":true},"content":{"blue":255,"creator":"alice","green":255,"red":0}}                                                                                                                                                                                                                                                                                                                                                                                                                                               |
        +-------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+"#]].assert_eq(&formatted);
    Ok(())
}

#[test(tokio::test)]
async fn event_states_feed_projection() -> Result<()> {
    // This ensures that a simple projection of the stream table works.
    // Any extra functions or casts should not be used so we ensure we are excersicing the schema
    // of the stream table directly.

    let mut feed = MockFeed::new();
    feed.expect_max_highwater_mark()
        .once()
        .return_once(|| Ok(None));
    feed.expect_conclusion_events_since()
        .returning(|h, l| Ok(events(1, h as u64, l as usize)));

    let (mut client, _ctx) = start_server(feed).await;

    let info = client
        .execute(
            r#"
            SELECT
                event_type
            FROM event_states_feed LIMIT 4"#
                .to_string(),
            None,
        )
        .await?;
    let batch = execute_flight(&mut client, info).await?;
    let formatted = pretty_format_batches(&[batch]).unwrap().to_string();
    expect![[r#"
        +------------+
        | event_type |
        +------------+
        | 0          |
        | 0          |
        | 1          |
        | 0          |
        +------------+"#]]
    .assert_eq(&formatted);
    Ok(())
}
