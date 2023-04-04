use crate::{error::Error, http::AppState, pubsub, IpfsDep};
use actix_multipart::Multipart;
use actix_web::{http::header::ContentType, web, HttpResponse, Scope};
use anyhow::anyhow;
use futures_util::StreamExt;
use iroh_api::{Bytes, GossipsubEvent};
use libipld::multibase::{self, Base};
use libp2p::gossipsub::GossipsubMessage;
use serde::{Deserialize, Serialize};

pub fn scope<T>() -> Scope
where
    T: IpfsDep + 'static,
{
    web::scope("/pubsub")
        .service(web::resource("/ls").route(web::post().to(list::<T>)))
        .service(web::resource("/pub").route(web::post().to(publish::<T>)))
        .service(web::resource("/sub").route(web::post().to(subscribe::<T>)))
}

#[tracing::instrument(skip(data))]
async fn list<T>(data: web::Data<AppState<T>>) -> Result<HttpResponse, Error>
where
    T: IpfsDep,
{
    let topics = pubsub::topics(data.api.clone()).await?;

    #[derive(Serialize)]
    struct ListResponse {
        #[serde(rename = "Strings")]
        strings: Vec<String>,
    }

    let connect_resp = ListResponse { strings: topics };
    let body = serde_json::to_vec(&connect_resp).map_err(|e| Error::Internal(e.into()))?;
    Ok(HttpResponse::Ok()
        .content_type(ContentType::json())
        .body(body))
}
#[derive(Debug, Deserialize)]
struct PublishQuery {
    arg: String,
}
#[tracing::instrument(skip(data, payload))]
async fn publish<T>(
    data: web::Data<AppState<T>>,
    query: web::Query<PublishQuery>,
    mut payload: Multipart,
) -> Result<HttpResponse, Error>
where
    T: IpfsDep,
{
    let (_base, topic_bytes) = multibase::decode(&query.arg).map_err(|e| {
        Error::Internal(Into::<anyhow::Error>::into(e).context("decoding multibase topic"))
    })?;
    let topic = String::from_utf8(topic_bytes).map_err(|e| {
        Error::Internal(Into::<anyhow::Error>::into(e).context("decoding topic as utf8 string"))
    })?;

    let mut fields = Vec::new();
    while let Some(item) = payload.next().await {
        let mut field = item.map_err(|e| {
            Error::Internal(Into::<anyhow::Error>::into(e).context("reading multipart field"))
        })?;
        fields.push(field.name().to_owned());
        if field.name() == "file" {
            let mut input_bytes: Vec<u8> = Vec::new();
            while let Some(chunk) = field.next().await {
                input_bytes.extend(
                    &chunk
                        .map_err(|e| {
                            Error::Internal(
                                Into::<anyhow::Error>::into(e).context("reading multipart chunk"),
                            )
                        })?
                        .to_vec(),
                )
            }

            pubsub::publish(data.api.clone(), topic, input_bytes.into()).await?;
            return Ok(HttpResponse::Ok().into());
        }
    }
    Err(Error::Invalid(anyhow!(
        "pubsub publish: missing multipart field 'file' found fields: {:?}",
        fields
    )))
}
#[derive(Debug, Deserialize)]
struct SubscribeQuery {
    arg: String,
}
#[tracing::instrument(skip(data))]
async fn subscribe<T>(
    data: web::Data<AppState<T>>,
    query: web::Query<SubscribeQuery>,
) -> Result<HttpResponse, Error>
where
    T: IpfsDep,
{
    let (_base, topic_bytes) = multibase::decode(&query.arg).map_err(|e| {
        Error::Internal(Into::<anyhow::Error>::into(e).context("decoding multibase topic"))
    })?;
    let topic = String::from_utf8(topic_bytes).map_err(|e| {
        Error::Internal(Into::<anyhow::Error>::into(e).context("decoding topic as utf8 string"))
    })?;

    let subscription = pubsub::subscribe(data.api.clone(), topic).await?;

    #[derive(Serialize)]
    struct MessageResponse {
        from: String,
        data: String,
        seqno: String,
        #[serde(rename = "topicIDs")]
        topic_ids: Vec<String>,
    }

    Ok(HttpResponse::Ok().streaming(subscription.map(|event| {
        event.map(|e| {
            let m = match e {
                // Ignore Subscribed and Unsubscribed events
                GossipsubEvent::Subscribed { .. } | GossipsubEvent::Unsubscribed { .. } => {
                    return Bytes::default()
                }
                GossipsubEvent::Message {
                    from: _,
                    id: _,
                    message:
                        GossipsubMessage {
                            source,
                            data,
                            sequence_number,
                            topic,
                        },
                } => MessageResponse {
                    from: source.map(|p| p.to_string()).unwrap_or_default(),
                    seqno: multibase::encode(
                        Base::Base64Url,
                        sequence_number
                            .map(|seqno| seqno.to_le_bytes())
                            .unwrap_or_default(),
                    ),
                    data: multibase::encode(Base::Base64Url, data),
                    topic_ids: vec![multibase::encode(
                        Base::Base64Url,
                        topic.to_string().as_bytes(),
                    )],
                },
            };
            let mut data = serde_json::to_vec(&m).expect("gossip event should serialize to JSON");
            data.push(b'\n');
            data.into()
        })
    })))
}
#[cfg(test)]
mod tests {

    use std::io::Cursor;
    use std::{collections::HashMap, str::FromStr};

    use crate::{
        error::Error,
        http::tests::{assert_body_json, assert_body_json_nl, build_server},
        IpfsDep, PeerInfo,
    };

    use actix_multipart_rfc7578::client::multipart;
    use actix_web::{body, test};
    use anyhow::anyhow;
    use async_stream::stream;
    use async_trait::async_trait;
    use expect_test::expect;
    use futures_util::{stream::BoxStream, StreamExt};
    use iroh_api::{Bytes, Cid, GossipsubEvent, IpfsPath, MessageId, Multiaddr, PeerId};
    use libipld::{
        multibase::{self, Base},
        Ipld,
    };
    use libp2p::gossipsub::{GossipsubMessage, TopicHash};
    use unimock::MockFn;
    use unimock::{matching, Unimock};

    use crate::IpfsDepMock;

    #[actix_web::test]
    async fn test_list() {
        let mock = Unimock::new(
            IpfsDepMock::topics
                .some_call(matching!(_))
                .returns(Ok(vec!["topicA".to_string(), "topicB".to_string()])),
        );
        let server = build_server(mock).await;
        let req = test::TestRequest::post().uri("/pubsub/ls").to_request();
        let resp = test::call_service(&server, req).await;
        assert!(resp.status().is_success());
        assert_eq!(
            "application/json",
            resp.headers().get("Content-Type").unwrap()
        );
        assert_body_json(
            resp.into_body(),
            expect![[r#"
                {
                  "Strings": [
                    "topicA",
                    "topicB"
                  ]
                }"#]],
        )
        .await;
    }

    #[actix_web::test]
    async fn test_publish() {
        let mock = Unimock::new(
            IpfsDepMock::publish
                .some_call(matching!((topic, _) if topic == "topicA"))
                .returns(Ok(())),
        );
        let topic_arg = multibase::encode(Base::Base64Url, "topicA");

        let mut form = multipart::Form::default();

        let file_bytes = Cursor::new(r#"{"some":"json","bytes":false}"#);
        form.add_reader_file("file", file_bytes, "");

        let ct = form.content_type();
        let body = body::to_bytes(multipart::Body::from(form)).await.unwrap();
        let server = build_server(mock).await;
        let req = test::TestRequest::post()
            .uri(format!("/pubsub/pub?arg={}", topic_arg).as_str())
            .insert_header(("Content-Type", ct))
            .set_payload(body)
            .to_request();
        let resp = test::call_service(&server, req).await;
        assert!(resp.status().is_success());
    }

    #[actix_web::test]
    async fn test_subscribe() {
        // Can't use unimock because it expects the stream to be Sync.
        // So we create a one off implementation of the trait and only implement the method we
        // need.
        // Maybe we could make a macro for this?

        #[derive(Clone)]
        struct TestDeps {}
        #[async_trait]
        impl IpfsDep for TestDeps {
            /// Get information about the local peer.
            async fn lookup_local(&self) -> Result<PeerInfo, Error> {
                todo!()
            }
            /// Get information about a peer.
            async fn lookup(&self, _peer_id: PeerId) -> Result<PeerInfo, Error> {
                todo!()
            }
            /// Get the size of an IPFS block.
            async fn block_size(&self, _cid: Cid) -> Result<u64, Error> {
                todo!()
            }
            /// Get a block from IPFS
            async fn block_get(&self, _cid: Cid) -> Result<Bytes, Error> {
                todo!()
            }
            /// Get a DAG node from IPFS returning the Cid of the resolved path and the bytes of the node.
            /// This will locally store the data as a result.
            async fn get(&self, _ipfs_path: &IpfsPath) -> Result<(Cid, Ipld), Error> {
                todo!()
            }
            /// Store a DAG node into IFPS.
            async fn put(&self, _cid: Cid, _blob: Bytes, _links: Vec<Cid>) -> Result<(), Error> {
                todo!()
            }
            /// Resolve an IPLD block.
            async fn resolve(&self, _ipfs_path: &IpfsPath) -> Result<(Cid, String), Error> {
                todo!()
            }
            /// Report all connected peers of the current node.
            async fn peers(&self) -> Result<HashMap<PeerId, Vec<Multiaddr>>, Error> {
                todo!()
            }
            /// Connect to a specific peer node.
            async fn connect(&self, _peer_id: PeerId, _addrs: Vec<Multiaddr>) -> Result<(), Error> {
                todo!()
            }
            /// Publish a message on a pub/sub Topic.
            async fn publish(&self, _topic: String, _data: Bytes) -> Result<(), Error> {
                todo!()
            }
            async fn subscribe(
                &self,
                topic: String,
            ) -> Result<BoxStream<'static, anyhow::Result<GossipsubEvent>>, Error> {
                if topic != "topicA" {
                    return Err(Error::Internal(anyhow!("unexpected topic")));
                }
                let first = Ok(GossipsubEvent::Message {
                    from: PeerId::from_str("12D3KooWHUfjwiTRVV8jxFcKRSQTPatayC4nQCNX86oxRF5XWzGe")
                        .unwrap(),
                    id: MessageId::new(&[]),
                    message: GossipsubMessage {
                        source: Some(
                            PeerId::from_str(
                                "12D3KooWM68GyFKBT9JsuTRB6CYkF61PtMuSkynUauSQEGBX51JW",
                            )
                            .unwrap(),
                        ),
                        data: "message 1".as_bytes().to_vec(),
                        sequence_number: Some(0),
                        topic: TopicHash::from_raw("topicA"),
                    },
                });
                let second = Ok(GossipsubEvent::Message {
                    from: PeerId::from_str("12D3KooWGnKwtpSh2ZLTvoC8mjiexMNRLNkT92pxq7MDgyJHktNJ")
                        .unwrap(),
                    id: MessageId::new(&[]),
                    message: GossipsubMessage {
                        source: Some(
                            PeerId::from_str(
                                "12D3KooWQVU9Pv3BqD6bD9w96tJxLedKCj4VZ75oqX9Tav4R4rUS",
                            )
                            .unwrap(),
                        ),
                        data: "message 2".as_bytes().to_vec(),
                        sequence_number: Some(1),
                        topic: TopicHash::from_raw("topicA"),
                    },
                });

                Ok(stream! {
                    yield first;
                    yield second;
                }
                .boxed())
            }
            async fn topics(&self) -> Result<Vec<String>, Error> {
                todo!()
            }
        }

        let mock = TestDeps {};
        let topic_arg = multibase::encode(Base::Base64Url, "topicA");

        let mut form = multipart::Form::default();

        let file_bytes = Cursor::new(r#"{"some":"json","bytes":false}"#);
        form.add_reader_file("file", file_bytes, "");

        let ct = form.content_type();
        let body = body::to_bytes(multipart::Body::from(form)).await.unwrap();
        let server = build_server(mock).await;
        let req = test::TestRequest::post()
            .uri(format!("/pubsub/sub?arg={}", topic_arg).as_str())
            .insert_header(("Content-Type", ct))
            .set_payload(body)
            .to_request();
        let resp = test::call_service(&server, req).await;
        assert!(resp.status().is_success());
        assert_body_json_nl(
            resp.into_body(),
            expect![[r#"
                {
                  "data": "ubWVzc2FnZSAx",
                  "from": "12D3KooWM68GyFKBT9JsuTRB6CYkF61PtMuSkynUauSQEGBX51JW",
                  "seqno": "uAAAAAAAAAAA",
                  "topicIDs": [
                    "udG9waWNB"
                  ]
                }
                {
                  "data": "ubWVzc2FnZSAy",
                  "from": "12D3KooWQVU9Pv3BqD6bD9w96tJxLedKCj4VZ75oqX9Tav4R4rUS",
                  "seqno": "uAQAAAAAAAAA",
                  "topicIDs": [
                    "udG9waWNB"
                  ]
                }
            "#]],
        )
        .await;
    }
}
