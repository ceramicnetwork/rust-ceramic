//! Provides an http implementation of the Kubo RPC methods.
use std::{collections::HashSet, io::Cursor, marker::PhantomData, str::FromStr};

use async_trait::async_trait;
use ceramic_kubo_rpc_server::models::DagPutPost200ResponseCid;
use ceramic_kubo_rpc_server::{
    models::{
        self, BlockPutPost200Response, Codecs, DagImportPost200Response, DagPutPost200Response,
        DagResolvePost200Response, DagResolvePost200ResponseCid, IdPost200Response, Multihash,
        PinAddPost200Response, PubsubLsPost200Response, SwarmPeersPost200Response,
        SwarmPeersPost200ResponsePeersInner, VersionPost200Response,
    },
    Api, BlockGetPostResponse, BlockPutPostResponse, BlockStatPostResponse, DagGetPostResponse,
    DagImportPostResponse, DagPutPostResponse, DagResolvePostResponse, IdPostResponse,
    PinAddPostResponse, PinRmPostResponse, PubsubLsPostResponse, PubsubPubPostResponse,
    PubsubSubPostResponse, SwarmConnectPostResponse, SwarmPeersPostResponse, VersionPostResponse,
};
use cid::{
    multibase::{self, Base},
    Cid,
};
use dag_jose::DagJoseCodec;
use futures_util::StreamExt;
use libipld::{cbor::DagCborCodec, json::DagJsonCodec, raw::RawCodec};
use libp2p::{gossipsub::Message, Multiaddr, PeerId};
use multiaddr::Protocol;
use serde::Serialize;
use swagger::{ApiError, ByteArray};

use crate::{
    block, dag, id, pin, pubsub, swarm, version, Bytes, GossipsubEvent, IpfsDep, IpfsPath,
};

/// Kubo RPC API Server implementation.
#[derive(Clone)]
pub struct Server<I, C> {
    ipfs: I,
    marker: PhantomData<C>,
}
impl<I, C> Server<I, C>
where
    I: IpfsDep,
{
    /// Construct a new Server
    pub fn new(ipfs: I) -> Self {
        Self {
            ipfs,
            marker: PhantomData,
        }
    }
}

fn to_api_error(err: impl std::fmt::Display) -> ApiError {
    ApiError(format!("{err}"))
}

fn create_error(msg: &str) -> models::Error {
    models::Error {
        message: msg.to_string(),
        code: 0f64,
        typ: "error".to_string(),
    }
}

// Helpful macro for early return of bad request if a Result errs.
// Similar to ? but returns Ok($t::BadRequest(err))
macro_rules! try_or_bad_request {
    ($e:expr, $t:ty) => {
        match $e {
            Ok(r) => r,
            Err(err) => return Ok(<$t>::BadRequest(create_error(&format!("{err}")))),
        }
    };
}

#[async_trait]
impl<I, C> Api<C> for Server<I, C>
where
    I: IpfsDep + Send + Sync,
    C: Send + Sync,
{
    async fn block_get_post(
        &self,
        arg: String,
        _context: &C,
    ) -> Result<BlockGetPostResponse, ApiError> {
        let cid = try_or_bad_request!(Cid::from_str(&arg), BlockGetPostResponse);
        let data = block::get(self.ipfs.clone(), cid)
            .await
            .map_err(to_api_error)?;
        Ok(BlockGetPostResponse::Success(ByteArray(data)))
    }

    async fn block_put_post(
        &self,
        file: ByteArray,
        cid_codec: Option<Codecs>,
        mhtype: Option<Multihash>,
        pin: Option<bool>,
        _context: &C,
    ) -> Result<BlockPutPostResponse, ApiError> {
        if let Some(pin) = pin {
            if pin {
                return Ok(BlockPutPostResponse::BadRequest(create_error(
                    "recursive pinning is not supported",
                )));
            }
        };
        if let Some(mhtype) = mhtype {
            if mhtype != Multihash::Sha2256 {
                return Ok(BlockPutPostResponse::BadRequest(create_error(
                    "unsupported multihash type",
                )));
            }
        };

        let size = file.0.len();
        let cid = match cid_codec.unwrap_or(Codecs::Raw) {
            Codecs::Raw => block::put(self.ipfs.clone(), RawCodec, file.0)
                .await
                .map_err(to_api_error)?,
            Codecs::DagCbor => block::put(self.ipfs.clone(), DagCborCodec, file.0)
                .await
                .map_err(to_api_error)?,
            Codecs::DagJson => block::put(self.ipfs.clone(), DagJsonCodec, file.0)
                .await
                .map_err(to_api_error)?,
            Codecs::DagJose => block::put(self.ipfs.clone(), DagJoseCodec, file.0)
                .await
                .map_err(to_api_error)?,
        };
        Ok(BlockPutPostResponse::Success(BlockPutPost200Response {
            key: cid.to_string(),
            size: size as f64,
        }))
    }

    async fn block_stat_post(
        &self,
        arg: String,
        _context: &C,
    ) -> Result<BlockStatPostResponse, ApiError> {
        let cid = try_or_bad_request!(Cid::from_str(&arg), BlockStatPostResponse);
        let size = block::stat(self.ipfs.clone(), cid)
            .await
            .map_err(to_api_error)?;
        Ok(BlockStatPostResponse::Success(BlockPutPost200Response {
            key: cid.to_string(),
            size: size as f64,
        }))
    }

    async fn dag_get_post(
        &self,
        arg: String,
        output_codec: Option<Codecs>,
        _context: &C,
    ) -> Result<DagGetPostResponse, ApiError> {
        let ipfs_path = try_or_bad_request!(IpfsPath::from_str(&arg), DagGetPostResponse);
        match output_codec.unwrap_or(Codecs::DagJson) {
            Codecs::DagJson => Ok(DagGetPostResponse::Success(ByteArray(
                dag::get(self.ipfs.clone(), &ipfs_path, DagJsonCodec)
                    .await
                    .map_err(to_api_error)?,
            ))),
            Codecs::DagCbor => Ok(DagGetPostResponse::Success(ByteArray(
                dag::get(self.ipfs.clone(), &ipfs_path, DagCborCodec)
                    .await
                    .map_err(to_api_error)?,
            ))),
            Codecs::DagJose | Codecs::Raw => Ok(DagGetPostResponse::BadRequest(create_error(
                &format!("unsupported output codec: {output_codec:?}"),
            ))),
        }
    }

    async fn dag_import_post(
        &self,
        file: swagger::ByteArray,
        _context: &C,
    ) -> Result<DagImportPostResponse, ApiError> {
        let cids = dag::import(self.ipfs.clone(), file.0.as_slice())
            .await
            .map_err(to_api_error)?;
        Ok(DagImportPostResponse::Success(DagImportPost200Response {
            root: DagPutPost200Response {
                cid: DagPutPost200ResponseCid {
                    // We know that the CAR file will have at least one root at this point,
                    // otherwise we'd have errored out during the import.
                    slash: cids[0].to_string(),
                },
            },
        }))
    }

    async fn dag_put_post(
        &self,
        file: ByteArray,
        store_codec: Option<Codecs>,
        input_codec: Option<Codecs>,
        _context: &C,
    ) -> Result<DagPutPostResponse, ApiError> {
        let mut file = Cursor::new(file.0);
        let cid = match (
            input_codec.unwrap_or(Codecs::DagJson),
            store_codec.unwrap_or(Codecs::DagCbor),
        ) {
            (Codecs::DagJson, Codecs::DagJson) => {
                dag::put(self.ipfs.clone(), DagJsonCodec, DagJsonCodec, &mut file)
                    .await
                    .map_err(to_api_error)?
            }
            (Codecs::DagJson, Codecs::DagCbor) => {
                dag::put(self.ipfs.clone(), DagJsonCodec, DagCborCodec, &mut file)
                    .await
                    .map_err(to_api_error)?
            }
            (Codecs::DagCbor, Codecs::DagCbor) => {
                dag::put(self.ipfs.clone(), DagCborCodec, DagCborCodec, &mut file)
                    .await
                    .map_err(to_api_error)?
            }
            (Codecs::DagJose, Codecs::DagJose) => {
                dag::put(self.ipfs.clone(), DagJoseCodec, DagJoseCodec, &mut file)
                    .await
                    .map_err(to_api_error)?
            }
            (input, store) => {
                return Ok(DagPutPostResponse::BadRequest(create_error(&format!(
                    "unsupported codec combination, input-codec: {input}, store-codec: {store}",
                ))))
            }
        };

        Ok(DagPutPostResponse::Success(DagPutPost200Response {
            cid: DagPutPost200ResponseCid {
                slash: cid.to_string(),
            },
        }))
    }

    async fn dag_resolve_post(
        &self,
        arg: String,
        _context: &C,
    ) -> Result<DagResolvePostResponse, ApiError> {
        let path = try_or_bad_request!(IpfsPath::from_str(&arg), DagResolvePostResponse);
        let (cid, rem_path) = dag::resolve(self.ipfs.clone(), &path)
            .await
            .map_err(to_api_error)?;
        Ok(DagResolvePostResponse::Success(DagResolvePost200Response {
            cid: DagResolvePost200ResponseCid {
                slash: cid.to_string(),
            },
            rem_path,
        }))
    }

    async fn id_post(&self, arg: Option<String>, _context: &C) -> Result<IdPostResponse, ApiError> {
        let info = if let Some(id) = &arg {
            let peer_id = try_or_bad_request!(PeerId::from_str(id), IdPostResponse);
            id::lookup(self.ipfs.clone(), peer_id)
                .await
                .map_err(to_api_error)?
        } else {
            id::lookup_local(self.ipfs.clone())
                .await
                .map_err(to_api_error)?
        };
        Ok(IdPostResponse::Success(IdPost200Response {
            id: info.peer_id.to_string(),
            addresses: info
                .listen_addrs
                .into_iter()
                .map(|a| a.to_string())
                .collect(),
            agent_version: info.agent_version,
            protocol_version: info.protocol_version,
            protocols: info.protocols,
        }))
    }

    async fn pin_add_post(
        &self,
        arg: String,
        recursive: Option<bool>,
        progress: Option<bool>,
        _context: &C,
    ) -> Result<PinAddPostResponse, ApiError> {
        if let Some(recursive) = recursive {
            if recursive {
                return Ok(PinAddPostResponse::BadRequest(create_error(
                    "recursive pinning is not supported",
                )));
            }
        };
        if let Some(progress) = progress {
            if progress {
                return Ok(PinAddPostResponse::BadRequest(create_error(
                    "pin progress is not supported",
                )));
            }
        };

        let ipfs_path = try_or_bad_request!(IpfsPath::from_str(&arg), PinAddPostResponse);
        let cid = pin::add(self.ipfs.clone(), &ipfs_path)
            .await
            .map_err(to_api_error)?;
        Ok(PinAddPostResponse::Success(PinAddPost200Response {
            pins: vec![cid.to_string()],
        }))
    }
    async fn pin_rm_post(&self, arg: String, _context: &C) -> Result<PinRmPostResponse, ApiError> {
        let ipfs_path = try_or_bad_request!(IpfsPath::from_str(&arg), PinRmPostResponse);
        let cid = pin::remove(self.ipfs.clone(), &ipfs_path)
            .await
            .map_err(to_api_error)?;
        Ok(PinRmPostResponse::Success(PinAddPost200Response {
            pins: vec![cid.to_string()],
        }))
    }

    async fn pubsub_ls_post(&self, _context: &C) -> Result<PubsubLsPostResponse, ApiError> {
        let topics = pubsub::topics(self.ipfs.clone())
            .await
            .map_err(to_api_error)?;
        Ok(PubsubLsPostResponse::Success(PubsubLsPost200Response {
            strings: topics,
        }))
    }

    async fn pubsub_pub_post(
        &self,
        arg: String,
        file: swagger::ByteArray,
        _context: &C,
    ) -> Result<PubsubPubPostResponse, ApiError> {
        let (_base, topic_bytes) =
            try_or_bad_request!(multibase::decode(&arg), PubsubPubPostResponse);

        let topic = try_or_bad_request!(String::from_utf8(topic_bytes), PubsubPubPostResponse);

        pubsub::publish(self.ipfs.clone(), topic, file.0.into())
            .await
            .map_err(to_api_error)?;
        Ok(PubsubPubPostResponse::Success)
    }

    async fn pubsub_sub_post(
        &self,
        arg: String,
        _context: &C,
    ) -> Result<PubsubSubPostResponse, ApiError> {
        let (_base, topic_bytes) =
            try_or_bad_request!(multibase::decode(&arg), PubsubSubPostResponse);

        let topic = try_or_bad_request!(String::from_utf8(topic_bytes), PubsubSubPostResponse);

        let subscription = pubsub::subscribe(self.ipfs.clone(), topic)
            .await
            .map_err(to_api_error)?;

        #[derive(Serialize)]
        struct MessageResponse {
            from: String,
            data: String,
            seqno: String,
            #[serde(rename = "topicIDs")]
            topic_ids: Vec<String>,
        }

        let messages = subscription.map(|event| {
            event
                .map(|e| {
                    let m = match e {
                        // Ignore Subscribed and Unsubscribed events
                        GossipsubEvent::Subscribed { .. } | GossipsubEvent::Unsubscribed { .. } => {
                            return Bytes::default()
                        }
                        GossipsubEvent::Message {
                            from: _,
                            id: _,
                            message:
                                Message {
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
                    let mut data =
                        serde_json::to_vec(&m).expect("gossip event should serialize to JSON");
                    data.push(b'\n');
                    data.into()
                })
                .map_err(Box::<dyn std::error::Error + Send + Sync>::from)
        });

        Ok(PubsubSubPostResponse::Success(Box::pin(messages)))
    }

    async fn swarm_connect_post(
        &self,
        arg: &Vec<String>,
        _context: &C,
    ) -> Result<SwarmConnectPostResponse, ApiError> {
        // Iterate over each arg and parse it as a multiaddr and search for peer ids.
        let iter = arg.iter().map(
            |addr| -> Result<(Multiaddr, Option<PeerId>), anyhow::Error> {
                let addr = Multiaddr::from_str(addr)?;
                let peer_id = addr
                    .iter()
                    .flat_map(|proto| match proto {
                        Protocol::P2p(mh) => vec![mh],
                        _ => Vec::new(),
                    })
                    .next()
                    .map(|mh| -> Result<PeerId, _> {
                        PeerId::from_multihash(mh)
                            .map_err(|_err| anyhow::anyhow!("invalid peer id"))
                    })
                    .transpose()?;
                Ok((addr, peer_id))
            },
        );

        let (addrs, peer_ids) = try_or_bad_request!(
            itertools::process_results(iter, |iter| iter.unzip::<_, _, Vec<_>, Vec<_>>()),
            SwarmConnectPostResponse
        );

        // Check we found exactly one unique peer id.
        let peer_ids: HashSet<PeerId> = peer_ids
            .into_iter()
            .flat_map(|p| if let Some(p) = p { vec![p] } else { Vec::new() })
            .collect();
        let peer_id = match peer_ids.len() {
            0 => {
                return Ok(SwarmConnectPostResponse::BadRequest(create_error(
                    "no peer id specificed in multiaddrs",
                )))
            }
            1 => peer_ids
                .into_iter()
                .next()
                .expect("unreachable: should be exactly one peer_id"),
            _ => {
                return Ok(SwarmConnectPostResponse::BadRequest(create_error(
                    "found multiple distinct peer ids",
                )))
            }
        };

        // Connect to the peer for all its addrs
        swarm::connect(self.ipfs.clone(), peer_id, addrs)
            .await
            .map_err(to_api_error)?;
        Ok(SwarmConnectPostResponse::Success(PubsubLsPost200Response {
            strings: vec![format!("connect {} success", peer_id)],
        }))
    }

    async fn swarm_peers_post(&self, _context: &C) -> Result<SwarmPeersPostResponse, ApiError> {
        let peers: Vec<SwarmPeersPost200ResponsePeersInner> = swarm::peers(self.ipfs.clone())
            .await
            .map_err(to_api_error)?
            .into_iter()
            .map(|(k, v)| SwarmPeersPost200ResponsePeersInner {
                addr: v
                    .get(0)
                    .map(|a| a.to_string())
                    .unwrap_or_else(|| "".to_string()),
                peer: k.to_string(),
            })
            .collect();
        Ok(SwarmPeersPostResponse::Success(SwarmPeersPost200Response {
            peers,
        }))
    }

    async fn version_post(&self, _context: &C) -> Result<VersionPostResponse, ApiError> {
        let v = version::version(self.ipfs.clone())
            .await
            .map_err(to_api_error)?;
        Ok(VersionPostResponse::Success(VersionPost200Response {
            commit: v.commit,
            system: format!("{}/{}", v.arch, v.os),
            version: v.version,
        }))
    }
}

#[derive(Serialize)]
struct ErrorJson<'a> {
    #[serde(rename = "Message")]
    pub message: String,
    #[serde(rename = "Code")]
    pub code: i32,
    #[serde(rename = "Type")]
    pub typ: &'a str,
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use super::*;
    use crate::{tests::MockIpfsDepTest, PeerInfo};

    use async_stream::stream;
    use ceramic_metadata::Version;
    use cid::multibase::{self, Base};
    use futures_util::TryStreamExt;
    use libipld::{pb::DagPbCodec, prelude::Decode, Ipld};
    use libp2p::gossipsub::{Message, TopicHash};
    use mockall::predicate;
    use tracing_test::traced_test;

    use expect_test::expect;

    // Empty Context
    struct Context;

    // Struct with unquoted debug format
    struct UnquotedString(String);

    impl std::fmt::Debug for UnquotedString {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    // Helper type for asserting responses with binary data.
    #[derive(Debug)]
    enum DebugResponse {
        Success(UnquotedString),
        BadRequest(models::Error),
    }

    impl From<BlockGetPostResponse> for DebugResponse {
        fn from(value: BlockGetPostResponse) -> Self {
            match value {
                BlockGetPostResponse::Success(data) => {
                    DebugResponse::Success(UnquotedString(hex::encode(data.0)))
                }
                BlockGetPostResponse::BadRequest(err) => DebugResponse::BadRequest(err),
            }
        }
    }
    impl From<DagGetPostResponse> for DebugResponse {
        fn from(value: DagGetPostResponse) -> Self {
            match value {
                DagGetPostResponse::Success(data) => {
                    DebugResponse::Success(UnquotedString(bytes_to_pretty_str(data.0)))
                }
                DagGetPostResponse::BadRequest(err) => DebugResponse::BadRequest(err),
            }
        }
    }

    // Construct a string from bytes using the following options until one succeeds:
    //  1. Pretty formatted JSON
    //  2. UTF-8 String
    //  3. Hex encoded bytes
    fn bytes_to_pretty_str(bytes: Vec<u8>) -> String {
        if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&bytes) {
            return serde_json::to_string_pretty(&json).unwrap();
        }

        match String::from_utf8(bytes.clone()) {
            Ok(s) => s,
            Err(_) => hex::encode(bytes),
        }
    }

    #[tokio::test]
    #[traced_test]
    async fn block_get() {
        // Test data from:
        // https://ipld.io/specs/codecs/dag-pb/fixtures/cross-codec/#dagpb_data_some
        let data = hex::decode("0a050001020304").unwrap();
        let cid =
            Cid::from_str("bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom").unwrap();
        let mut mock_ipfs = MockIpfsDepTest::new();
        mock_ipfs.expect_clone().once().return_once(move || {
            let mut m = MockIpfsDepTest::new();
            m.expect_block_get()
                .once()
                .with(predicate::eq(cid))
                .return_once(move |_| Ok(Bytes::from(data)));
            m
        });
        let server = Server::new(mock_ipfs);
        let resp = server
            .block_get_post(cid.to_string(), &Context)
            .await
            .unwrap();

        expect![[r#"
            Success(
                0a050001020304,
            )
        "#]]
        .assert_debug_eq(&DebugResponse::from(resp));
    }

    #[tokio::test]
    #[traced_test]
    async fn block_get_bad_request() {
        let mock_ipfs = MockIpfsDepTest::new();
        let server = Server::new(mock_ipfs);

        let resp = server
            .block_get_post("invalid cid".to_string(), &Context)
            .await
            .unwrap();

        expect![[r#"
            BadRequest(
                Error {
                    message: "Failed to parse multihash",
                    code: 0.0,
                    typ: "error",
                },
            )
        "#]]
        .assert_debug_eq(&DebugResponse::from(resp));
    }

    #[tokio::test]
    #[traced_test]
    async fn block_put() {
        // Test data from:
        // https://ipld.io/specs/codecs/dag-json/fixtures/cross-codec/#array-mixed
        let cbor_cid =
            Cid::from_str("bafyreidufmzzejc3p7gmh6ivp4fjvca5jfazk57nu6vdkvki4c4vpja724").unwrap();

        // Cbor encoded bytes
        let file = hex::decode("8c1b0016db6db6db6db71a000100001901f40200202238ff3aa5f702b33b0016db6db6db6db74261316fc48c6175657320c39f76c49b746521").unwrap();
        let blob = Bytes::from(file.clone());
        let mut mock_ipfs = MockIpfsDepTest::new();
        mock_ipfs.expect_clone().once().return_once(move || {
            let mut m = MockIpfsDepTest::new();
            m.expect_put()
                .once()
                .with(
                    predicate::eq(cbor_cid),
                    predicate::eq(blob),
                    predicate::eq(vec![]),
                )
                .return_once(move |_, _, _| Ok(()));
            m
        });
        let server = Server::new(mock_ipfs);
        let resp = server
            .block_put_post(
                ByteArray(file),
                Some(Codecs::DagCbor),
                Some(Multihash::Sha2256),
                Some(false),
                &Context,
            )
            .await
            .unwrap();

        expect![[r#"
            Success(
                BlockPutPost200Response {
                    key: "bafyreidufmzzejc3p7gmh6ivp4fjvca5jfazk57nu6vdkvki4c4vpja724",
                    size: 57.0,
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }

    #[tokio::test]
    #[traced_test]
    async fn block_put_bad_request() {
        let mock_ipfs = MockIpfsDepTest::new();
        let server = Server::new(mock_ipfs);

        let resp = server
            .block_put_post(
                ByteArray(vec![]),
                Some(Codecs::DagCbor),
                Some(Multihash::Sha2256),
                Some(true),
                &Context,
            )
            .await
            .unwrap();

        expect![[r#"
            BadRequest(
                Error {
                    message: "recursive pinning is not supported",
                    code: 0.0,
                    typ: "error",
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }

    #[tokio::test]
    #[traced_test]
    async fn block_stat() {
        // Test data from:
        // https://ipld.io/specs/codecs/dag-pb/fixtures/cross-codec/#dagpb_data_some
        let cid =
            Cid::from_str("bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom").unwrap();
        let mut mock_ipfs = MockIpfsDepTest::new();
        mock_ipfs.expect_clone().once().return_once(move || {
            let mut m = MockIpfsDepTest::new();
            m.expect_block_size()
                .once()
                .with(predicate::eq(cid))
                .return_once(move |_| Ok(7));
            m
        });
        let server = Server::new(mock_ipfs);
        let resp = server
            .block_stat_post(cid.to_string(), &Context)
            .await
            .unwrap();

        expect![[r#"
            Success(
                BlockPutPost200Response {
                    key: "bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom",
                    size: 7.0,
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }

    #[tokio::test]
    #[traced_test]
    async fn block_stat_bad_request() {
        let mock_ipfs = MockIpfsDepTest::new();
        let server = Server::new(mock_ipfs);

        let resp = server
            .block_stat_post("bad cid".to_string(), &Context)
            .await
            .unwrap();

        expect![[r#"
            BadRequest(
                Error {
                    message: "Failed to parse multihash",
                    code: 0.0,
                    typ: "error",
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }

    #[tokio::test]
    #[traced_test]
    async fn dag_get_json() {
        // Test data from:
        // https://ipld.io/specs/codecs/dag-pb/fixtures/cross-codec/#dagpb_data_some
        let data = Ipld::decode(
            DagPbCodec,
            &mut Cursor::new(hex::decode("0a050001020304").expect("should be valid hex data")),
        )
        .expect("should be valid dag-pb data");

        let cid =
            Cid::from_str("bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom").unwrap();
        let ipfs_path = IpfsPath::from_str(&cid.to_string()).unwrap();
        let mut mock_ipfs = MockIpfsDepTest::new();
        mock_ipfs.expect_clone().once().return_once(move || {
            let mut m = MockIpfsDepTest::new();
            m.expect_get()
                .once()
                .with(predicate::eq(ipfs_path))
                .return_once(move |_| Ok((cid, data)));
            m
        });
        let server = Server::new(mock_ipfs);
        let resp = server
            .dag_get_post(cid.to_string(), None, &Context)
            .await
            .unwrap();

        expect![[r#"
            Success(
                {
                  "Data": {
                    "/": {
                      "bytes": "AAECAwQ"
                    }
                  },
                  "Links": []
                },
            )
        "#]]
        .assert_debug_eq(&DebugResponse::from(resp));
    }
    #[tokio::test]
    #[traced_test]
    async fn dag_get_cbor() {
        // Test data from:
        // https://ipld.io/specs/codecs/dag-pb/fixtures/cross-codec/#dagpb_data_some
        let data = Ipld::decode(
            DagPbCodec,
            &mut Cursor::new(hex::decode("0a050001020304").expect("should be valid hex data")),
        )
        .expect("should be valid dag-pb data");

        let cid =
            Cid::from_str("bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom").unwrap();
        let ipfs_path = IpfsPath::from_cid(cid);
        let mut mock_ipfs = MockIpfsDepTest::new();
        mock_ipfs.expect_clone().once().return_once(move || {
            let mut m = MockIpfsDepTest::new();
            m.expect_get()
                .once()
                .with(predicate::eq(ipfs_path))
                .return_once(move |_| Ok((cid, data)));
            m
        });
        let server = Server::new(mock_ipfs);
        let resp = server
            .dag_get_post(cid.to_string(), Some(Codecs::DagCbor), &Context)
            .await
            .unwrap();

        expect![[r#"
            Success(
                a26444617461450001020304654c696e6b7380,
            )
        "#]]
        .assert_debug_eq(&DebugResponse::from(resp));
    }

    #[tokio::test]
    #[traced_test]
    async fn dag_get_bad_request() {
        let mock_ipfs = MockIpfsDepTest::new();
        let server = Server::new(mock_ipfs);

        let resp = server
            .dag_get_post("bad cid".to_string(), None, &Context)
            .await
            .unwrap();

        expect![[r#"
            BadRequest(
                Error {
                    message: "invalid cid",
                    code: 0.0,
                    typ: "error",
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }

    #[tokio::test]
    #[traced_test]
    async fn dag_import() {
        let car_file = include_bytes!("testdata/carv1-basic.car");
        let mut mock_ipfs = MockIpfsDepTest::new();
        mock_ipfs.expect_clone().once().return_once(move || {
            let mut m = MockIpfsDepTest::new();

            fn expect_put(m: &mut MockIpfsDepTest, cid: &str) {
                m.expect_put()
                    .once()
                    .with(
                        predicate::eq(Cid::from_str(cid).unwrap()),
                        predicate::always(),
                        predicate::always(),
                    )
                    .return_once(|_, _, _| Ok(()));
            }

            for cid in [
                "bafyreihyrpefhacm6kkp4ql6j6udakdit7g3dmkzfriqfykhjw6cad5lrm",
                "QmNX6Tffavsya4xgBi2VJQnSuqy9GsxongxZZ9uZBqp16d",
                "bafkreifw7plhl6mofk6sfvhnfh64qmkq73oeqwl6sloru6rehaoujituke",
                "QmWXZxVQ9yZfhQxLD35eDR8LiMRsYtHxYqTFCBbJoiJVys",
                "bafkreiebzrnroamgos2adnbpgw5apo3z4iishhbdx77gldnbk57d4zdio4",
                "QmdwjhxpxzcMsR3qUuj7vUL8pbA7MgR3GAxWi2GLHjsKCT",
                "bafkreidbxzk2ryxwwtqxem4l3xyyjvw35yu4tcct4cqeqxwo47zhxgxqwq",
                "bafyreidj5idub6mapiupjwjsyyxhyhedxycv4vihfsicm2vt46o7morwlm",
            ] {
                expect_put(&mut m, cid)
            }
            m
        });
        let server = Server::new(mock_ipfs);
        let resp = server
            .dag_import_post(ByteArray(car_file.to_vec()), &Context)
            .await
            .unwrap();

        expect![[r#"
            Success(
                DagImportPost200Response {
                    root: DagPutPost200Response {
                        cid: DagPutPost200ResponseCid {
                            slash: "bafyreihyrpefhacm6kkp4ql6j6udakdit7g3dmkzfriqfykhjw6cad5lrm",
                        },
                    },
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }

    #[tokio::test]
    #[traced_test]
    async fn dag_put() {
        // Test data from:
        // https://ipld.io/specs/codecs/dag-json/fixtures/cross-codec/#array-mixed
        let cbor_cid =
            Cid::from_str("bafyreidufmzzejc3p7gmh6ivp4fjvca5jfazk57nu6vdkvki4c4vpja724").unwrap();

        let file = ByteArray(r#"[6433713753386423,65536,500,2,0,-1,-3,-256,-2784428724,-6433713753386424,{"/":{"bytes":"YTE"}},"Čaues ßvěte!"]"#.as_bytes().to_vec());
        // Cbor encoded bytes
        let blob = Bytes::from(hex::decode("8c1b0016db6db6db6db71a000100001901f40200202238ff3aa5f702b33b0016db6db6db6db74261316fc48c6175657320c39f76c49b746521").unwrap());
        let mut mock_ipfs = MockIpfsDepTest::new();
        mock_ipfs.expect_clone().once().return_once(move || {
            let mut m = MockIpfsDepTest::new();
            m.expect_put()
                .once()
                .with(
                    predicate::eq(cbor_cid),
                    predicate::eq(blob),
                    predicate::eq(vec![]),
                )
                .return_once(move |_, _, _| Ok(()));
            m
        });
        let server = Server::new(mock_ipfs);
        let resp = server
            .dag_put_post(file, None, None, &Context)
            .await
            .unwrap();

        expect![[r#"
            Success(
                DagPutPost200Response {
                    cid: DagPutPost200ResponseCid {
                        slash: "bafyreidufmzzejc3p7gmh6ivp4fjvca5jfazk57nu6vdkvki4c4vpja724",
                    },
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }
    #[tokio::test]
    #[traced_test]
    async fn dag_put_store_json() {
        // Test data from:
        // https://ipld.io/specs/codecs/dag-json/fixtures/cross-codec/#array-mixed
        let json_cid =
            Cid::from_str("baguqeera4iuxsgqusw3ctry362niptivjyio6dxnsn5afctijsahacub2eza").unwrap();

        let file = ByteArray(r#"[6433713753386423,65536,500,2,0,-1,-3,-256,-2784428724,-6433713753386424,{"/":{"bytes":"YTE"}},"Čaues ßvěte!"]"#.as_bytes().to_vec());
        // JSON encoded bytes
        let blob = Bytes::from(file.0.clone());
        let mut mock_ipfs = MockIpfsDepTest::new();
        mock_ipfs.expect_clone().once().return_once(move || {
            let mut m = MockIpfsDepTest::new();
            m.expect_put()
                .once()
                .with(
                    predicate::eq(json_cid),
                    predicate::eq(blob),
                    predicate::eq(vec![]),
                )
                .return_once(move |_, _, _| Ok(()));
            m
        });
        let server = Server::new(mock_ipfs);
        let resp = server
            .dag_put_post(file, Some(Codecs::DagJson), None, &Context)
            .await
            .unwrap();

        expect![[r#"
            Success(
                DagPutPost200Response {
                    cid: DagPutPost200ResponseCid {
                        slash: "baguqeera4iuxsgqusw3ctry362niptivjyio6dxnsn5afctijsahacub2eza",
                    },
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }

    #[tokio::test]
    #[traced_test]
    async fn dag_put_bad_request() {
        let mock_ipfs = MockIpfsDepTest::new();
        let server = Server::new(mock_ipfs);

        let resp = server
            .dag_put_post(ByteArray(vec![]), Some(Codecs::Raw), None, &Context)
            .await
            .unwrap();

        expect![[r#"
            BadRequest(
                Error {
                    message: "unsupported codec combination, input-codec: dag-json, store-codec: raw",
                    code: 0.0,
                    typ: "error",
                },
            )
        "#]]
        .assert_debug_eq(&resp);

        let resp = server
            .dag_put_post(ByteArray(vec![]), None, Some(Codecs::Raw), &Context)
            .await
            .unwrap();

        expect![[r#"
            BadRequest(
                Error {
                    message: "unsupported codec combination, input-codec: raw, store-codec: dag-cbor",
                    code: 0.0,
                    typ: "error",
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }
    #[tokio::test]
    #[traced_test]
    async fn dag_resolve() {
        // Test data uses getting started guide for IPFS:
        // ipfs://QmQPeNsJPyVWPFDVHb77w8G42Fvo15z4bG2X8D2GhfbSXc
        let path = "QmQPeNsJPyVWPFDVHb77w8G42Fvo15z4bG2X8D2GhfbSXc/ping";

        let cid = Cid::from_str("QmejvEPop4D7YUadeGqYWmZxHhLc4JBUCzJJHWMzdcMe2y").unwrap();
        let mut mock_ipfs = MockIpfsDepTest::new();
        mock_ipfs.expect_clone().once().return_once(move || {
            let mut m = MockIpfsDepTest::new();
            m.expect_resolve()
                .once()
                .with(predicate::eq(IpfsPath::from_str(path).unwrap()))
                .return_once(move |_| Ok((cid, "".to_string())));
            m
        });
        let server = Server::new(mock_ipfs);
        let resp = server
            .dag_resolve_post(path.to_string(), &Context)
            .await
            .unwrap();

        expect![[r#"
            Success(
                DagResolvePost200Response {
                    cid: DagResolvePost200ResponseCid {
                        slash: "QmejvEPop4D7YUadeGqYWmZxHhLc4JBUCzJJHWMzdcMe2y",
                    },
                    rem_path: "",
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }

    #[tokio::test]
    #[traced_test]
    async fn dag_resolve_remaining() {
        let path = "bafyreih6aqnl3v2d6jlidqqnw6skf2ntrtswvra65xz73ymrqspdy2jfai/chainId";

        let cid =
            Cid::from_str("bafyreih6aqnl3v2d6jlidqqnw6skf2ntrtswvra65xz73ymrqspdy2jfai").unwrap();
        let mut mock_ipfs = MockIpfsDepTest::new();
        mock_ipfs.expect_clone().once().return_once(move || {
            let mut m = MockIpfsDepTest::new();
            m.expect_resolve()
                .once()
                .with(predicate::eq(IpfsPath::from_str(path).unwrap()))
                .return_once(move |_| Ok((cid, "chainId".to_string())));
            m
        });
        let server = Server::new(mock_ipfs);
        let resp = server
            .dag_resolve_post(path.to_string(), &Context)
            .await
            .unwrap();

        expect![[r#"
            Success(
                DagResolvePost200Response {
                    cid: DagResolvePost200ResponseCid {
                        slash: "bafyreih6aqnl3v2d6jlidqqnw6skf2ntrtswvra65xz73ymrqspdy2jfai",
                    },
                    rem_path: "chainId",
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }
    #[tokio::test]
    #[traced_test]
    async fn dag_resolve_bad_request() {
        let mock_ipfs = MockIpfsDepTest::new();
        let server = Server::new(mock_ipfs);

        let resp = server
            .dag_resolve_post("bad cid".to_string(), &Context)
            .await
            .unwrap();

        expect![[r#"
            BadRequest(
                Error {
                    message: "invalid cid",
                    code: 0.0,
                    typ: "error",
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }
    #[tokio::test]
    #[traced_test]
    async fn id_local() {
        let info = PeerInfo {
            peer_id: "12D3KooWQuKj4A11GNZ4MmcAmJzCNGZjArjyRTgkLhSutqeqVypv"
                .parse()
                .unwrap(),
            protocol_version: "ipfs/0.1.0".to_string(),
            agent_version: "iroh/0.2.0".to_string(),
            listen_addrs: vec![
                "/ip4/127.0.0.1/udp/35826/quic-v1".parse().unwrap(),
                "/ip4/192.168.12.189/tcp/43113".parse().unwrap(),
            ],
            protocols: vec![
                "/ipfs/ping/1.0.0".parse().unwrap(),
                "/ipfs/id/1.0.0".parse().unwrap(),
                "/ipfs/id/push/1.0.0".parse().unwrap(),
                "/ipfs/bitswap/1.2.0".parse().unwrap(),
                "/ipfs/bitswap/1.1.0".parse().unwrap(),
                "/ipfs/bitswap/1.0.0".parse().unwrap(),
                "/ipfs/bitswap".parse().unwrap(),
                "/ipfs/kad/1.0.0".parse().unwrap(),
                "/libp2p/autonat/1.0.0".parse().unwrap(),
                "/libp2p/circuit/relay/0.2.0/hop".parse().unwrap(),
                "/libp2p/circuit/relay/0.2.0/stop".parse().unwrap(),
                "/libp2p/dcutr".parse().unwrap(),
                "/meshsub/1.1.0".parse().unwrap(),
                "/meshsub/1.0.0".parse().unwrap(),
            ],
        };
        let mut mock_ipfs = MockIpfsDepTest::new();
        mock_ipfs.expect_clone().once().return_once(move || {
            let mut m = MockIpfsDepTest::new();
            m.expect_lookup_local().once().return_once(move || Ok(info));
            m
        });
        let server = Server::new(mock_ipfs);
        let resp = server.id_post(None, &Context).await.unwrap();

        expect![[r#"
            Success(
                IdPost200Response {
                    id: "12D3KooWQuKj4A11GNZ4MmcAmJzCNGZjArjyRTgkLhSutqeqVypv",
                    addresses: [
                        "/ip4/127.0.0.1/udp/35826/quic-v1",
                        "/ip4/192.168.12.189/tcp/43113",
                    ],
                    agent_version: "iroh/0.2.0",
                    protocol_version: "ipfs/0.1.0",
                    protocols: [
                        "/ipfs/ping/1.0.0",
                        "/ipfs/id/1.0.0",
                        "/ipfs/id/push/1.0.0",
                        "/ipfs/bitswap/1.2.0",
                        "/ipfs/bitswap/1.1.0",
                        "/ipfs/bitswap/1.0.0",
                        "/ipfs/bitswap",
                        "/ipfs/kad/1.0.0",
                        "/libp2p/autonat/1.0.0",
                        "/libp2p/circuit/relay/0.2.0/hop",
                        "/libp2p/circuit/relay/0.2.0/stop",
                        "/libp2p/dcutr",
                        "/meshsub/1.1.0",
                        "/meshsub/1.0.0",
                    ],
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }
    #[tokio::test]
    #[traced_test]
    async fn id_remote() {
        let info = PeerInfo {
            peer_id: "12D3KooWQuKj4A11GNZ4MmcAmJzCNGZjArjyRTgkLhSutqeqVypv"
                .parse()
                .unwrap(),
            protocol_version: "ipfs/0.1.0".to_string(),
            agent_version: "iroh/0.2.0".to_string(),
            listen_addrs: vec![
                "/ip4/127.0.0.1/udp/35826/quic-v1".parse().unwrap(),
                "/ip4/192.168.12.189/tcp/43113".parse().unwrap(),
            ],
            protocols: vec![
                "/ipfs/ping/1.0.0".parse().unwrap(),
                "/ipfs/id/1.0.0".parse().unwrap(),
                "/ipfs/id/push/1.0.0".parse().unwrap(),
                "/ipfs/bitswap/1.2.0".parse().unwrap(),
                "/ipfs/bitswap/1.1.0".parse().unwrap(),
                "/ipfs/bitswap/1.0.0".parse().unwrap(),
                "/ipfs/bitswap".parse().unwrap(),
                "/ipfs/kad/1.0.0".parse().unwrap(),
                "/libp2p/autonat/1.0.0".parse().unwrap(),
                "/libp2p/circuit/relay/0.2.0/hop".parse().unwrap(),
                "/libp2p/circuit/relay/0.2.0/stop".parse().unwrap(),
                "/libp2p/dcutr".parse().unwrap(),
                "/meshsub/1.1.0".parse().unwrap(),
                "/meshsub/1.0.0".parse().unwrap(),
            ],
        };
        let peer_id = info.peer_id.to_string();
        let mut mock_ipfs = MockIpfsDepTest::new();
        mock_ipfs.expect_clone().once().return_once(move || {
            let mut m = MockIpfsDepTest::new();
            m.expect_lookup()
                .once()
                .with(predicate::eq(info.peer_id))
                .return_once(move |_| Ok(info));
            m
        });
        let server = Server::new(mock_ipfs);
        let resp = server.id_post(Some(peer_id), &Context).await.unwrap();

        expect![[r#"
            Success(
                IdPost200Response {
                    id: "12D3KooWQuKj4A11GNZ4MmcAmJzCNGZjArjyRTgkLhSutqeqVypv",
                    addresses: [
                        "/ip4/127.0.0.1/udp/35826/quic-v1",
                        "/ip4/192.168.12.189/tcp/43113",
                    ],
                    agent_version: "iroh/0.2.0",
                    protocol_version: "ipfs/0.1.0",
                    protocols: [
                        "/ipfs/ping/1.0.0",
                        "/ipfs/id/1.0.0",
                        "/ipfs/id/push/1.0.0",
                        "/ipfs/bitswap/1.2.0",
                        "/ipfs/bitswap/1.1.0",
                        "/ipfs/bitswap/1.0.0",
                        "/ipfs/bitswap",
                        "/ipfs/kad/1.0.0",
                        "/libp2p/autonat/1.0.0",
                        "/libp2p/circuit/relay/0.2.0/hop",
                        "/libp2p/circuit/relay/0.2.0/stop",
                        "/libp2p/dcutr",
                        "/meshsub/1.1.0",
                        "/meshsub/1.0.0",
                    ],
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }
    #[tokio::test]
    #[traced_test]
    async fn id_bad_request() {
        let mock_ipfs = MockIpfsDepTest::new();
        let server = Server::new(mock_ipfs);

        let resp = server
            .id_post(Some("invalid peer id".to_string()), &Context)
            .await
            .unwrap();

        expect![[r#"
            BadRequest(
                Error {
                    message: "base-58 decode error: provided string contained invalid character 'l' at byte 4",
                    code: 0.0,
                    typ: "error",
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }

    #[tokio::test]
    #[traced_test]
    async fn pin_add() {
        // Test data from:
        // https://ipld.io/specs/codecs/dag-pb/fixtures/cross-codec/#dagpb_data_some
        let data = Ipld::decode(
            DagPbCodec,
            &mut Cursor::new(hex::decode("0a050001020304").expect("should be valid hex data")),
        )
        .expect("should be valid dag-pb data");

        let cid =
            Cid::from_str("bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom").unwrap();
        let ipfs_path = IpfsPath::from_cid(cid);
        let mut mock_ipfs = MockIpfsDepTest::new();
        mock_ipfs.expect_clone().once().return_once(move || {
            let mut m = MockIpfsDepTest::new();
            m.expect_get()
                .once()
                .with(predicate::eq(ipfs_path))
                .return_once(move |_| Ok((cid, data)));
            m
        });
        let server = Server::new(mock_ipfs);
        let resp = server
            .pin_add_post(cid.to_string(), None, None, &Context)
            .await
            .unwrap();

        expect![[r#"
            Success(
                PinAddPost200Response {
                    pins: [
                        "bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom",
                    ],
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }
    #[tokio::test]
    #[traced_test]
    async fn pin_add_bad_request() {
        let cid =
            Cid::from_str("bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom").unwrap();
        let mock_ipfs = MockIpfsDepTest::new();
        let server = Server::new(mock_ipfs);

        let resp = server
            .pin_add_post("bad cid".to_string(), None, None, &Context)
            .await
            .unwrap();

        expect![[r#"
            BadRequest(
                Error {
                    message: "invalid cid",
                    code: 0.0,
                    typ: "error",
                },
            )
        "#]]
        .assert_debug_eq(&resp);

        let resp = server
            .pin_add_post(cid.to_string(), Some(true), None, &Context)
            .await
            .unwrap();

        expect![[r#"
            BadRequest(
                Error {
                    message: "recursive pinning is not supported",
                    code: 0.0,
                    typ: "error",
                },
            )
        "#]]
        .assert_debug_eq(&resp);

        let resp = server
            .pin_add_post(cid.to_string(), None, Some(true), &Context)
            .await
            .unwrap();

        expect![[r#"
            BadRequest(
                Error {
                    message: "pin progress is not supported",
                    code: 0.0,
                    typ: "error",
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }
    #[tokio::test]
    #[traced_test]
    async fn pin_rm() {
        let cid =
            Cid::from_str("bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom").unwrap();
        let mut mock_ipfs = MockIpfsDepTest::new();
        mock_ipfs
            .expect_clone()
            .once()
            .return_once(MockIpfsDepTest::new);
        let server = Server::new(mock_ipfs);
        let resp = server.pin_rm_post(cid.to_string(), &Context).await.unwrap();

        expect![[r#"
            Success(
                PinAddPost200Response {
                    pins: [
                        "bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom",
                    ],
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }

    #[tokio::test]
    #[traced_test]
    async fn pin_rm_bad_request() {
        let mock_ipfs = MockIpfsDepTest::new();
        let server = Server::new(mock_ipfs);
        let resp = server
            .pin_rm_post("invalid cid".to_string(), &Context)
            .await
            .unwrap();

        expect![[r#"
            BadRequest(
                Error {
                    message: "invalid cid",
                    code: 0.0,
                    typ: "error",
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }
    #[tokio::test]
    #[traced_test]
    async fn swarm_peers() {
        let mut mock_ipfs = MockIpfsDepTest::new();
        mock_ipfs.expect_clone().once().return_once(|| {
            let mut m = MockIpfsDepTest::new();
            m.expect_peers().once().return_once(|| {
                Ok(HashMap::from([
                    (
                        PeerId::from_str("12D3KooWRyGSRzzEBpHbHyRkGTgCpXuoRMQgYrqk7tFQzM3AFEWp")
                            .unwrap(),
                        vec![Multiaddr::from_str("/ip4/98.165.227.74/udp/15685/quic").unwrap()],
                    ),
                    (
                        PeerId::from_str("12D3KooWBSyp3QZQBFakvXT2uqT2L5ZmTNnpYNXgyVZq5YB3P7DU")
                            .unwrap(),
                        vec![Multiaddr::from_str("/ip4/95.211.198.178/udp/4001/quic").unwrap()],
                    ),
                ]))
            });
            m
        });
        let server = Server::new(mock_ipfs);
        let resp = server.swarm_peers_post(&Context).await.unwrap();

        expect![[r#"
            Success(
                SwarmPeersPost200Response {
                    peers: [
                        SwarmPeersPost200ResponsePeersInner {
                            addr: "/ip4/95.211.198.178/udp/4001/quic",
                            peer: "12D3KooWBSyp3QZQBFakvXT2uqT2L5ZmTNnpYNXgyVZq5YB3P7DU",
                        },
                        SwarmPeersPost200ResponsePeersInner {
                            addr: "/ip4/98.165.227.74/udp/15685/quic",
                            peer: "12D3KooWRyGSRzzEBpHbHyRkGTgCpXuoRMQgYrqk7tFQzM3AFEWp",
                        },
                    ],
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }
    #[tokio::test]
    #[traced_test]
    async fn pubsub_ls() {
        let mut mock_ipfs = MockIpfsDepTest::new();
        mock_ipfs.expect_clone().once().return_once(|| {
            let mut m = MockIpfsDepTest::new();
            m.expect_topics()
                .once()
                .return_once(|| Ok(vec!["topic1".to_string(), "topic2".to_string()]));
            m
        });
        let server = Server::new(mock_ipfs);
        let resp = server.pubsub_ls_post(&Context).await.unwrap();

        expect![[r#"
            Success(
                PubsubLsPost200Response {
                    strings: [
                        "topic1",
                        "topic2",
                    ],
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }

    #[tokio::test]
    #[traced_test]
    async fn pubsub_pub() {
        let topic = "test-topic".to_string();
        let topic_encoded = multibase::encode(Base::Base64, topic.as_bytes());
        let message = b"message bytes";
        let message_bytes = Bytes::from(&message[..]);

        let mut mock_ipfs = MockIpfsDepTest::new();
        mock_ipfs.expect_clone().once().return_once(move || {
            let mut m = MockIpfsDepTest::new();
            m.expect_publish()
                .once()
                .with(predicate::eq(topic), predicate::eq(message_bytes))
                .return_once(|_, _| Ok(()));
            m
        });
        let server = Server::new(mock_ipfs);
        let resp = server
            .pubsub_pub_post(topic_encoded, ByteArray(message.to_vec()), &Context)
            .await
            .unwrap();

        expect![[r#"
            Success
        "#]]
        .assert_debug_eq(&resp);
    }
    #[tokio::test]
    #[traced_test]
    async fn pubsub_sub() {
        let topic = "test-topic".to_string();
        let topic_encoded = multibase::encode(Base::Base64, topic.as_bytes());

        let mut mock_ipfs = MockIpfsDepTest::new();
        mock_ipfs.expect_clone().once().return_once(move || {
            let mut m = MockIpfsDepTest::new();
            m.expect_subscribe()
                .once()
                .with(predicate::eq(topic))
                .return_once(|_| {
                    let first = Ok(GossipsubEvent::Message {
                        from: PeerId::from_str(
                            "12D3KooWHUfjwiTRVV8jxFcKRSQTPatayC4nQCNX86oxRF5XWzGe",
                        )
                        .unwrap(),
                        id: libp2p::gossipsub::MessageId::new(&[]),
                        message: Message {
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
                        from: PeerId::from_str(
                            "12D3KooWGnKwtpSh2ZLTvoC8mjiexMNRLNkT92pxq7MDgyJHktNJ",
                        )
                        .unwrap(),
                        id: libp2p::gossipsub::MessageId::new(&[]),
                        message: Message {
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

                    Ok(Box::pin(stream! {
                        yield first;
                        yield second;
                    }))
                });
            m
        });
        let server = Server::new(mock_ipfs);
        let resp = server
            .pubsub_sub_post(topic_encoded, &Context)
            .await
            .unwrap();

        if let PubsubSubPostResponse::Success(body) = resp {
            let messages: Result<Vec<Bytes>, _> = body.try_collect().await;
            let messages: Vec<UnquotedString> = messages
                .unwrap()
                .into_iter()
                .map(|m| UnquotedString(bytes_to_pretty_str(m.to_vec())))
                .collect();
            expect![[r#"
                [
                    {
                      "data": "ubWVzc2FnZSAx",
                      "from": "12D3KooWM68GyFKBT9JsuTRB6CYkF61PtMuSkynUauSQEGBX51JW",
                      "seqno": "uAAAAAAAAAAA",
                      "topicIDs": [
                        "udG9waWNB"
                      ]
                    },
                    {
                      "data": "ubWVzc2FnZSAy",
                      "from": "12D3KooWQVU9Pv3BqD6bD9w96tJxLedKCj4VZ75oqX9Tav4R4rUS",
                      "seqno": "uAQAAAAAAAAA",
                      "topicIDs": [
                        "udG9waWNB"
                      ]
                    },
                ]
            "#]]
            .assert_debug_eq(&messages);
        } else {
            panic!("did not get success from server");
        }
    }
    #[tokio::test]
    #[traced_test]
    async fn swarm_connect() {
        let mut mock_ipfs = MockIpfsDepTest::new();
        mock_ipfs.expect_clone().once().return_once(|| {
            let mut m = MockIpfsDepTest::new();
            m.expect_connect()
                .once()
                .with(
                    predicate::eq(
                        PeerId::from_str("12D3KooWFtPWZ1uHShnbvmxYJGmygUfTVmcb6iSQfiAm4XnmsQ8t")
                            .unwrap(),
                    ),
                    predicate::always(),
                )
                .return_once(|_, _| Ok(()));
            m
        });
        let server = Server::new(mock_ipfs);
        let resp = server.swarm_connect_post(&vec!["/ip4/1.1.1.1/tcp/4001/p2p/12D3KooWFtPWZ1uHShnbvmxYJGmygUfTVmcb6iSQfiAm4XnmsQ8t".to_string()], &Context).await.unwrap();

        expect![[r#"
            Success(
                PubsubLsPost200Response {
                    strings: [
                        "connect 12D3KooWFtPWZ1uHShnbvmxYJGmygUfTVmcb6iSQfiAm4XnmsQ8t success",
                    ],
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }
    #[tokio::test]
    #[traced_test]
    async fn swarm_connect_bad_request() {
        let mock_ipfs = MockIpfsDepTest::new();
        let server = Server::new(mock_ipfs);
        let resp = server
            .swarm_connect_post(&vec!["/ip4/1.1.1.1/tcp/4001".to_string()], &Context)
            .await
            .unwrap();

        expect![[r#"
            BadRequest(
                Error {
                    message: "no peer id specificed in multiaddrs",
                    code: 0.0,
                    typ: "error",
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }
    #[tokio::test]
    #[traced_test]
    async fn version() {
        let mut mock_ipfs = MockIpfsDepTest::new();
        mock_ipfs.expect_clone().once().return_once(|| {
            let mut m = MockIpfsDepTest::new();
            m.expect_version().once().return_once(|| {
                Ok(Version {
                    version: "version".to_string(),
                    arch: "arch".to_string(),
                    os: "os".to_string(),
                    commit: "commit".to_string(),
                })
            });
            m
        });
        let server = Server::new(mock_ipfs);
        let resp = server.version_post(&Context).await.unwrap();

        expect![[r#"
            Success(
                VersionPost200Response {
                    commit: "commit",
                    system: "arch/os",
                    version: "version",
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }
}
