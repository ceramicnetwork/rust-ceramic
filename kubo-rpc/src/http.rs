//! Provides an http implementation of the Kubo RPC methods.

// See https://github.com/tokio-rs/tracing/pull/2880
#![allow(clippy::blocks_in_conditions)]

use std::{collections::HashSet, marker::PhantomData, str::FromStr, time::Duration};

use anyhow::anyhow;
use async_trait::async_trait;
use ceramic_kubo_rpc_server::{
    models::{
        self, BlockStatPost200Response, Codecs, DagResolvePost200Response,
        DagResolvePost200ResponseCid, IdPost200Response, PinAddPost200Response,
        SwarmConnectPost200Response, SwarmPeersPost200Response,
        SwarmPeersPost200ResponsePeersInner, VersionPost200Response,
    },
    Api, BlockGetPostResponse, BlockStatPostResponse, DagGetPostResponse, DagResolvePostResponse,
    IdPostResponse, PinAddPostResponse, PinRmPostResponse, SwarmConnectPostResponse,
    SwarmPeersPostResponse, VersionPostResponse,
};
use cid::Cid;
use go_parse_duration::parse_duration;
use libp2p::{Multiaddr, PeerId};
use multiaddr::Protocol;
use serde_ipld_dagcbor::codec::DagCborCodec;
use serde_ipld_dagjson::codec::DagJsonCodec;
use swagger::{ApiError, ByteArray};
use tracing::{instrument, Level};

use crate::{block, dag, id, pin, swarm, version, IpfsDep, IpfsPath};

/// Kubo RPC API Server implementation.
#[derive(Clone)]
pub struct Server<I, C> {
    ipfs: I,
    marker: PhantomData<C>,
}
impl<I, C> Server<I, C>
where
    I: IpfsDep + Send + Sync + 'static,
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
        r#type: "error".to_string(),
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
    I: IpfsDep + Send + Sync + 'static,
    C: Send + Sync,
{
    #[instrument(skip(self, _context), ret(level = Level::DEBUG), err(level = Level::ERROR))]
    async fn block_get_post(
        &self,
        arg: String,
        timeout: Option<String>,
        offline: Option<bool>,
        _context: &C,
    ) -> Result<BlockGetPostResponse, ApiError> {
        // We use these hard-coded error strings to be compatible with Go/Kubo.
        // This Go/Kubo compatibility makes it possible to better emulate the Kubo RPC API.
        // Eventually when we deprecate the use of the Kubo RPC API we can remove
        // this logic entirely.
        const BLOCK_NOT_FOUND_LOCALLY: &str = "block was not found locally (offline)";
        const CONTEXT_DEADLINE_EXCEEDED: &str = "context deadline exceeded";

        // Online is the default so we expect that offline=true is explicitly passed.
        if !offline.unwrap_or(false) {
            return Ok(BlockGetPostResponse::BadRequest(create_error(
                "only offline mode is supported",
            )));
        }

        let cid = try_or_bad_request!(Cid::from_str(&arg), BlockGetPostResponse);
        let data_fut = block::get(self.ipfs.clone(), cid);
        let data = if let Some(timeout) = timeout {
            let timeout = try_or_bad_request!(
                parse_duration(&timeout).map_err(|err| match err {
                    go_parse_duration::Error::ParseError(msg) =>
                        anyhow!("invalid timeout duration string: {}", msg),
                }),
                BlockGetPostResponse
            );
            let timeout = Duration::from_nanos(timeout as u64);
            match tokio::time::timeout(timeout, data_fut).await {
                Ok(Err(crate::error::Error::NotFound)) => {
                    return Ok(BlockGetPostResponse::InternalError(create_error(
                        BLOCK_NOT_FOUND_LOCALLY,
                    )));
                }
                Ok(res) => res.map_err(to_api_error)?,
                Err(_err) => {
                    return Ok(BlockGetPostResponse::InternalError(create_error(
                        CONTEXT_DEADLINE_EXCEEDED,
                    )));
                }
            }
        } else {
            match data_fut.await {
                Err(crate::error::Error::NotFound) => {
                    return Ok(BlockGetPostResponse::InternalError(create_error(
                        BLOCK_NOT_FOUND_LOCALLY,
                    )));
                }
                res => res.map_err(to_api_error)?,
            }
        };
        Ok(BlockGetPostResponse::Success(ByteArray(data)))
    }

    #[instrument(skip(self, _context), ret(level = Level::DEBUG), err(level = Level::ERROR))]
    async fn block_stat_post(
        &self,
        arg: String,
        _context: &C,
    ) -> Result<BlockStatPostResponse, ApiError> {
        let cid = try_or_bad_request!(Cid::from_str(&arg), BlockStatPostResponse);
        let size = block::stat(self.ipfs.clone(), cid)
            .await
            .map_err(to_api_error)?;
        Ok(BlockStatPostResponse::Success(BlockStatPost200Response {
            key: cid.to_string(),
            size: size as f64,
        }))
    }

    #[instrument(skip(self, _context), ret(level = Level::DEBUG), err(level = Level::ERROR))]
    async fn dag_get_post(
        &self,
        arg: String,
        output_codec: Option<Codecs>,
        _context: &C,
    ) -> Result<DagGetPostResponse, ApiError> {
        let ipfs_path = try_or_bad_request!(IpfsPath::from_str(&arg), DagGetPostResponse);
        match output_codec.unwrap_or(Codecs::DagJson) {
            Codecs::DagJson => Ok(DagGetPostResponse::Success(ByteArray(
                dag::get::<_, DagJsonCodec>(self.ipfs.clone(), &ipfs_path)
                    .await
                    .map_err(to_api_error)?,
            ))),
            Codecs::DagCbor => Ok(DagGetPostResponse::Success(ByteArray(
                dag::get::<_, DagCborCodec>(self.ipfs.clone(), &ipfs_path)
                    .await
                    .map_err(to_api_error)?,
            ))),
            Codecs::DagJose | Codecs::Raw => Ok(DagGetPostResponse::BadRequest(create_error(
                &format!("unsupported output codec: {output_codec:?}"),
            ))),
        }
    }

    #[instrument(skip(self, _context), ret(level = Level::DEBUG), err(level = Level::ERROR))]
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

    #[instrument(skip(self, _context), ret(level = Level::DEBUG), err(level = Level::ERROR))]
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

    #[instrument(skip(self, _context), ret(level = Level::DEBUG), err(level = Level::ERROR))]
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
    #[instrument(skip(self, _context), ret(level = Level::DEBUG), err(level = Level::ERROR))]
    async fn pin_rm_post(&self, arg: String, _context: &C) -> Result<PinRmPostResponse, ApiError> {
        let ipfs_path = try_or_bad_request!(IpfsPath::from_str(&arg), PinRmPostResponse);
        let cid = pin::remove(self.ipfs.clone(), &ipfs_path)
            .await
            .map_err(to_api_error)?;
        Ok(PinRmPostResponse::Success(PinAddPost200Response {
            pins: vec![cid.to_string()],
        }))
    }

    #[instrument(skip(self, _context), ret(level = Level::DEBUG), err(level = Level::ERROR))]
    async fn swarm_connect_post(
        &self,
        arg: &Vec<String>,
        _context: &C,
    ) -> Result<SwarmConnectPostResponse, ApiError> {
        // Iterate over each arg and parse it as a multiaddr and search for peer ids.
        let iter = arg.iter().map(
            |addr| -> Result<(Multiaddr, Option<PeerId>), anyhow::Error> {
                let addr = Multiaddr::from_str(addr)?;
                let peer_id = addr.iter().find_map(|proto| match proto {
                    Protocol::P2p(peer_id) => Some(peer_id),
                    _ => None,
                });
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
        Ok(SwarmConnectPostResponse::Success(
            SwarmConnectPost200Response {
                strings: vec![format!("connect {} success", peer_id)],
            },
        ))
    }

    #[instrument(skip(self, _context), ret(level = Level::DEBUG), err(level = Level::ERROR))]
    async fn swarm_peers_post(&self, _context: &C) -> Result<SwarmPeersPostResponse, ApiError> {
        let peers: Vec<SwarmPeersPost200ResponsePeersInner> = swarm::peers(self.ipfs.clone())
            .await
            .map_err(to_api_error)?
            .into_iter()
            .map(|(k, v)| SwarmPeersPost200ResponsePeersInner {
                addr: v
                    .first()
                    .map(|a| a.to_string())
                    .unwrap_or_else(|| "".to_string()),
                peer: k.to_string(),
            })
            .collect();
        Ok(SwarmPeersPostResponse::Success(SwarmPeersPost200Response {
            peers,
        }))
    }

    #[instrument(skip(self, _context), ret(level = Level::DEBUG), err(level = Level::ERROR))]
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

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use super::*;
    use crate::{tests::MockIpfsDepTest, PeerInfo};

    use bytes::Bytes;
    use ceramic_metadata::Version;
    use ipld_core::codec::Codec;
    use ipld_dagpb::DagPbCodec;
    use mockall::predicate;
    use test_pretty_log::test;

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
    #[allow(dead_code)]
    enum DebugResponse {
        Success(UnquotedString),
        BadRequest(models::Error),
        InternalError(models::Error),
    }

    impl From<BlockGetPostResponse> for DebugResponse {
        fn from(value: BlockGetPostResponse) -> Self {
            match value {
                BlockGetPostResponse::Success(data) => {
                    DebugResponse::Success(UnquotedString(hex::encode(data.0)))
                }
                BlockGetPostResponse::BadRequest(err) => DebugResponse::BadRequest(err),
                BlockGetPostResponse::InternalError(err) => DebugResponse::InternalError(err),
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

    #[test(tokio::test)]
    async fn block_get() {
        let cid =
            Cid::from_str("bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom").unwrap();
        let mock_ipfs = MockIpfsDepTest::new();
        let server = Server::new(mock_ipfs);
        let resp = server
            .block_get_post(cid.to_string(), None, None, &Context)
            .await
            .unwrap();

        expect![[r#"
            BadRequest(
                Error {
                    message: "only offline mode is supported",
                    code: 0.0,
                    type: "error",
                },
            )
        "#]]
        .assert_debug_eq(&DebugResponse::from(resp));
    }

    #[test(tokio::test)]
    async fn block_get_offline() {
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
            .block_get_post(cid.to_string(), None, Some(true), &Context)
            .await
            .unwrap();

        expect![[r#"
            Success(
                0a050001020304,
            )
        "#]]
        .assert_debug_eq(&DebugResponse::from(resp));
    }
    #[test(tokio::test)]
    async fn block_get_offline_not_found() {
        let cid =
            Cid::from_str("bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom").unwrap();
        let mut mock_ipfs = MockIpfsDepTest::new();
        mock_ipfs.expect_clone().once().return_once(move || {
            let mut m = MockIpfsDepTest::new();
            m.expect_block_get()
                .once()
                .with(predicate::eq(cid))
                .return_once(move |_| Err(crate::error::Error::NotFound));
            m
        });
        let server = Server::new(mock_ipfs);
        let resp = server
            .block_get_post(cid.to_string(), None, Some(true), &Context)
            .await
            .unwrap();

        expect![[r#"
            InternalError(
                Error {
                    message: "block was not found locally (offline)",
                    code: 0.0,
                    type: "error",
                },
            )
        "#]]
        .assert_debug_eq(&DebugResponse::from(resp));
    }

    #[test(tokio::test)]
    async fn block_get_timeout_success() {
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
            .block_get_post(
                cid.to_string(),
                Some("1s".to_string()),
                Some(true),
                &Context,
            )
            .await
            .unwrap();

        // We test the non-timeout case as mockall doesn't allow us to delay returning values.
        expect![[r#"
            Success(
                0a050001020304,
            )
        "#]]
        .assert_debug_eq(&DebugResponse::from(resp));
    }

    #[test(tokio::test)]
    async fn block_get_bad_request() {
        let mock_ipfs = MockIpfsDepTest::new();
        let server = Server::new(mock_ipfs);

        let resp = server
            .block_get_post("invalid cid".to_string(), None, Some(true), &Context)
            .await
            .unwrap();

        expect![[r#"
            BadRequest(
                Error {
                    message: "Failed to parse multihash",
                    code: 0.0,
                    type: "error",
                },
            )
        "#]]
        .assert_debug_eq(&DebugResponse::from(resp));
    }

    #[test(tokio::test)]
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
                BlockStatPost200Response {
                    key: "bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom",
                    size: 7.0,
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }

    #[test(tokio::test)]
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
                    type: "error",
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }

    #[test(tokio::test)]
    async fn dag_get_json() {
        // Test data from:
        // https://ipld.io/specs/codecs/dag-pb/fixtures/cross-codec/#dagpb_data_some
        let data = DagPbCodec::decode_from_slice(
            &hex::decode("0a050001020304").expect("should be valid hex data"),
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
    #[test(tokio::test)]
    async fn dag_get_cbor() {
        // Test data from:
        // https://ipld.io/specs/codecs/dag-pb/fixtures/cross-codec/#dagpb_data_some
        let data = DagPbCodec::decode_from_slice(
            &hex::decode("0a050001020304").expect("should be valid hex data"),
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

    #[test(tokio::test)]
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
                    type: "error",
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }

    #[test(tokio::test)]
    async fn dag_resolve() {
        // Test data uses getting started guide for IPFS:
        // ipfs://QmQPeNsJPyVWPFDVHb77w8G42Fvo15z4bG2X8D2GhfbSXc // cspell:disable-line
        let path = "QmQPeNsJPyVWPFDVHb77w8G42Fvo15z4bG2X8D2GhfbSXc/ping"; // cspell:disable-line

        let cid = Cid::from_str("QmejvEPop4D7YUadeGqYWmZxHhLc4JBUCzJJHWMzdcMe2y").unwrap(); // cspell:disable-line
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

    #[test(tokio::test)]
    async fn dag_resolve_remaining() {
        let path = "bafyreih6aqnl3v2d6jlidqqnw6skf2ntrtswvra65xz73ymrqspdy2jfai/chainId"; // cspell:disable-line

        let cid =
            Cid::from_str("bafyreih6aqnl3v2d6jlidqqnw6skf2ntrtswvra65xz73ymrqspdy2jfai").unwrap(); // cspell:disable-line
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
    #[test(tokio::test)]
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
                    type: "error",
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }
    #[test(tokio::test)]
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
                        "/ip4/127.0.0.1/udp/35826/quic-v1/p2p/12D3KooWQuKj4A11GNZ4MmcAmJzCNGZjArjyRTgkLhSutqeqVypv",
                        "/ip4/192.168.12.189/tcp/43113/p2p/12D3KooWQuKj4A11GNZ4MmcAmJzCNGZjArjyRTgkLhSutqeqVypv",
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
    #[test(tokio::test)]
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
                        "/ip4/127.0.0.1/udp/35826/quic-v1/p2p/12D3KooWQuKj4A11GNZ4MmcAmJzCNGZjArjyRTgkLhSutqeqVypv",
                        "/ip4/192.168.12.189/tcp/43113/p2p/12D3KooWQuKj4A11GNZ4MmcAmJzCNGZjArjyRTgkLhSutqeqVypv",
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
    #[test(tokio::test)]
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
                    type: "error",
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }

    #[test(tokio::test)]
    async fn pin_add() {
        // Test data from:
        // https://ipld.io/specs/codecs/dag-pb/fixtures/cross-codec/#dagpb_data_some
        let data = DagPbCodec::decode_from_slice(
            &hex::decode("0a050001020304").expect("should be valid hex data"),
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
    #[test(tokio::test)]
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
                    type: "error",
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
                    type: "error",
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
                    type: "error",
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }
    #[test(tokio::test)]
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

    #[test(tokio::test)]
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
                    type: "error",
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }
    #[test(tokio::test)]
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
                        vec![Multiaddr::from_str("/ip4/95.211.198.178/udp/4101/quic").unwrap()],
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
                            addr: "/ip4/95.211.198.178/udp/4101/quic",
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

    #[test(tokio::test)]
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
        let resp = server.swarm_connect_post(&vec!["/ip4/1.1.1.1/tcp/4101/p2p/12D3KooWFtPWZ1uHShnbvmxYJGmygUfTVmcb6iSQfiAm4XnmsQ8t".to_string()], &Context).await.unwrap();

        expect![[r#"
            Success(
                SwarmConnectPost200Response {
                    strings: [
                        "connect 12D3KooWFtPWZ1uHShnbvmxYJGmygUfTVmcb6iSQfiAm4XnmsQ8t success",
                    ],
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }
    #[test(tokio::test)]
    async fn swarm_connect_bad_request() {
        let mock_ipfs = MockIpfsDepTest::new();
        let server = Server::new(mock_ipfs);
        let resp = server
            .swarm_connect_post(&vec!["/ip4/1.1.1.1/tcp/4101".to_string()], &Context)
            .await
            .unwrap();

        expect![[r#"
            BadRequest(
                Error {
                    message: "no peer id specificed in multiaddrs",
                    code: 0.0,
                    type: "error",
                },
            )
        "#]]
        .assert_debug_eq(&resp);
    }
    #[test(tokio::test)]
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
