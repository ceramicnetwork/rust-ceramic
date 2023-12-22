use bytes::Bytes;
use cid::Cid;
use derive_more::{From, TryInto};
use libp2p::{Multiaddr, PeerId};
use quic_rpc::{
    message::{Msg, RpcMsg, ServerStreaming},
    Service,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::{RpcResult, VersionRequest, VersionResponse, WatchRequest, WatchResponse};

pub type P2pAddr = super::addr::Addr<P2pService>;

#[derive(Serialize, Deserialize, Debug)]
pub struct Key(pub Bytes);

#[derive(Serialize, Deserialize, Debug)]
pub struct LocalPeerIdRequest;

#[derive(Serialize, Deserialize, Debug)]
pub struct LocalPeerIdResponse {
    pub peer_id: PeerId,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ExternalAddrsRequest;

#[derive(Serialize, Deserialize, Debug)]
pub struct ExternalAddrsResponse {
    pub addrs: Vec<Multiaddr>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ListenersRequest;

#[derive(Serialize, Deserialize, Debug)]
pub struct ListenersResponse {
    pub addrs: Vec<Multiaddr>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BitswapRequest {
    pub cid: Cid,
    pub providers: Vec<PeerId>,
    pub ctx: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BitswapResponse {
    pub data: Bytes,
    pub ctx: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FetchProvidersDhtRequest {
    pub key: Key,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FetchProvidersDhtResponse {
    pub providers: Vec<PeerId>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct NotifyNewBlocksBitswapRequest {
    pub blocks: Vec<BitswapBlock>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BitswapBlock {
    pub cid: Cid,
    pub data: Bytes,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StopSessionBitswapRequest {
    pub ctx: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StartProvidingRequest {
    pub key: Key,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StopProvidingRequest {
    pub key: Key,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GetListeningAddrsRequest;

#[derive(Serialize, Deserialize, Debug)]
pub struct GetListeningAddrsResponse {
    pub peer_id: PeerId,
    pub addrs: Vec<Multiaddr>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GetPeersRequest;

#[derive(Serialize, Deserialize, Debug)]
pub struct GetPeersResponse {
    pub peers: BTreeMap<PeerId, Vec<Multiaddr>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ConnectRequest {
    pub peer_id: PeerId,
    pub addrs: Vec<Multiaddr>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ConnectByPeerIdRequest {
    pub peer_id: PeerId,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DisconnectRequest {
    pub peer_id: PeerId,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ShutdownRequest;

#[derive(Serialize, Deserialize, Debug)]
pub struct LookupRequest {
    pub peer_id: PeerId,
    pub addr: Option<Multiaddr>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LookupLocalRequest;

#[derive(Serialize, Deserialize, Debug)]
pub struct LookupResponse {
    pub peer_id: PeerId,
    pub protocol_version: String,
    pub agent_version: String,
    pub listen_addrs: Vec<Multiaddr>,
    pub protocols: Vec<String>,
    pub observed_addrs: Vec<Multiaddr>,
}

#[derive(Serialize, Deserialize, Debug, From, TryInto)]
pub enum P2pRequest {
    Watch(WatchRequest),
    Version(VersionRequest),
    Shutdown(ShutdownRequest),
    FetchBitswap(BitswapRequest),
    FetchProviderDht(FetchProvidersDhtRequest),
    StopSessionBitswap(StopSessionBitswapRequest),
    NotifyNewBlocksBitswap(NotifyNewBlocksBitswapRequest),
    GetListeningAddrs(GetListeningAddrsRequest),
    GetPeers(GetPeersRequest),
    PeerConnect(ConnectRequest),
    PeerConnectByPeerId(ConnectByPeerIdRequest),
    PeerDisconnect(DisconnectRequest),
    Lookup(LookupRequest),
    LookupLocal(LookupLocalRequest),
    StartProviding(StartProvidingRequest),
    StopProviding(StopProvidingRequest),
    LocalPeerId(LocalPeerIdRequest),
    ExternalAddrs(ExternalAddrsRequest),
    Listeners(ListenersRequest),
}

#[derive(Serialize, Deserialize, Debug, From, TryInto)]
pub enum P2pResponse {
    Watch(WatchResponse),
    Version(VersionResponse),
    FetchBitswap(RpcResult<BitswapResponse>),
    FetchProviderDht(RpcResult<FetchProvidersDhtResponse>),
    GetListeningAddrs(RpcResult<GetListeningAddrsResponse>),
    GetPeers(RpcResult<GetPeersResponse>),
    Lookup(RpcResult<LookupResponse>),
    LocalPeerId(RpcResult<LocalPeerIdResponse>),
    ExternalAddrs(RpcResult<ExternalAddrsResponse>),
    Listeners(RpcResult<ListenersResponse>),
    UnitResult(RpcResult<()>),
}

#[derive(Debug, Clone)]
pub struct P2pService;

impl Service for P2pService {
    type Req = P2pRequest;
    type Res = P2pResponse;
}

impl Msg<P2pService> for WatchRequest {
    type Response = WatchResponse;

    type Update = Self;

    type Pattern = ServerStreaming;
}

impl RpcMsg<P2pService> for VersionRequest {
    type Response = VersionResponse;
}

impl RpcMsg<P2pService> for ShutdownRequest {
    type Response = RpcResult<()>;
}

impl RpcMsg<P2pService> for BitswapRequest {
    type Response = RpcResult<BitswapResponse>;
}

impl Msg<P2pService> for FetchProvidersDhtRequest {
    type Response = RpcResult<FetchProvidersDhtResponse>;

    type Update = Self;

    type Pattern = ServerStreaming;
}

impl RpcMsg<P2pService> for StopSessionBitswapRequest {
    type Response = RpcResult<()>;
}

impl RpcMsg<P2pService> for NotifyNewBlocksBitswapRequest {
    type Response = RpcResult<()>;
}

impl RpcMsg<P2pService> for GetListeningAddrsRequest {
    type Response = RpcResult<GetListeningAddrsResponse>;
}

impl RpcMsg<P2pService> for GetPeersRequest {
    type Response = RpcResult<GetPeersResponse>;
}

impl RpcMsg<P2pService> for ConnectRequest {
    type Response = RpcResult<()>;
}

impl RpcMsg<P2pService> for ConnectByPeerIdRequest {
    type Response = RpcResult<()>;
}

impl RpcMsg<P2pService> for DisconnectRequest {
    type Response = RpcResult<()>;
}

impl RpcMsg<P2pService> for LookupRequest {
    type Response = RpcResult<LookupResponse>;
}

impl RpcMsg<P2pService> for LookupLocalRequest {
    type Response = RpcResult<LookupResponse>;
}

impl RpcMsg<P2pService> for StartProvidingRequest {
    type Response = RpcResult<()>;
}

impl RpcMsg<P2pService> for StopProvidingRequest {
    type Response = RpcResult<()>;
}

impl RpcMsg<P2pService> for LocalPeerIdRequest {
    type Response = RpcResult<LocalPeerIdResponse>;
}

impl RpcMsg<P2pService> for ExternalAddrsRequest {
    type Response = RpcResult<ExternalAddrsResponse>;
}

impl RpcMsg<P2pService> for ListenersRequest {
    type Response = RpcResult<ListenersResponse>;
}
