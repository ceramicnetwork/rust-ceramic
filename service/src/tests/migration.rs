use std::{
    collections::{BTreeMap, BTreeSet},
    io::Cursor,
    str::FromStr,
};

use anyhow::Result;
use async_trait::async_trait;
use ceramic_car::CarReader;
use ceramic_core::{DidDocument, EventId, Network, StreamId};
use ceramic_event::unvalidated;
use cid::Cid;
use futures::{pin_mut, stream::BoxStream, StreamExt as _, TryStreamExt as _};
use ipld_core::{codec::Codec, ipld, ipld::Ipld};
use multihash_codetable::{Code, MultihashDigest};
use rand::{thread_rng, Rng, RngCore};
use recon::Key;
use serde_ipld_dagcbor::codec::DagCborCodec;
use test_log::test;

use crate::{event::BlockStore, CeramicEventService};

struct InMemBlockStore {
    blocks: BTreeMap<Cid, Vec<u8>>,
}

#[async_trait]
impl BlockStore for InMemBlockStore {
    fn blocks(&self) -> BoxStream<'static, Result<(Cid, Vec<u8>)>> {
        let blocks = self.blocks.clone();
        futures::stream::iter(blocks.into_iter().map(Result::Ok)).boxed()
    }
    async fn block_data(&self, cid: &Cid) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self.blocks.get(cid).cloned())
    }
}

async fn blocks_from_cars(cars: Vec<Vec<u8>>) -> InMemBlockStore {
    let mut blocks = BTreeMap::new();
    for car in cars {
        let reader = CarReader::new(Cursor::new(car)).await.unwrap();
        let stream = reader.stream();
        pin_mut!(stream);

        while let Some((cid, block)) = stream.try_next().await.unwrap() {
            blocks.insert(cid, block);
        }
    }
    InMemBlockStore { blocks }
}
async fn test_migration(cars: Vec<Vec<u8>>) {
    let expected_events: BTreeSet<_> = cars
        .iter()
        .map(|car| multibase::encode(multibase::Base::Base64Url, car))
        .collect();
    let blocks = blocks_from_cars(cars).await;
    let conn = ceramic_store::SqlitePool::connect_in_memory()
        .await
        .unwrap();
    let service = CeramicEventService::new_with_event_validation(conn)
        .await
        .unwrap();
    service
        .migrate_from_ipfs(Network::Local(42), blocks)
        .await
        .unwrap();
    let actual_events: BTreeSet<_> = recon::Store::range_with_values(
        &service,
        &EventId::min_value()..&EventId::max_value(),
        0,
        usize::MAX,
    )
    .await
    .unwrap()
    .map(|(_event_id, car)| multibase::encode(multibase::Base::Base64Url, car))
    .collect();
    assert_eq!(expected_events, actual_events)
}
async fn signer() -> unvalidated::signed::JwkSigner {
    unvalidated::signed::JwkSigner::new(
            DidDocument::new("did:key:z6MktBynAPLrEyeS7pVthbiyScmfu8n5V7boXgxyo5q3SZRR#z6MktBynAPLrEyeS7pVthbiyScmfu8n5V7boXgxyo5q3SZRR"),
            "df9ecf4c79e5ad77701cfc88c196632b353149d85810a381f469f8fc05dc1b92",
        )
        .await
        .unwrap()
}
async fn random_unsigned_init_event() -> unvalidated::init::Payload<Ipld> {
    let model =
        StreamId::from_str("kjzl6hvfrbw6c90uwoyz8j519gxma787qbsfjtrarkr1huq1g1s224k7hopvsyg")
            .unwrap();
    let model = model.to_vec();
    let mut unique = vec![0; 12];
    thread_rng().fill_bytes(&mut unique);
    let data = ipld_core::ipld!({"key": thread_rng().gen::<u32>()});

    unvalidated::Builder::init()
        .with_controller("did:key:z6MktBynAPLrEyeS7pVthbiyScmfu8n5V7boXgxyo5q3SZRR".to_string())
        .with_sep("model".to_string(), model)
        .with_unique(unique)
        .with_data(data)
        .build()
}
async fn random_signed_init_event() -> unvalidated::signed::Event<Ipld> {
    let model =
        StreamId::from_str("kjzl6hvfrbw6c90uwoyz8j519gxma787qbsfjtrarkr1huq1g1s224k7hopvsyg")
            .unwrap();
    let model = model.to_vec();
    let mut unique = vec![0; 12];
    thread_rng().fill_bytes(&mut unique);
    let data = ipld_core::ipld!({"key": thread_rng().gen::<u32>()});

    let payload = unvalidated::Builder::init()
        .with_controller("did:key:z6MktBynAPLrEyeS7pVthbiyScmfu8n5V7boXgxyo5q3SZRR".to_string())
        .with_sep("model".to_string(), model)
        .with_unique(unique)
        .with_data(data)
        .build();

    unvalidated::signed::Event::from_payload(unvalidated::Payload::Init(payload), signer().await)
        .unwrap()
}
// We do not yet have the ability to sign events with a CACAO.
// So for now we test against a hard coded event signed with a CACAO.
const CACAO_CAR:&str="mO6Jlcm9vdHOB2CpYJgABhQESIN12WhnV8Y3aMRxZYqUSpaO4mSQnbGxEpeC8jMsldwv6Z3ZlcnNpb24BigQBcRIgtP3Gc62zs2I/pu98uctnwBAYUUrgyLjnPaxYwOnBytajYWihYXRnZWlwNDM2MWFwqWNhdWR4OGRpZDprZXk6ejZNa3I0YTNaM0ZGYUpGOFlxV25XblZIcWQ1ZURqWk45YkRTVDZ3VlpDMWhKODFQY2V4cHgYMjAyNC0wNi0xOVQyMDowNDo0Mi40NjRaY2lhdHgYMjAyNC0wNi0xMlQyMDowNDo0Mi40NjRaY2lzc3g7ZGlkOnBraDplaXAxNTU6MToweDM3OTRkNGYwNzdjMDhkOTI1ZmY4ZmY4MjAwMDZiNzM1MzI5OWIyMDBlbm9uY2Vqd1BpQ09jcGtsbGZkb21haW5kdGVzdGd2ZXJzaW9uYTFpcmVzb3VyY2VzgWtjZXJhbWljOi8vKmlzdGF0ZW1lbnR4PEdpdmUgdGhpcyBhcHBsaWNhdGlvbiBhY2Nlc3MgdG8gc29tZSBvZiB5b3VyIGRhdGEgb24gQ2VyYW1pY2FzomFzeIQweGIyNjY5OTkyNjM0NDZkZGI5YmY1ODg4MjVlOWFjMDhiNTQ1ZTY1NWY2MDc3ZThkODU3OWE4ZDY2MzljMTE2N2M1NmY3ZGFlN2FjNzBmN2ZhZWQ4YzE0MWFmOWUxMjRhN2ViNGY3NzQyM2E1NzJiMzYxNDRhZGE4ZWYyMjA2Y2RhMWNhdGZlaXAxOTHTAQFxEiB0pJYyJOf2PmUMzjHeHCaX9ETIA0AKnHWwb1l0CfEy/KJkZGF0YaFkc3RlcBkCWGZoZWFkZXKkY3NlcGVtb2RlbGVtb2RlbFgozgECAYUBEiAGE66nrxdaqHUZ5oBVN6FulPnXix/we9MdpVKJHSR4uGZ1bmlxdWVMxwa2TBHgC66/1V9xa2NvbnRyb2xsZXJzgXg7ZGlkOnBraDplaXAxNTU6MToweDM3OTRkNGYwNzdjMDhkOTI1ZmY4ZmY4MjAwMDZiNzM1MzI5OWIyMDCFAwGFARIg3XZaGdXxjdoxHFlipRKlo7iZJCdsbESl4LyMyyV3C/qiZ3BheWxvYWRYJAFxEiB0pJYyJOf2PmUMzjHeHCaX9ETIA0AKnHWwb1l0CfEy/GpzaWduYXR1cmVzgaJpcHJvdGVjdGVkWMx7ImFsZyI6IkVkRFNBIiwiY2FwIjoiaXBmczovL2JhZnlyZWlmdTd4ZGhobG50d25yZDdqeHBwczQ0d3o2YWNhbWZjc3hhemM0b29wbm1sZGFvdHFvazJ5Iiwia2lkIjoiZGlkOmtleTp6Nk1rcjRhM1ozRkZhSkY4WXFXblduVkhxZDVlRGpaTjliRFNUNndWWkMxaEo4MVAjejZNa3I0YTNaM0ZGYUpGOFlxV25XblZIcWQ1ZURqWk45YkRTVDZ3VlpDMWhKODFQIn1pc2lnbmF0dXJlWEB19qFT2VTY3D/LT8MYvVi0fK4tfVCgB3tMZ18ZPG+Tc4CSxm+R+Q6u57MEUWXUf1dBzBU0l1Un3lxurDlSueID";
fn new_cacao_signed_data_event() -> Vec<u8> {
    multibase::decode(CACAO_CAR).unwrap().1
}

async fn random_signed_data_event() -> Vec<unvalidated::Event<Ipld>> {
    let init = random_signed_init_event().await;
    let data = ipld_core::ipld!({"key": thread_rng().gen::<u32>()});
    let payload = unvalidated::Builder::data()
        .with_id(*init.envelope_cid())
        .with_prev(*init.envelope_cid())
        .with_data(data)
        .build();
    vec![
        init.into(),
        unvalidated::signed::Event::from_payload(
            unvalidated::Payload::Data(payload),
            signer().await,
        )
        .unwrap()
        .into(),
    ]
}

fn random_cid() -> cid::Cid {
    use multihash_codetable::Code;
    use multihash_derive::MultihashDigest;

    let mut data = [0u8; 8];
    rand::Rng::fill(&mut rand::thread_rng(), &mut data);
    let hash = Code::Sha2_256.digest(&data);
    cid::Cid::new_v1(0x00, hash)
}

fn cid_from_dag_cbor(data: &[u8]) -> Cid {
    Cid::new_v1(
        <DagCborCodec as Codec<Ipld>>::CODE,
        Code::Sha2_256.digest(data),
    )
}
// create random time event with a previous unsigned init event
async fn random_unsigned_init_time_event() -> Vec<unvalidated::Event<Ipld>> {
    let init = random_unsigned_init_event().await;
    let init_cid = init.encoded_cid().unwrap();
    vec![init.into(), random_time_event(init_cid).await.into()]
}
// create random time event with a previous signed init event
async fn random_signed_init_time_event() -> Vec<unvalidated::Event<Ipld>> {
    let init = random_signed_init_event().await;

    let time_event = random_time_event(*init.envelope_cid()).await.into();
    vec![init.into(), time_event]
}

async fn random_time_event(prev: Cid) -> Box<unvalidated::TimeEvent> {
    let mut next = prev;
    let mut witness_nodes = Vec::new();
    for _ in 0..10 {
        // The other branch is randomly a cid or null
        let other = if thread_rng().gen() {
            Ipld::Link(random_cid())
        } else {
            Ipld::Null
        };
        let (idx, edge) = if thread_rng().gen() {
            (0, ipld!([next, other]))
        } else {
            (1, ipld!([other, next]))
        };
        let edge_bytes = serde_ipld_dagcbor::to_vec(&edge).unwrap();
        next = cid_from_dag_cbor(&edge_bytes);
        witness_nodes.push((idx, edge));
    }
    let (root_idx, root_edge) = witness_nodes.pop().unwrap();

    let mut builder = unvalidated::Builder::time()
        .with_id(prev)
        .with_tx(
            "eip155:11155111".to_string(),
            random_cid(),
            "f(bytes32)".to_string(),
        )
        .with_root(root_idx, root_edge);
    for (idx, edge) in witness_nodes.into_iter().rev() {
        builder = builder.with_witness_node(idx, edge);
    }
    let time = builder.build().unwrap();
    Box::new(time)
}
#[test(tokio::test)]
async fn unsigned_init_event() {
    test_migration(vec![random_unsigned_init_event()
        .await
        .encode_car()
        .unwrap()])
    .await;
}
#[test(tokio::test)]
async fn many_unsigned_init_events() {
    test_migration(vec![
        random_unsigned_init_event().await.encode_car().unwrap(),
        random_unsigned_init_event().await.encode_car().unwrap(),
        random_unsigned_init_event().await.encode_car().unwrap(),
    ])
    .await;
}
#[test(tokio::test)]
async fn signed_init_event() {
    test_migration(vec![random_signed_init_event().await.encode_car().unwrap()]).await;
}
#[test(tokio::test)]
async fn many_signed_init_events() {
    test_migration(vec![
        random_signed_init_event().await.encode_car().unwrap(),
        random_signed_init_event().await.encode_car().unwrap(),
        random_signed_init_event().await.encode_car().unwrap(),
    ])
    .await;
}
#[test(tokio::test)]
async fn signed_data_event() {
    let mut cars = Vec::new();
    for event in random_signed_data_event().await {
        cars.push(event.encode_car().unwrap());
    }
    test_migration(cars).await;
}
#[test(tokio::test)]
async fn many_signed_data_events() {
    let mut cars = Vec::new();
    for _ in 0..3 {
        for event in random_signed_data_event().await {
            cars.push(event.encode_car().unwrap());
        }
    }
    test_migration(cars).await;
}
#[test(tokio::test)]
async fn cacao_signed_data_event() {
    test_migration(vec![new_cacao_signed_data_event()]).await;
}
#[test(tokio::test)]
async fn unsigned_time_event() {
    let mut cars = Vec::new();
    for event in random_unsigned_init_time_event().await {
        cars.push(event.encode_car().unwrap());
    }
    test_migration(cars).await;
}
#[test(tokio::test)]
async fn signed_init_time_event() {
    let mut cars = Vec::new();
    for event in random_signed_init_time_event().await {
        cars.push(event.encode_car().unwrap());
    }

    test_migration(cars).await;
}
#[test(tokio::test)]
async fn many_time_events() {
    let mut cars = Vec::new();
    for _ in 0..3 {
        for event in random_unsigned_init_time_event().await {
            cars.push(event.encode_car().unwrap());
        }
    }
    for _ in 0..3 {
        for event in random_signed_init_time_event().await {
            cars.push(event.encode_car().unwrap());
        }
    }
    test_migration(cars).await;
}
#[test(tokio::test)]
async fn all_events() {
    let mut cars = Vec::new();

    cars.push(new_cacao_signed_data_event());

    for _ in 0..3 {
        cars.push(random_unsigned_init_event().await.encode_car().unwrap());
    }
    for _ in 0..3 {
        cars.push(random_signed_init_event().await.encode_car().unwrap());
    }
    for _ in 0..3 {
        for event in random_signed_data_event().await {
            cars.push(event.encode_car().unwrap());
        }
    }
    for _ in 0..3 {
        for event in random_unsigned_init_time_event().await {
            cars.push(event.encode_car().unwrap());
        }
    }
    for _ in 0..3 {
        for event in random_signed_init_time_event().await {
            cars.push(event.encode_car().unwrap());
        }
    }
    test_migration(cars).await;
}
