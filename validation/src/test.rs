use std::collections::HashMap;

use ceramic_core::Cid;
use ceramic_event::unvalidated::{self, signed};

use ceramic_car::{
    sync::{CarReader, CarWriter},
    CarHeader,
};
use ipld_core::ipld::Ipld;

use crate::{
    cacao_verifier::Verifier as _, event_verifier::Verifier as _, verifier::opts::VerifyJwsOpts,
    VerifyCacaoOpts,
};

/// A signed init event encoded as a carfile
pub const SIGNED_INIT_EVENT_CAR: &str = "uO6Jlcm9vdHOB2CpYJgABhQESII6AYH_8-NniumDaEPcbPId6ZQAMFfViEDLxGVnVBNOOZ3ZlcnNpb24B0QEBcRIgEXCWI0EH1zQcSl57SUKRoo0WwhL6nMo8kLKfvgNaG0OiZGRhdGGhZXN0ZXBoGQFNZmhlYWRlcqRjc2VwZW1vZGVsZW1vZGVsWCjOAQIBhQESIKDoMqM144vTQLQ6DwKZvzxRWg_DPeTNeRCkPouTHo1YZnVuaXF1ZUxEpvE6skELu2qFaN5rY29udHJvbGxlcnOBeDhkaWQ6a2V5Ono2TWt0QnluQVBMckV5ZVM3cFZ0aGJpeVNjbWZ1OG41Vjdib1hneHlvNXEzU1pSUroCAYUBEiCOgGB__PjZ4rpg2hD3GzyHemUADBX1YhAy8RlZ1QTTjqJncGF5bG9hZFgkAXESIBFwliNBB9c0HEpee0lCkaKNFsIS-pzKPJCyn74DWhtDanNpZ25hdHVyZXOBomlwcm90ZWN0ZWRYgXsiYWxnIjoiRWREU0EiLCJraWQiOiJkaWQ6a2V5Ono2TWt0QnluQVBMckV5ZVM3cFZ0aGJpeVNjbWZ1OG41Vjdib1hneHlvNXEzU1pSUiN6Nk1rdEJ5bkFQTHJFeWVTN3BWdGhiaXlTY21mdThuNVY3Ym9YZ3h5bzVxM1NaUlIifWlzaWduYXR1cmVYQCQDjlx8fT8rbTR4088HtOE27LJMc38DSuf1_XtK14hDp1Q6vhHqnuiobqp5EqNOp0vNFCCzwgG-Dsjmes9jJww";
/// Data Event for a stream with a signed init event
pub const SIGNED_DATA_EVENT_CAR: &str = "uO6Jlcm9vdHOB2CpYJgABhQESICddBxl5Sk2e7I20pzX9kDLf0jj6WvIQ1KqbM3WQiClDZ3ZlcnNpb24BqAEBcRIgdtssXEgR7sXQQQA1doBpxUpTn4pcAaVFZfQjyo-03SGjYmlk2CpYJgABhQESII6AYH_8-NniumDaEPcbPId6ZQAMFfViEDLxGVnVBNOOZGRhdGGBo2JvcGdyZXBsYWNlZHBhdGhmL3N0ZXBoZXZhbHVlGQFOZHByZXbYKlgmAAGFARIgjoBgf_z42eK6YNoQ9xs8h3plAAwV9WIQMvEZWdUE0466AgGFARIgJ10HGXlKTZ7sjbSnNf2QMt_SOPpa8hDUqpszdZCIKUOiZ3BheWxvYWRYJAFxEiB22yxcSBHuxdBBADV2gGnFSlOfilwBpUVl9CPKj7TdIWpzaWduYXR1cmVzgaJpcHJvdGVjdGVkWIF7ImFsZyI6IkVkRFNBIiwia2lkIjoiZGlkOmtleTp6Nk1rdEJ5bkFQTHJFeWVTN3BWdGhiaXlTY21mdThuNVY3Ym9YZ3h5bzVxM1NaUlIjejZNa3RCeW5BUExyRXllUzdwVnRoYml5U2NtZnU4bjVWN2JvWGd4eW81cTNTWlJSIn1pc2lnbmF0dXJlWECym-Kwb5ti-T5dCygt4zf8Lr6MescAbkk_DILoy3fFjYG8fZVUCGKDQiTTHbNbzOk1yze7-2hA3AKdBfzJY1kA";
/// Data event signed with a CACAO
pub const CACAO_SIGNED_DATA_EVENT_CAR:&str="mO6Jlcm9vdHOB2CpYJgABhQESIN12WhnV8Y3aMRxZYqUSpaO4mSQnbGxEpeC8jMsldwv6Z3ZlcnNpb24BigQBcRIgtP3Gc62zs2I/pu98uctnwBAYUUrgyLjnPaxYwOnBytajYWihYXRnZWlwNDM2MWFwqWNhdWR4OGRpZDprZXk6ejZNa3I0YTNaM0ZGYUpGOFlxV25XblZIcWQ1ZURqWk45YkRTVDZ3VlpDMWhKODFQY2V4cHgYMjAyNC0wNi0xOVQyMDowNDo0Mi40NjRaY2lhdHgYMjAyNC0wNi0xMlQyMDowNDo0Mi40NjRaY2lzc3g7ZGlkOnBraDplaXAxNTU6MToweDM3OTRkNGYwNzdjMDhkOTI1ZmY4ZmY4MjAwMDZiNzM1MzI5OWIyMDBlbm9uY2Vqd1BpQ09jcGtsbGZkb21haW5kdGVzdGd2ZXJzaW9uYTFpcmVzb3VyY2VzgWtjZXJhbWljOi8vKmlzdGF0ZW1lbnR4PEdpdmUgdGhpcyBhcHBsaWNhdGlvbiBhY2Nlc3MgdG8gc29tZSBvZiB5b3VyIGRhdGEgb24gQ2VyYW1pY2FzomFzeIQweGIyNjY5OTkyNjM0NDZkZGI5YmY1ODg4MjVlOWFjMDhiNTQ1ZTY1NWY2MDc3ZThkODU3OWE4ZDY2MzljMTE2N2M1NmY3ZGFlN2FjNzBmN2ZhZWQ4YzE0MWFmOWUxMjRhN2ViNGY3NzQyM2E1NzJiMzYxNDRhZGE4ZWYyMjA2Y2RhMWNhdGZlaXAxOTHTAQFxEiB0pJYyJOf2PmUMzjHeHCaX9ETIA0AKnHWwb1l0CfEy/KJkZGF0YaFkc3RlcBkCWGZoZWFkZXKkY3NlcGVtb2RlbGVtb2RlbFgozgECAYUBEiAGE66nrxdaqHUZ5oBVN6FulPnXix/we9MdpVKJHSR4uGZ1bmlxdWVMxwa2TBHgC66/1V9xa2NvbnRyb2xsZXJzgXg7ZGlkOnBraDplaXAxNTU6MToweDM3OTRkNGYwNzdjMDhkOTI1ZmY4ZmY4MjAwMDZiNzM1MzI5OWIyMDCFAwGFARIg3XZaGdXxjdoxHFlipRKlo7iZJCdsbESl4LyMyyV3C/qiZ3BheWxvYWRYJAFxEiB0pJYyJOf2PmUMzjHeHCaX9ETIA0AKnHWwb1l0CfEy/GpzaWduYXR1cmVzgaJpcHJvdGVjdGVkWMx7ImFsZyI6IkVkRFNBIiwiY2FwIjoiaXBmczovL2JhZnlyZWlmdTd4ZGhobG50d25yZDdqeHBwczQ0d3o2YWNhbWZjc3hhemM0b29wbm1sZGFvdHFvazJ5Iiwia2lkIjoiZGlkOmtleTp6Nk1rcjRhM1ozRkZhSkY4WXFXblduVkhxZDVlRGpaTjliRFNUNndWWkMxaEo4MVAjejZNa3I0YTNaM0ZGYUpGOFlxV25XblZIcWQ1ZURqWk45YkRTVDZ3VlpDMWhKODFQIn1pc2lnbmF0dXJlWEB19qFT2VTY3D/LT8MYvVi0fK4tfVCgB3tMZ18ZPG+Tc4CSxm+R+Q6u57MEUWXUf1dBzBU0l1Un3lxurDlSueID";

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TestMeta {
    pub controller: String,
    pub model: Ipld,
    // TODO: use invalid/expired in tests
    pub expired_init_event_capability: Option<Cid>,
    pub expired_data_event_capability: Option<Cid>,
    pub invalid_init_event_signature: Cid,
    pub invalid_init_event_capability_signature: Option<Cid>,
    pub invalid_data_event_signature: Cid,
    pub invalid_data_event_capability_signature: Option<Cid>,
    pub valid_init_event: Cid,
    pub valid_init_payload: Cid,
    pub valid_data_event: Cid,
    pub valid_data_payload: Cid,
    pub valid_deterministic_event: Cid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TestEventType {
    DeterministicInit,
    SignedInit,
    SignedData,
    InvalidSignedInit,
    InvalidSignedData,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SigningType {
    Ethereum,
    Solana,
    EcdsaP256,
    Ed2559,
    WebAuthN,
}

#[derive(Debug)]
pub struct ParsedSampleData {
    pub meta: TestMeta,
    pub blocks: HashMap<Cid, Vec<u8>>,
}

fn parse_test_data(data: &[u8]) -> ParsedSampleData {
    let car = CarReader::new(data).unwrap();
    let header = car.header().to_owned();
    let blocks: HashMap<Cid, Vec<u8>> = car.into_iter().collect::<Result<_, _>>().unwrap();
    assert_eq!(1, header.roots().len(), "More than one root not supported");
    let root_cid = &header.roots()[0];
    let meta = serde_ipld_dagcbor::from_slice::<TestMeta>(blocks.get(root_cid).unwrap()).unwrap();
    ParsedSampleData { meta, blocks }
}

fn extract_controller(event: &unvalidated::Event<Ipld>) -> String {
    match event {
        unvalidated::Event::Time(_) => unreachable!("not a time event"),
        unvalidated::Event::Signed(s) => match s.payload() {
            unvalidated::Payload::Data(_) => {
                unreachable!("should not have data event: {:?}", event)
            }
            unvalidated::Payload::Init(init) => {
                init.header().controllers().first().cloned().unwrap()
            }
        },
        unvalidated::Event::Unsigned(_) => unreachable!("not unsigned event"),
    }
}

fn extract_init_controller(
    signing_type: SigningType,
    event_type: TestEventType,
    event: &unvalidated::Event<Ipld>,
) -> String {
    match event_type {
        TestEventType::DeterministicInit => unreachable!("nothing to verify"),
        TestEventType::SignedInit | TestEventType::InvalidSignedInit => extract_controller(event),
        TestEventType::SignedData | TestEventType::InvalidSignedData => {
            // get init event controller
            let (_cid, init) = get_test_event(signing_type, TestEventType::SignedInit);
            extract_controller(&init)
        }
    }
}

pub async fn assert_invalid_event(
    signing_type: SigningType,
    event_type: TestEventType,
    opts: &VerifyJwsOpts,
) {
    let (_cid, event) = get_test_event(signing_type, event_type);
    let controller = extract_init_controller(signing_type, event_type, &event);
    match event {
        unvalidated::Event::Time(_) => unreachable!("not a time event"),
        unvalidated::Event::Signed(s) => {
            // should verify against init controller and self contained
            match s.verify_signature(Some(&controller), opts).await {
                Ok(_) => {
                    panic!("should have been invalid")
                }
                Err(e) => {
                    tracing::debug!("failed as expected: {:#}", e);
                }
            }
            match s.verify_signature(None, opts).await {
                Ok(_) => {
                    panic!("should have been invalid")
                }
                Err(e) => {
                    tracing::debug!("failed as expected: {:#}", e);
                }
            }
        }
        unvalidated::Event::Unsigned(_) => unreachable!("not unsigned"),
    };
}

pub async fn verify_event(
    signing_type: SigningType,
    event_type: TestEventType,
    opts: &VerifyJwsOpts,
) {
    let (_cid, event) = get_test_event(signing_type, event_type);

    let controller = extract_init_controller(signing_type, event_type, &event);
    match event {
        unvalidated::Event::Time(_) => unreachable!("not a time event"),
        unvalidated::Event::Signed(s) => {
            // should verify against init controller and self contained
            match s.verify_signature(Some(&controller), opts).await {
                Ok(_) => {}
                Err(e) => {
                    panic!(
                        "{:?} for {:?} failed with controller {controller}: {:?}: {:#}",
                        event_type, signing_type, s, e,
                    )
                }
            }
            match s.verify_signature(None, opts).await {
                Ok(_) => {}
                Err(e) => {
                    panic!(
                        "{:?} for {:?} failed: {:?}: {:#}",
                        event_type, signing_type, s, e
                    )
                }
            }
        }
        unvalidated::Event::Unsigned(_) => unreachable!("not unsigned"),
    };
}

pub async fn verify_event_cacao(
    signing_type: SigningType,
    event_type: TestEventType,
    opts: &VerifyCacaoOpts,
) {
    let (_cid, event) = get_test_event(signing_type, event_type);
    let event = match event {
        unvalidated::Event::Time(_) => unreachable!("not a time event"),
        unvalidated::Event::Signed(s) => s,
        unvalidated::Event::Unsigned(_) => unreachable!("not unsigned"),
    };
    let cap = &event.capability().unwrap().1;
    match cap.verify_signature(opts).await {
        Ok(_) => {}
        Err(e) => {
            panic!("{:?}: {:#}", cap, e)
        }
    }
}

pub fn get_test_event(
    signing_type: SigningType,
    event_type: TestEventType,
) -> (Cid, unvalidated::Event<Ipld>) {
    let parsed = match signing_type {
        SigningType::Ethereum => {
            let data = include_bytes!("../test-vectors/pkh-ethereum.car");
            parse_test_data(data.as_slice())
        }
        SigningType::Solana => {
            let data = include_bytes!("../test-vectors/pkh-solana.car");
            parse_test_data(data.as_slice())
        }
        SigningType::EcdsaP256 => {
            let data = include_bytes!("../test-vectors/key-ecdsa-p256.car");
            parse_test_data(data.as_slice())
        }
        SigningType::Ed2559 => {
            let data = include_bytes!("../test-vectors/key-ed25519.car");
            parse_test_data(data.as_slice())
        }
        SigningType::WebAuthN => {
            let data = include_bytes!("../test-vectors/key-webauthn.car");
            parse_test_data(data.as_slice())
        }
    };
    let mut event_car = Vec::new();
    let (envelope_cid, payload_cid) = match event_type {
        TestEventType::DeterministicInit => (parsed.meta.valid_deterministic_event, None),
        TestEventType::SignedInit => (
            parsed.meta.valid_init_event,
            Some(parsed.meta.valid_init_payload),
        ),
        TestEventType::SignedData => (
            parsed.meta.valid_data_event,
            Some(parsed.meta.valid_data_payload),
        ),
        TestEventType::InvalidSignedInit => (
            parsed.meta.invalid_init_event_signature,
            Some(parsed.meta.valid_init_payload),
        ),
        TestEventType::InvalidSignedData => (
            parsed.meta.invalid_data_event_signature,
            Some(parsed.meta.valid_data_payload),
        ),
    };
    let envelope = parsed.blocks.get(&envelope_cid).unwrap();

    let cacao_cid = if matches!(event_type, TestEventType::DeterministicInit) {
        None
    } else {
        let envelope = serde_ipld_dagcbor::from_slice::<signed::Envelope>(envelope).unwrap();
        envelope.capability()
    };

    let roots: Vec<Cid> = vec![envelope_cid];
    let mut writer = CarWriter::new(CarHeader::V1(roots.into()), &mut event_car);
    if let Some(cacao_cid) = cacao_cid {
        let cacao = parsed.blocks.get(&cacao_cid).unwrap();
        writer.write(cacao_cid, cacao).unwrap();
    }
    if let Some(payload_cid) = payload_cid {
        let payload = parsed.blocks.get(&payload_cid).unwrap();
        writer.write(payload_cid, payload).unwrap();
    }
    writer.write(envelope_cid, envelope).unwrap();
    writer.finish().unwrap();

    unvalidated::Event::<Ipld>::decode_car(event_car.as_slice(), false).unwrap()
}
