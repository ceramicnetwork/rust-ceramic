use anyhow::{anyhow, bail, Context as _, Result};
use ceramic_core::Jwk;
use ceramic_event::unvalidated::signed::{self, cacao::Capability};
use ipld_core::ipld::Ipld;
use ssi::{did_resolve::ResolutionInputMetadata, jws::Header};
use tracing::debug;

use super::{
    cacao_verifier::Verifier as _,
    jws::{jws_digest, verify_jws, VerifyJwsInput},
    opts::{AtTime, VerifyJwsOpts},
};

#[async_trait::async_trait]
/// Trait to represeng verifying a signature.
pub trait Verifier {
    /// Verify the JWS signature against the controller (issuer) if provided.
    /// Without issuer simply verifies the JWS and CACAO (if included) are valid, without following
    /// the DID delegation chain for the controller.
    async fn verify_signature(&self, controller: Option<&str>, opts: &VerifyJwsOpts) -> Result<()>;
}

/// Struct to wrap pieces needed from the Ceramic event being validated that makes testing easier.
/// We don't have to build events from the carfile, but can build the parts from json and construct this.
pub(crate) struct SignatureData<'a> {
    jws_header: Header,
    /// The event signature bytes
    signature: &'a [u8],
    /// The protected header bytes (the i.e. the `jws_header` field before being deserialized).
    /// We could compute this by serializing the data but the caller already has it.
    /// Using `ceramic_event::unvalidated::signed::Signature` would be nicer but is harder for testing
    /// as we can't easily construct one from the bytes since things are private, and getting the
    /// deserialization right is currently eluding me without reading the dag-jose bytes directly.
    protected: &'a [u8],
    /// The payload being signed over
    payload: &'a [u8],
    /// The CACAO used to delegate signing permission
    capability: Option<&'a Capability>,
}

impl SignatureData<'_> {
    async fn verify_signature(&self, controller: Option<&str>, opts: &VerifyJwsOpts) -> Result<()> {
        let did = self
            .jws_header
            .key_id
            .as_ref()
            .ok_or_else(|| anyhow!("missing jws kid"))?;

        // Should add time checks for revoked DID key and if it was signed in the valid window
        let signer_did = Jwk::resolve_did(did, &ResolutionInputMetadata::default())
            .await
            .context("failed to resolve did")?;

        let jwk = Jwk::new(&signer_did)
            .await
            .map_err(|e| anyhow!("failed to generate jwk for did: {}", e))?;

        let signer_did = &signer_did.id;

        if let Some(cacao) = self.capability {
            if cacao.payload.audience != *signer_did {
                bail!("signer '{signer_did}' was not granted permission by capability")
            }
            if let Some(issuer) = controller {
                if issuer_equals(issuer, &cacao.payload.issuer) {
                    // if controller is the cacao issuer being valid is sufficient
                } else if issuer != signer_did {
                    // we have to resolve the controller DID, check its controller list
                    // and make sure the issuer is included
                    resolve_did_verify_delegated(issuer, &cacao.payload.issuer, &opts.at_time)
                        .await?;
                } else {
                    // if the issuer (controller) is the signer, a valid CACAO is sufficient since
                    // we delegated to them as audience
                }
            }

            cacao.verify_signature(&opts.to_owned().into()).await?;
        } else if let Some(issuer) = controller {
            if issuer != signer_did {
                // if it's not a CACAO and the signer isn't the controller, we
                // make sure the signer delgated through the DID controllers list
                resolve_did_verify_delegated(issuer, signer_did, &opts.at_time).await?;
            }
        }

        verify_jws(VerifyJwsInput {
            jwk: &jwk,
            jws_digest: &jws_digest(self.protected, self.payload),
            alg: self.jws_header.algorithm,
            signature: self.signature,
        })
        .await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl Verifier for signed::Event<Ipld> {
    async fn verify_signature(&self, controller: Option<&str>, opts: &VerifyJwsOpts) -> Result<()> {
        let signature = self
            .envelope()
            .signature()
            .first()
            .ok_or_else(|| anyhow!("missing signature on signed event"))?;

        let protected = signature
            .protected()
            .ok_or_else(|| anyhow!("missing protected field"))?
            .as_slice();

        let jws_header = signature.jws_header().context("signature is not jws")?;
        let payload = self.envelope().payload().as_slice();
        let signature = signature.signature().as_slice();
        let to_verify = SignatureData {
            jws_header,
            protected,
            payload,
            signature,
            capability: self.capability().map(|(_, ref c)| c),
        };

        to_verify.verify_signature(controller, opts).await
    }
}

/// Resolve the controller DID and verify the delegated DID is in the controller list
async fn resolve_did_verify_delegated(issuer: &str, delegated: &str, time: &AtTime) -> Result<()> {
    let mut meta = ResolutionInputMetadata::default();
    let controller_did = match time {
        AtTime::At(t) => {
            meta.version_time = Some(
                t.unwrap_or_else(chrono::Utc::now)
                    .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            );
            Jwk::resolve_did(issuer, &meta)
                .await
                .context(format!("failed to resolve issuer did with time: {issuer}"))?
        }
        AtTime::SkipTimeChecks => Jwk::resolve_did(issuer, &meta)
            .await
            .context(format!("failed to resolve issuer did: {issuer}"))?,
    };
    if controller_did
        .controller
        .as_ref()
        .is_none_or(|c| !c.any(|c| c == delegated))
    {
        debug!(
            ?controller_did,
            ?issuer,
            ?delegated,
            ?time,
            "Failed to validate DID delegation"
        );
        bail!("invalid_jws: '{delegated}' not in controllers list for issuer: '{issuer}'")
    }
    Ok(())
}

fn issuer_equals(did_a: &str, did_b: &str) -> bool {
    if did_a == did_b {
        true
    } else if did_a.starts_with("did:pkh:eip155:1:") {
        did_a.to_lowercase() == did_b.to_lowercase()
    } else {
        false
    }
}

#[cfg(test)]
mod test {

    use base64::engine::Engine as _;
    use ceramic_event::unvalidated::{self};
    use test_log::test;

    use super::*;

    use crate::{
        test::{
            assert_invalid_event, verify_event, SigningType, TestEventType, SIGNED_DATA_EVENT_CAR,
            SIGNED_INIT_EVENT_CAR,
        },
        verifier::opts::VerifyJwsOpts,
    };

    const TEST_DID: &str = "did:key:z6MkgSV3tAuw7gUWqKCUY7ae6uWNxqYgdwPhUJbJhF9EFXm9";
    const TEST_DID2: &str = "did:key:z6Mkk3rtfoKDMMG4zyarNGwCQs44GSQ49pcYKQspHJPXSnVw";

    #[test(tokio::test)]
    async fn resolve_dids_not_delegated() {
        for did in [TEST_DID, TEST_DID2] {
            match resolve_did_verify_delegated(did, did, &AtTime::SkipTimeChecks).await {
                Ok(_) => panic!("should have errored: {did}"),
                Err(e) => assert!(e.to_string().starts_with("invalid_jws:"), "{did} {:#}", e),
            }
            match resolve_did_verify_delegated(did, did, &AtTime::At(Some(chrono::Utc::now())))
                .await
            {
                Ok(_) => panic!("should have errored: {did}"),
                Err(e) => assert!(e.to_string().starts_with("invalid_jws:"), "{did} {:#}", e),
            }
            match resolve_did_verify_delegated(did, did, &AtTime::At(None)).await {
                Ok(_) => panic!("should have errored: {did}"),
                Err(e) => assert!(e.to_string().starts_with("invalid_jws:"), "{did} {:#}", e),
            }
        }
    }

    #[test(tokio::test)]
    async fn valid_init_signatures() {
        for e_type in [
            SigningType::Solana,
            SigningType::Ethereum,
            SigningType::Ed2559,
            SigningType::EcdsaP256,
            SigningType::WebAuthN,
        ] {
            verify_event(e_type, TestEventType::SignedInit, &VerifyJwsOpts::default()).await;
        }
    }

    #[test(tokio::test)]
    async fn valid_data_signatures() {
        for e_type in [
            SigningType::Solana,
            SigningType::Ethereum,
            SigningType::Ed2559,
            SigningType::EcdsaP256,
            SigningType::WebAuthN,
        ] {
            verify_event(e_type, TestEventType::SignedData, &VerifyJwsOpts::default()).await;
        }
    }

    #[test(tokio::test)]
    async fn invalid_init_signatures() {
        for e_type in [
            SigningType::Solana,
            SigningType::Ethereum,
            SigningType::Ed2559,
            SigningType::EcdsaP256,
            SigningType::WebAuthN,
        ] {
            assert_invalid_event(
                e_type,
                TestEventType::InvalidSignedInit,
                &VerifyJwsOpts::default(),
            )
            .await;
        }
    }

    #[test(tokio::test)]
    async fn invalid_data_signatures() {
        for e_type in [
            SigningType::Solana,
            SigningType::Ethereum,
            SigningType::Ed2559,
            SigningType::EcdsaP256,
            SigningType::WebAuthN,
        ] {
            assert_invalid_event(
                e_type,
                TestEventType::InvalidSignedData,
                &VerifyJwsOpts::default(),
            )
            .await;
        }
    }

    #[test(tokio::test)]
    async fn local_car_init_event() {
        let (_, bytes) = multibase::decode(
            SIGNED_INIT_EVENT_CAR
                .chars()
                .filter(|c| !c.is_whitespace())
                .collect::<String>(),
        )
        .unwrap();
        let (_cid, event) =
            unvalidated::Event::<Ipld>::decode_car(std::io::Cursor::new(bytes), false).unwrap();

        match event {
            unvalidated::Event::Time(_) => unreachable!("not time"),
            unvalidated::Event::Signed(s) => s
                .verify_signature(
                    None,
                    &VerifyJwsOpts {
                        at_time: AtTime::SkipTimeChecks,
                        ..Default::default()
                    },
                )
                .await
                .expect("should be valid"),

            unvalidated::Event::Unsigned(_) => unreachable!("not unsigned"),
        }
    }

    #[test(tokio::test)]
    async fn local_car_data_event() {
        let (_, bytes) = multibase::decode(
            SIGNED_DATA_EVENT_CAR
                .chars()
                .filter(|c| !c.is_whitespace())
                .collect::<String>(),
        )
        .unwrap();
        let (_cid, event) =
            unvalidated::Event::<Ipld>::decode_car(std::io::Cursor::new(bytes), false).unwrap();

        match event {
            unvalidated::Event::Time(_) => unreachable!("not time"),
            unvalidated::Event::Signed(s) => s
                .verify_signature(
                    None,
                    &VerifyJwsOpts {
                        at_time: AtTime::SkipTimeChecks,
                        ..Default::default()
                    },
                )
                .await
                .expect("should be valid"),

            unvalidated::Event::Unsigned(_) => unreachable!("not unsigned"),
        }
    }

    const GITCOIN_INIT_EVENT_CAR: &str = "kgd10rrrmwc877zs8v0ve2mx4tlrjyzvn1uqfjv6s5ykd68yhmvr903ppohmfijme7elityjcccgmxil9fr3wsxscrw4pxalpp0rg40d8813jbtlmoh1amdvhy7v2a0rlmgov8s60pml43t6pixtq36rqimtevaa9ib14sa1iow4f73coib38z5ahdvgi0ugent21qg7axkemi3j75rkpdgeq0gkxhrgsid5f662curpk09k8u4faqg9dkhc7ftervjpvbt4045uifse5rmor2t6xvk41met9oe0k9hmvuzn258rt4392o000cecl2voa4vv453bm8vrwwqz2z33zkxzpv3jx3wq9s78kp2n19521jd3vtv4d4wigpwfoqu5qsfd39exhna208iev7x1ugld3qdvwbhnohfxtcnxymjcvvr877pctwi9npg3fpfkg4nub9lps27sb0g0mqrflctju7pg4bidh1erelku12loi2mdxj1iy7a8k97hnoqo0gfpwcmdysweky0io7q7z8coa1c32me95acye4cl5id7nemusrt7u8zjm5woo9s5o2j1pg2d7p5udbkgqadzh7q1k90ksr9rglb3ewwz1yszz9leto4x9fl07f5lvajabxmss24h3ju1g0wnxslgqolmv28cdfuk52l51mqg51ag40bscgdls1tgrvqimc53a023aan2atslh86ggr03nzro0waupp7ib4qoz2sed5ktjgr81jll07sohoqj5fdjp7gx7s6h4nkwbixbmhyeeuk8ju6oe7uv6s4y6c7lrwngkctakglgvqgsmnam8pox7h5wnemg9y1kubr3l1xe1tlmwnculcyr2xgt1av26okns1igiwuj6fanarunlwrmxt31miqykqtyudareyc2izop0t1cw77kx2ipwug932mfm73upuqiefts1mzdzyi72pt3rlcsojvizywi2z2llge81cpzs5p5x1w79t9qhhk0puhapr5pv3ibouzbau7s3n3bj7t77a5t32ik3sqt13pjqifhvggvguqu7kfwp73u64k26rd0vvei7emugww1v3c7443remjdardyom4drzxnmxy9crp7u6c6y0redhh5sl1zp47a0u0t5nuonb49wsr74yt9dmoudtp7i850il1bki660105ktun4p4bg8ocn2e7qdvqkgdcya4nestu0cxkozxfsczh22fc6v99izs6lsa0iwvz19ohgua79kjgprjhygrl7ghtsw9p8q9ci7znwvg5kj01k04yk8to9wcc6qq22lzzmdb3b82q0tkey73im42dw7v2x92tfa307m75s41t5f5qxy1wld0ch8kqdh64jc6tgcrdjnyvtor82rsqm2pq9mt3dqbximh3xbncvm5oxp02qjpgdbnjlj0hv5x0a94gp5vjf3oukgcmkhu0zss85z08zv3tp7h3i8nhhwg4dg8ho1zw1xsmpsshtnrt2xb9fy922twura7uqmt388qyeeuq3p874c07qwqoqa8mnq5fbsu3zb3q47xtreskrcd0yis3h28rsx85xyd1cqrsuchw7z8a9h82xn03bke27jn2lxhjgilfv3dy2753lpcan532jed2lee8prufvr7svlrw8aigu33qg6veayszqp6g3mhqpwvcdqvau6lgf4ns6014hsj7jvruauc15t5hd5g4vhvge5ds5pfnlrlq3uaurbgzw6nio491v03w5ew3jm2mi25ucrdo5j4pqgb4gv1rzpf08yfgzyt34wkj7brzsj33jl730upbarlyvydiy92jlfruuqp4djgrpoiwegw1f8ikomgpbo12s6o33midz1jtz8qg0dia2abl27bcuxz67pwdg6x64wj8k9mgxz2mym0r2ms4ox8eo2w1z1jiwfmon4xd53vzs7ytel5fxbksgm3xwcgvjd1l1lhfkm1hssz5jqkbhhjlzremwsnf463yn8vkb3ef788je032wybpvsz6xyxums8n9ecwfeeoqmc47l4sa5ksb8njtu2qvxfig6v2y17l51le9hcmpp2lmji3r2n3yjz8bop2x9j66rt0xc1iyouyohifj89jzz7xw3l6bssujogsuizujxddfd18l2yijb0b73e7w3pi0ww1wjo7iy0rxymfk0nx4nvisozqtmq0esjb6o8n2io9rqrftqfzx6j151abxl23ybmxfkvpeyea03itvwelfedofujjarunz7umhh9onxkc82ysn5lby9c7dunw9nozgsgobrt64jirpyts8zekkov87r9tbtp6usq44kvcxgdpzerynnifqls7aj3bjjpjbpt5y4556yp93tobxk90axw5rex3p2906utuuuqzg72ilxiiikv7v66ec2p8bu88cesvbjnbzj0in0qdslgtviqwibk3f7tlejrvqv72qlff8y2wro3zevjz4yep5tfb2lo1fcejgnic00uteujn7n6o7lkkcz74vqqpztj5wi60zotir6sgebvkxio5kfr4aj08xmxbxobl0ams67xkrt83hzowa6ale344np0krtl7opjzejkwx99s27dpt0ughizay6n9zhduqsbx82c95lt6kyrp8oix1ujebx84oguzxlwu3nbx2gqo642b44updmezhm9a4syubz53oo9dd4m7130tmezgc4mvpst1yafbvevken64l05n4f8j6yx0md1sgyopzpy1o262bc0whvnhs74ubp68al2c82slrc5ra1326yrj4ltqw6j8df0y2fdhpcd503b7opxi9c2kpo81hp00kfthyx7io56ehfgod0zbwwumj0jss94elgsl4bsfz6u83pv94n0917xiceok9fxvpclfuqjp19fguq54nuo31lbg9ftlw5kcuqjgd22jnp3falvoo4hemxvj3mu44hvio4x4bar214birejr8odzuwd7jgykqwjnootd5ugexjfmmiw2u8ot6elwkatvslbyzzmeb7dp4cbfxvwcj51az62sdh5durtbergcukqi2m3u47qpc1xqixhoj3rh8dp0w7bmrkdl1l5inztnwhbw0nmb4mve52ac89ruwqs0xjf77d6kir0bdril1com8jn6pows0o1sh13rd2e3cvipxfhweciaiy9dii1qgbtcrzky5dsmbz25ud1kjnsol7ae54iot7m48n0p4mqzjcj6zm319fbk6ob6v8kvf2xqxpas5u81qo1c649yj5k922m49s6bjg495c2paeoxv4i3dpnlf5ql6riua7pi038tg7odyf8xybvnsjogq82jzp7cjl303yqrg478vyz9dh6doe7x1ypw8r8xezp3629l7n0cebgvbhqnuhva7xbk0bkte2modiem2kp3pjhlo91gs5bvtvtsmidm8slzxadznzpf8ur4oe70fvodsdxfqe46lndd5v1jt1lp0lka50ikqxj9z48pwbvoesrg2vaanr489ife5n0soni8nmbixk1hk4zkqfxgcdfp7i0d0x41pifd55861swjw0q7oewcnsj54p5ve69v6wnujfvgxkzkg795pp625on2fosluqhtv4sv4epfgmmzbmqf4shky3ip498oz53pzhxnp0tg2l44cbzpsht1jsghxzjy8x3bdqspgk2dvo0lfovv4d9jynobijvlpoeamf3tucy9thcwzb89uw7q8mz4y4s5u78xcd7545my4t9tjw6czuoy9mttf6l91kbuhzla66vnguztwxmoz24fkvthbk1bqmcwcqkad63e3ws8prfuss6jns84a8o95tf2vlsxlp4wecvqujx2sj2mpr26h5wc0maty21dr1g9x9jdj4nm4slof1k2z0e8fzngwa9088lcau73q5n8y0o3filj66a4c3o4euknn93cfx67im6ed7hqvlckyapt0iuk6ocklboou3a16dm3fr3gyl3gd162vv4p8wazsvjqypbkrf66cn4xnz16b7zutpnkjs5k3hf16jy9pknf1gz68pr89b3atea6o40msfe0mbu1rqtboir2mfjl2yvv9uhaf92xr08fvt2uzxbnmfmj77e0am7seay6xbavlk1w58w3hbo32nh7jufexd0wk253g12x0vlmyyymurwpj5tjwsvhp8bd83kumfhhejrkxq2sk8jk1s3xisiybqt574c038vq5u4oprovyngymt";

    #[tokio::test]
    async fn gitcoin_cacao_car() {
        /*
        This event was created by getting the blocks and building the carfile, multibase encoding it and using it as a const

        ipfs dag get bagcqcera6zhhs3vt3vmkanmwj3gksysl3vk7y3uzr3wjsj4i5lbiyca5bhna --output-codec dag-jose > envelope.bytes
        ipfs dag get bafyreielo2n4ulcmw2ptrg7mvxauj7cx3t5xc535dchiuh4igwb4yozy6m --output-codec dag-cbor > payload.bytes
        ipfs dag get bafyreifgx54wr5byt4aqw2zinptjqbriuxemjvbfgmcckzl2xujndghzd4 --output-codec dag-cbor > cacao.bytes

        use ceramic_car::sync::{CarHeader, CarHeaderV1, CarWriter};
        use multihash_codetable::Code;

        let dag_cbor_code = 0x71;
        use multihash_codetable::MultihashDigest;

        let envelope = include_bytes!("envelope.bytes");
        let cacao = include_bytes!("cacao.bytes");
        let payload = include_bytes!("payload.bytes");

        let envelope_digest = Code::Sha2_256.digest(envelope);
        let envelope_cid = ceramic_core::Cid::new_v1(dag_cbor_code, envelope_digest);

        let payload_digest = Code::Sha2_256.digest(payload);
        let payload_cid = ceramic_core::Cid::new_v1(dag_cbor_code, payload_digest);

        let cacao_digest = Code::Sha2_256.digest(cacao);
        let cacao_cid = ceramic_core::Cid::new_v1(dag_cbor_code, cacao_digest);

        let header = CarHeader::V1(CarHeaderV1::from(vec![envelope_cid]));

        let mut buffer = Vec::new();
        let mut writer = CarWriter::new(header, &mut buffer);
        writer.write(envelope_cid, envelope).unwrap();
        writer.write(payload_cid, payload).unwrap();
        writer.write(cacao_cid, cacao).unwrap();
        writer.finish().unwrap();
        let content = multibase::encode(multibase::Base::Base36Lower, buffer);
        panic!("{}", content);

        */
        let (_, buffer) = multibase::decode(GITCOIN_INIT_EVENT_CAR).unwrap();
        let (_cid, event) =
            unvalidated::Event::<Ipld>::decode_car(std::io::Cursor::new(buffer), false).unwrap();

        match event {
            unvalidated::Event::Time(_) => unreachable!(),
            unvalidated::Event::Signed(event) => {
                let controller = match event.payload() {
                    unvalidated::Payload::Data(_) => unreachable!(),
                    unvalidated::Payload::Init(payload) => {
                        payload.header().controllers().first().unwrap()
                    }
                };
                assert_eq!(
                    controller,
                    "did:pkh:eip155:1:0xc1e722551c7eac8675903f95f1330a2fe6ad34ea"
                );
                let opts = VerifyJwsOpts {
                    at_time: AtTime::At(Some(
                        chrono::DateTime::parse_from_rfc3339("2024-12-20T20:27:13.330Z")
                            .unwrap()
                            .to_utc(),
                    )),
                    revocation_phaseout_secs: chrono::Duration::seconds(0),
                };
                event
                    .verify_signature(Some(controller), &opts)
                    .await
                    .expect("event should be valid");
            }
            unvalidated::Event::Unsigned(_) => unreachable!(),
        }
    }

    #[tokio::test]
    /// The event used to verify in js-did. Demonstrates how you can parse the dag-json envelope and cacao to verify the signatureca
    async fn gitcoin_cacao_from_pieces() {
        // dag-json event
        let event = serde_json::json!({
            "link": {
                "/": "bafyreiezmymvqjtss5emo4uuj7emmoxnhpm3kjukzodahp5rqij5xsreo4"
            },
            "payload": "AXESIJlmGVgmcpdIx3KUT8jGOu072bUmisuGA7-xghPbyiR3",
            "signatures": [
                {
                "protected": "eyJhbGciOiJFUzI1NiIsImNhcCI6ImlwZnM6Ly9iYWZ5cmVpYWdvNWQ2NzRsaWxsc2piZHp2bXNidDNva3dhcmdqZ2VrY2V2c3U3d3h1endmY3h6NDd4YSIsImtpZCI6ImRpZDprZXk6ekRuYWVVM3o2d0pLWWN4ZGVRZE5CMWI2YmduVVpCdE55NnU4VzdBVDNyVlBkd2oyZiN6RG5hZVUzejZ3SktZY3hkZVFkTkIxYjZiZ25VWkJ0Tnk2dThXN0FUM3JWUGR3ajJmIn0",
                "signature": "F4V-EpyUujZGIrgx0bify8AWMgAVviNFMB5vHW7oxEk0OyFsl4UKmmzLKb2bN0GnUJi0rrkUy8FmRFqgPgEWQQ"
                }
            ]
        });
        let event = event.as_object().unwrap();
        // bafyreiago5d674lillsjbdzvmsbt3okwargjgekcevsu7wxuzwfcxz47xa from the protected data
        let cacao = r#"{
            "h": {
              "t": "eip4361"
            },
            "p": {
              "aud": "did:key:zDnaeU3z6wJKYcxdeQdNB1b6bgnUZBtNy6u8W7AT3rVPdwj2f",
              "domain": "app.passport.xyz",
              "exp": "2024-12-24T20:27:13.330Z",
              "iat": "2024-12-17T20:27:13.330Z",
              "iss": "did:pkh:eip155:1:0xeeec6dcedfe42e5ff150d0165c816106c4633ac2",
              "nonce": "c9M7ErwgxK",
              "resources": [
                "ceramic://*"
              ],
              "statement": "Give this application access to some of your data on Ceramic",
              "version": "1"
            },
            "s": {
              "s": "0xc12517c95db32c01b72ef84797172a7c4bff94f42d52e8deddb6f3626d2f74055677cb3b19c3fe70d866a52a22e69ae4874bfe598a1fa0f5c6145a19049628e61b",
              "t": "eip191"
            }
          }"#;

        let opts = VerifyJwsOpts {
            at_time: AtTime::At(Some(
                chrono::DateTime::parse_from_rfc3339("2024-12-20T20:27:13.330Z")
                    .unwrap()
                    .to_utc(),
            )),
            revocation_phaseout_secs: chrono::Duration::seconds(0),
        };

        let payload = event
            .get("payload")
            .unwrap()
            .as_str()
            .map(|p| {
                base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(p)
                    .unwrap()
            })
            .unwrap();
        let signatures = event
            .get("signatures")
            .unwrap()
            .as_array()
            .unwrap()
            .first()
            .unwrap()
            .as_object()
            .unwrap();
        let protected = signatures
            .get("protected")
            .unwrap()
            .as_str()
            .map(|p| {
                base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(p)
                    .unwrap()
            })
            .unwrap();
        let protected = protected.as_slice();
        let signature = signatures
            .get("signature")
            .unwrap()
            .as_str()
            .map(|s| {
                base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(s)
                    .unwrap()
            })
            .unwrap();
        let signature = signature.as_slice();

        let cacao: Capability = serde_json::from_str(cacao).unwrap();
        let jws_header: Header = serde_json::from_slice(protected).unwrap();

        let to_verify = SignatureData {
            jws_header,
            signature,
            protected,
            payload: &payload,
            capability: Some(&cacao),
        };
        to_verify
            .verify_signature(
                Some("did:pkh:eip155:1:0xeeec6dcedfe42e5ff150d0165c816106c4633ac2"),
                &opts,
            )
            .await
            .unwrap()
    }
}
