use serde_json::json;

/// decoded protected bytes for SIGNED_EVENT : 
// /// {
//   "alg": "EdDSA",
//   "cap": "",
//   "kid": "did:key:z6Mkr4a3Z3FFaJF8YqWnWnVHqd5eDjZN9bDST6wVZC1hJ81P#z6Mkr4a3Z3FFaJF8YqWnWnVHqd5eDjZN9bDST6wVZC1hJ81P"
// }

pub const SIGNED_INIT_EVENT_PAYLOAD_NO_CACAO: &str = r#"
{
    "Signed": {
        "Event": {
            "envelope": {
                "Envelope": {
                    "payload": "f01711220117096234107d7341c4a5e7b494291a28d16c212fa9cca3c90b29fbe035a1b43",
                    "signatures": [
                        {
                            "header": null,
                            "protected": {
                                "Bytes": "f7b22616c67223a224564445341222c226b6964223a226469643a6b65793a7a364d6b7442796e41504c7245796553377056746862697953636d6675386e355637626f586778796f357133535a5252237a364d6b7442796e41504c7245796553377056746862697953636d6675386e355637626f586778796f357133535a5252227d"
                            },
                            "signature": {
                                "Bytes": "f24038e5c7c7d3f2b6d3478d3cf07b4e136ecb24c737f034ae7f5fd7b4ad78843a7543abe11ea9ee8a86eaa7912a34ea74bcd1420b3c201be0ec8e67acf63270c"
                            }
                        }
                    ]
                },
                "envelope_cid": "bagcqcerar2aga7747dm6fota3iipogz4q55gkaamcx2weebs6emvtvie2oha",
                "payload": {
                    "Init": {
                        "Payload": {
                            "header": {
                                "Header": {
                                    "controllers": [
                                        "did:key:z6MktBynAPLrEyeS7pVthbiyScmfu8n5V7boXgxyo5q3SZRR"
                                    ],
                                    "sep": "model",
                                    "model": {
                                        "Bytes": "fce01020185011220a0e832a335e38bd340b43a0f0299bf3c515a0fc33de4cd7910a43e8b931e8d58"
                                    },
                                    "should_index": null,
                                    "unique": {
                                        "Bytes": "f44a6f13ab2410bbb6a8568de"
                                    },
                                    "context": null
                                }
                            },
                            "data": {
                                "steph": 333
                            }
                        }
                    }
                },
                "payload_cid": "bafyreiaroclcgqih242byss6pneufencrulmeex2ttfdzefst67agwq3im",
                "capability": null
            }
        }
    }
}
"#;

/// decoded protected bytes for SIGNED_EVENT : 
// /// {
//   "alg": "EdDSA",
//   "cap": "ipfs://bafyreifu7xdhhlntwnrd7jxpps44wz6acamfcsxazc4oopnmldaotqok2y",
//   "kid": "did🔑z6Mkr4a3Z3FFaJF8YqWnWnVHqd5eDjZN9bDST6wVZC1hJ81P#z6Mkr4a3Z3FFaJF8YqWnWnVHqd5eDjZN9bDST6wVZC1hJ81P"
// }
pub const SIGNED_EVENT: &str = r#"
Signed(Event {
    envelope: Envelope {
        payload: Bytes("f0171122074a4963224e7f63e650cce31de1c2697f444c803400a9c75b06f597409f132fc"),
        signatures: [
            Signature {
                header: None,
                protected: Some(Bytes("f7b22616c67223a224564445341222c22636170223a22697066733a2f2f62616679726569667537786468686c6e74776e7264376a787070733434777a366163616d66637378617a63346f6f706e6d6c64616f74716f6b3279222c226b6964223a226469643a6b65793a7a364d6b723461335a334646614a46385971576e576e564871643565446a5a4e39624453543677565a4331684a383150237a364d6b723461335a334646614a46385971576e576e564871643565446a5a4e39624453543677565a4331684a383150227d")),
                signature: Bytes("f75f6a153d954d8dc3fcb4fc318bd58b47cae2d7d50a0077b4c675f193c6f93738092c66f91f90eaee7b3045165d47f5741cc1534975527de5c6eac3952b9e203")
            }
        ]
    },
    envelope_cid: "bagcqcera3v3fugov6gg5umi4lfrkkevfuo4jsjbhnrwejjpaxsgmwjlxbp5a",
    payload: Init(Payload {
        header: Header {
            controllers: ["did:pkh:eip155:1:0x3794d4f077c08d925ff8ff820006b7353299b200"],
            sep: Some("model"),
            model: Bytes("fce010201850112200613aea7af175aa87519e6805537a16e94f9d78b1ff07bd31da552891d2478b8"),
            should_index: None,
            unique: Some(Bytes("fc706b64c11e00baebfd55f71")),
            context: None
        },
        data: Some({"step": 600})
    }),
    payload_cid: "bafyreiduusldejhh6y7gkdgoghpbyjux6rcmqa2abkohlmdplf2at4js7q",
    capability: Some((
        "bafyreifu7xdhhlntwnrd7jxpps44wz6acamfcsxazc4oopnmldaotqok2y",
        Capability {
            header: Header { type: EIP4361 },
            payload: Payload {
                audience: "did:key:z6Mkr4a3Z3FFaJF8YqWnWnVHqd5eDjZN9bDST6wVZC1hJ81P",
                domain: "test",
                expiration: Some(2024-06-19T20:04:42.464Z),
                issued_at: 2024-06-12T20:04:42.464Z,
                issuer: "did:pkh:eip155:1:0x3794d4f077c08d925ff8ff820006b7353299b200",
                not_before: None,
                nonce: "wPiCOcpkll",
                request_id: None,
                resources: Some(["ceramic://*"]),
                statement: Some("Give this application access to some of your data on Ceramic"),
                version: "1"
            },
            signature: Signature {
                metadata: None,
                type: EIP191,
                signature: "0xb266999263446ddb9bf588825e9ac08b545e655f6077e8d8579a8d6639c1167c56f7dae7ac70f7faed8c141af9e124a7eb4f77423a572b36144ada8ef2206cda1c"
            }
        }
    ))
})
"#;

/// Internal signature check=verify signature for the capability , then issuer = controller, envelope is correctly signed by which key?




