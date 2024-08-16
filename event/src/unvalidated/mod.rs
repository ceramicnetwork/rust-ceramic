mod builder;
mod event;
mod payload;

pub mod signed;

pub use builder::*;
pub use event::*;
pub use payload::*;

use cid::Cid;
use multihash_codetable::{Code, MultihashDigest};

fn cid_from_dag_jose(data: &[u8]) -> Cid {
    Cid::new_v1(
        0x85, // TODO use constant for DagJose codec
        Code::Sha2_256.digest(data),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use expect_test::expect;
    use serde::{Deserialize, Serialize};

    use ceramic_core::SerdeIpld;

    pub const SIGNED_INIT_EVENT_CID: &str =
        "bagcqcerar2aga7747dm6fota3iipogz4q55gkaamcx2weebs6emvtvie2oha";
    pub const SIGNED_INIT_EVENT_CAR: &str = "uO6Jlcm9vdHOB2CpYJgABhQESII6AYH_8-NniumDaEPcbPId6ZQAMFfViEDLxGVnVBNOOZ3ZlcnNpb24B0QEBcRIgEXCWI0EH1zQcSl57SUKRoo0WwhL6nMo8kLKfvgNaG0OiZGRhdGGhZXN0ZXBoGQFNZmhlYWRlcqRjc2VwZW1vZGVsZW1vZGVsWCjOAQIBhQESIKDoMqM144vTQLQ6DwKZvzxRWg_DPeTNeRCkPouTHo1YZnVuaXF1ZUxEpvE6skELu2qFaN5rY29udHJvbGxlcnOBeDhkaWQ6a2V5Ono2TWt0QnluQVBMckV5ZVM3cFZ0aGJpeVNjbWZ1OG41Vjdib1hneHlvNXEzU1pSUroCAYUBEiCOgGB__PjZ4rpg2hD3GzyHemUADBX1YhAy8RlZ1QTTjqJncGF5bG9hZFgkAXESIBFwliNBB9c0HEpee0lCkaKNFsIS-pzKPJCyn74DWhtDanNpZ25hdHVyZXOBomlwcm90ZWN0ZWRYgXsiYWxnIjoiRWREU0EiLCJraWQiOiJkaWQ6a2V5Ono2TWt0QnluQVBMckV5ZVM3cFZ0aGJpeVNjbWZ1OG41Vjdib1hneHlvNXEzU1pSUiN6Nk1rdEJ5bkFQTHJFeWVTN3BWdGhiaXlTY21mdThuNVY3Ym9YZ3h5bzVxM1NaUlIifWlzaWduYXR1cmVYQCQDjlx8fT8rbTR4088HtOE27LJMc38DSuf1_XtK14hDp1Q6vhHqnuiobqp5EqNOp0vNFCCzwgG-Dsjmes9jJww";

    pub const SIGNED_INIT_EVENT: &str = "uomdwYXlsb2FkWCQBcRIgEXCWI0EH1zQcSl57SUKRoo0WwhL6nMo8kLKfvgNaG0Nqc2lnbmF0dXJlc4GiaXByb3RlY3RlZFiBeyJhbGciOiJFZERTQSIsImtpZCI6ImRpZDprZXk6ejZNa3RCeW5BUExyRXllUzdwVnRoYml5U2NtZnU4bjVWN2JvWGd4eW81cTNTWlJSI3o2TWt0QnluQVBMckV5ZVM3cFZ0aGJpeVNjbWZ1OG41Vjdib1hneHlvNXEzU1pSUiJ9aXNpZ25hdHVyZVhAJAOOXHx9PyttNHjTzwe04TbsskxzfwNK5_X9e0rXiEOnVDq-Eeqe6KhuqnkSo06nS80UILPCAb4OyOZ6z2MnDA";
    pub const SIGNED_INIT_EVENT_PAYLOAD: &str = "uomRkYXRhoWVzdGVwaBkBTWZoZWFkZXKkY3NlcGVtb2RlbGVtb2RlbFgozgECAYUBEiCg6DKjNeOL00C0Og8Cmb88UVoPwz3kzXkQpD6Lkx6NWGZ1bmlxdWVMRKbxOrJBC7tqhWjea2NvbnRyb2xsZXJzgXg4ZGlkOmtleTp6Nk1rdEJ5bkFQTHJFeWVTN3BWdGhiaXlTY21mdThuNVY3Ym9YZ3h5bzVxM1NaUlI";

    pub const UNSIGNED_INIT_EVENT_CAR: &str = "uOqJlcm9vdHOB2CpYJQABcRIgCkMGCgfs8ht9NWnDxnqenauyk-FwopBeHTefuwaqvmZndmVyc2lvbgHDAQFxEiAKQwYKB-zyG301acPGep6dq7KT4XCikF4dN5-7Bqq-ZqJkZGF0YfZmaGVhZGVypGNzZXBlbW9kZWxlbW9kZWxYKM4BAgGFARIghHTHRYxxeQXgc9Q6LUJVelzW5bnrw9TWgoBJlBIOVtdmdW5pcXVlR2Zvb3xiYXJrY29udHJvbGxlcnOBeDhkaWQ6a2V5Ono2TWt0Q0ZSY3dMUkZRQTlXYmVEUk03VzdrYkJkWlRIUTJ4blBneXhaTHExZ0NwSw";
    // Data Event for a stream with a signed init event
    pub const SIGNED_DATA_EVENT_CAR: &str = "uO6Jlcm9vdHOB2CpYJgABhQESICddBxl5Sk2e7I20pzX9kDLf0jj6WvIQ1KqbM3WQiClDZ3ZlcnNpb24BqAEBcRIgdtssXEgR7sXQQQA1doBpxUpTn4pcAaVFZfQjyo-03SGjYmlk2CpYJgABhQESII6AYH_8-NniumDaEPcbPId6ZQAMFfViEDLxGVnVBNOOZGRhdGGBo2JvcGdyZXBsYWNlZHBhdGhmL3N0ZXBoZXZhbHVlGQFOZHByZXbYKlgmAAGFARIgjoBgf_z42eK6YNoQ9xs8h3plAAwV9WIQMvEZWdUE0466AgGFARIgJ10HGXlKTZ7sjbSnNf2QMt_SOPpa8hDUqpszdZCIKUOiZ3BheWxvYWRYJAFxEiB22yxcSBHuxdBBADV2gGnFSlOfilwBpUVl9CPKj7TdIWpzaWduYXR1cmVzgaJpcHJvdGVjdGVkWIF7ImFsZyI6IkVkRFNBIiwia2lkIjoiZGlkOmtleTp6Nk1rdEJ5bkFQTHJFeWVTN3BWdGhiaXlTY21mdThuNVY3Ym9YZ3h5bzVxM1NaUlIjejZNa3RCeW5BUExyRXllUzdwVnRoYml5U2NtZnU4bjVWN2JvWGd4eW81cTNTWlJSIn1pc2lnbmF0dXJlWECym-Kwb5ti-T5dCygt4zf8Lr6MescAbkk_DILoy3fFjYG8fZVUCGKDQiTTHbNbzOk1yze7-2hA3AKdBfzJY1kA";

    pub const DATA_EVENT_PAYLOAD: &str = "uo2JpZNgqWCYAAYUBEiCOgGB__PjZ4rpg2hD3GzyHemUADBX1YhAy8RlZ1QTTjmRkYXRhgaNib3BncmVwbGFjZWRwYXRoZi9zdGVwaGV2YWx1ZRkBTmRwcmV22CpYJgABhQESII6AYH_8-NniumDaEPcbPId6ZQAMFfViEDLxGVnVBNOO";

    // Data Event for a stream with an unsigned init event
    pub const DATA_EVENT_CAR_UNSIGNED_INIT: &str = "uO6Jlcm9vdHOB2CpYJgABhQESIAlT-MndVmni9jiwS6JPtXtvYAa1-4tjruqLftM6BxvTZ3ZlcnNpb24B-gEBcRIguZ-ORAzcRLjL2LKcFJX2lC3Cv_4bywuG4Q8gEc5dbYajYmlk2CpYJQABcRIgCkMGCgfs8ht9NWnDxnqenauyk-FwopBeHTefuwaqvmZkZGF0YYSjYm9wY2FkZGRwYXRoZC9vbmVldmFsdWVjZm9vo2JvcGNhZGRkcGF0aGQvdHdvZXZhbHVlY2JhcqNib3BjYWRkZHBhdGhmL3RocmVlZXZhbHVlZmZvb2JhcqNib3BjYWRkZHBhdGhnL215RGF0YWV2YWx1ZQFkcHJldtgqWCUAAXESIApDBgoH7PIbfTVpw8Z6np2rspPhcKKQXh03n7sGqr5mugIBhQESIAlT-MndVmni9jiwS6JPtXtvYAa1-4tjruqLftM6BxvTomdwYXlsb2FkWCQBcRIguZ-ORAzcRLjL2LKcFJX2lC3Cv_4bywuG4Q8gEc5dbYZqc2lnbmF0dXJlc4GiaXByb3RlY3RlZFiBeyJhbGciOiJFZERTQSIsImtpZCI6ImRpZDprZXk6ejZNa3RDRlJjd0xSRlFBOVdiZURSTTdXN2tiQmRaVEhRMnhuUGd5eFpMcTFnQ3BLI3o2TWt0Q0ZSY3dMUkZRQTlXYmVEUk03VzdrYkJkWlRIUTJ4blBneXhaTHExZ0NwSyJ9aXNpZ25hdHVyZVhAZSJEw5QkFrYhbLYdLgnBn5SIbGAgm5i2jHhntWwe8nDkyKcCu4OvLMvFyGpjPloYVOr0JKwXlQfbgccHtbJpDw";

    // Data event signed with a CACAO
    pub const CACAO_SIGNED_DATA_EVENT_CAR:&str="mO6Jlcm9vdHOB2CpYJgABhQESIN12WhnV8Y3aMRxZYqUSpaO4mSQnbGxEpeC8jMsldwv6Z3ZlcnNpb24BigQBcRIgtP3Gc62zs2I/pu98uctnwBAYUUrgyLjnPaxYwOnBytajYWihYXRnZWlwNDM2MWFwqWNhdWR4OGRpZDprZXk6ejZNa3I0YTNaM0ZGYUpGOFlxV25XblZIcWQ1ZURqWk45YkRTVDZ3VlpDMWhKODFQY2V4cHgYMjAyNC0wNi0xOVQyMDowNDo0Mi40NjRaY2lhdHgYMjAyNC0wNi0xMlQyMDowNDo0Mi40NjRaY2lzc3g7ZGlkOnBraDplaXAxNTU6MToweDM3OTRkNGYwNzdjMDhkOTI1ZmY4ZmY4MjAwMDZiNzM1MzI5OWIyMDBlbm9uY2Vqd1BpQ09jcGtsbGZkb21haW5kdGVzdGd2ZXJzaW9uYTFpcmVzb3VyY2VzgWtjZXJhbWljOi8vKmlzdGF0ZW1lbnR4PEdpdmUgdGhpcyBhcHBsaWNhdGlvbiBhY2Nlc3MgdG8gc29tZSBvZiB5b3VyIGRhdGEgb24gQ2VyYW1pY2FzomFzeIQweGIyNjY5OTkyNjM0NDZkZGI5YmY1ODg4MjVlOWFjMDhiNTQ1ZTY1NWY2MDc3ZThkODU3OWE4ZDY2MzljMTE2N2M1NmY3ZGFlN2FjNzBmN2ZhZWQ4YzE0MWFmOWUxMjRhN2ViNGY3NzQyM2E1NzJiMzYxNDRhZGE4ZWYyMjA2Y2RhMWNhdGZlaXAxOTHTAQFxEiB0pJYyJOf2PmUMzjHeHCaX9ETIA0AKnHWwb1l0CfEy/KJkZGF0YaFkc3RlcBkCWGZoZWFkZXKkY3NlcGVtb2RlbGVtb2RlbFgozgECAYUBEiAGE66nrxdaqHUZ5oBVN6FulPnXix/we9MdpVKJHSR4uGZ1bmlxdWVMxwa2TBHgC66/1V9xa2NvbnRyb2xsZXJzgXg7ZGlkOnBraDplaXAxNTU6MToweDM3OTRkNGYwNzdjMDhkOTI1ZmY4ZmY4MjAwMDZiNzM1MzI5OWIyMDCFAwGFARIg3XZaGdXxjdoxHFlipRKlo7iZJCdsbESl4LyMyyV3C/qiZ3BheWxvYWRYJAFxEiB0pJYyJOf2PmUMzjHeHCaX9ETIA0AKnHWwb1l0CfEy/GpzaWduYXR1cmVzgaJpcHJvdGVjdGVkWMx7ImFsZyI6IkVkRFNBIiwiY2FwIjoiaXBmczovL2JhZnlyZWlmdTd4ZGhobG50d25yZDdqeHBwczQ0d3o2YWNhbWZjc3hhemM0b29wbm1sZGFvdHFvazJ5Iiwia2lkIjoiZGlkOmtleTp6Nk1rcjRhM1ozRkZhSkY4WXFXblduVkhxZDVlRGpaTjliRFNUNndWWkMxaEo4MVAjejZNa3I0YTNaM0ZGYUpGOFlxV25XblZIcWQ1ZURqWk45YkRTVDZ3VlpDMWhKODFQIn1pc2lnbmF0dXJlWEB19qFT2VTY3D/LT8MYvVi0fK4tfVCgB3tMZ18ZPG+Tc4CSxm+R+Q6u57MEUWXUf1dBzBU0l1Un3lxurDlSueID";

    pub const TIME_EVENT_CAR: &str = "uOqJlcm9vdHOB2CpYJQABcRIgcmqgb7eHSgQ32hS1NGVKZruLJGcKDI1f4lqOyNYn3eVndmVyc2lvbgG3AQFxEiByaqBvt4dKBDfaFLU0ZUpmu4skZwoMjV_iWo7I1ifd5aRiaWTYKlgmAAGFARIgjoBgf_z42eK6YNoQ9xs8h3plAAwV9WIQMvEZWdUE045kcGF0aGEwZHByZXbYKlgmAAGFARIgJ10HGXlKTZ7sjbSnNf2QMt_SOPpa8hDUqpszdZCIKUNlcHJvb2bYKlglAAFxEiAFKLx3fi7-yD1aPNyqnblI_r_5XllReVz55jBMvMxs9q4BAXESIAUovHd-Lv7IPVo83KqduUj-v_leWVF5XPnmMEy8zGz2pGRyb2902CpYJQABcRIgfWtbF-FQN6GN6ZL8OtHvp2YrGlmLbZwkOl6UY-3AUNFmdHhIYXNo2CpYJgABkwEbIBv-WU6fLnsyo5_lDSTC_T-xUlW95brOAUDByGHJzbCRZnR4VHlwZWpmKGJ5dGVzMzIpZ2NoYWluSWRvZWlwMTU1OjExMTU1MTExeQFxEiB9a1sX4VA3oY3pkvw60e-nZisaWYttnCQ6XpRj7cBQ0YPYKlgmAAGFARIgJ10HGXlKTZ7sjbSnNf2QMt_SOPpa8hDUqpszdZCIKUP22CpYJQABcRIgqVOMo-IVjo08Mk0cim3Z8flNyHY7c9g7uGMqeS0PFHA";

    pub const UNSIGNED_INIT_NO_SEP_CAR:&str="uOqJlcm9vdHOB2CpYJQABcRIgrY2L_wTWrzng7Mpf2kvh9Q9Uyz-Ei2CF7NfjzRD0illndmVyc2lvbgG5AwFxEiCtjYv_BNavOeDsyl_aS-H1D1TLP4SLYIXs1-PNEPSKWaJkZGF0YaZkZGF0YaNjdXJsYGVsYWJlbGdGYXN0aW5nbmNoaWxkcmVuSGlkZGVu9GR0eXBlbFF1ZXN0aW9uTm9kZWdjcmVhdGVkeBgyMDIzLTAyLTIwVDE1OjE5OjM2LjI3OVpocG9zaXRpb26iYXj7QKWDkiAAAABheftAtqeAAAAAAGlsYXRlcmFsSUR4JDY5YWFmMzdkLTU1OWItNGI5NS1hMTAwLTVlZTk4MThmYzVlZGlwcm9qZWN0SUR4P2tqemw2a2N5bTd3OHk1ZDY1Zjlva240cml5ZGF0OTJoM3M2dnZ6cHd3dTc1NDRpOTJmanlnYzU2N2x6aHJ2Y2ZoZWFkZXKjZW1vZGVsWCjOAQIBhQESIJbss9X3kzfag-eF6OFx0KDIOV6P5DxOi-cWAWnTN5FjZnVuaXF1ZUwq2_OJxYVaSoawmJtrY29udHJvbGxlcnOBeDtkaWQ6cGtoOmVpcDE1NToxOjB4Y2E1ZmY0YjM0NDJmY2FjMjdlMWFmNDQ1N2UwMmViNjI5YzcxMjk4Mw";

    #[derive(Serialize, Deserialize)]
    struct TestStruct {
        a: u32,
        b: String,
        c: Vec<u8>,
        d: Option<u32>,
        e: Option<u32>,
        f: bool,
        g: BTreeMap<String, u32>,
    }

    #[test]
    fn test_derive_serde_ipld() {
        let test_struct = TestStruct {
            a: 42,
            b: "hello".to_string(),
            c: vec![1, 2, 3],
            d: Some(42),
            e: None,
            f: true,
            g: BTreeMap::from([
                ("a".to_owned(), 1),
                ("b".to_owned(), 2),
                ("c".to_owned(), 3),
            ]),
        };
        expect![[
            r#"{"a":42,"b":"hello","c":[1,2,3],"d":42,"e":null,"f":true,"g":{"a":1,"b":2,"c":3}}"#
        ]]
        .assert_eq(&test_struct.to_json().unwrap());
        expect![
            "a76161182a61626568656c6c6f6163830102036164182a6165f66166f56167a3616101616202616303"
        ]
        .assert_eq(&hex::encode(test_struct.to_cbor().unwrap()));
        expect!["bafyreig5jqfc6u6rrotcohh42jvrcnyracgpasd6lnbkbjottvw4uwxs3e"]
            .assert_eq(&test_struct.to_cid().unwrap().to_string());
    }
}
