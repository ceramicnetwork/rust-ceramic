use crate::unvalidated;
use cid::Cid;
use ipld_core::ipld::Ipld;

/// Builder for construct events.
pub struct Builder;

impl Builder {
    /// Create builder for init events
    #[allow(private_interfaces)]
    pub fn init() -> InitBuilder<InitBuilderEmpty> {
        InitBuilder {
            state: InitBuilderEmpty,
        }
    }

    /// Create builder for data events
    #[allow(private_interfaces)]
    pub fn data() -> DataBuilder<DataBuilderEmpty> {
        DataBuilder {
            state: DataBuilderEmpty,
        }
    }
}

struct Separator {
    key: String,
    value: Vec<u8>,
}

/// Builder for constructing an [`unvalidated::init::Payload`].
#[derive(Default)]
#[allow(private_bounds)]
pub struct InitBuilder<S: InitBuilderState> {
    state: S,
}

trait InitBuilderState {}

struct InitBuilderEmpty;
impl InitBuilderState for InitBuilderEmpty {}

struct InitBuilderWithController {
    controller: String,
}
impl InitBuilderState for InitBuilderWithController {}
impl InitBuilder<InitBuilderEmpty> {
    /// Specify the controller.
    pub fn with_controller(self, controller: String) -> InitBuilder<InitBuilderWithController> {
        InitBuilder {
            state: InitBuilderWithController { controller },
        }
    }
}

struct InitBuilderWithSep {
    controller: String,
    sep: Separator,
    unique: Option<Vec<u8>>,
    data: Option<Ipld>,
    should_index: Option<bool>,
}
impl InitBuilderState for InitBuilderWithSep {}
impl InitBuilder<InitBuilderWithController> {
    /// Specify the separator key and value.
    pub fn with_sep(self, key: String, value: Vec<u8>) -> InitBuilder<InitBuilderWithSep> {
        InitBuilder {
            state: InitBuilderWithSep {
                controller: self.state.controller,
                sep: Separator { key, value },
                unique: None,
                data: None,
                should_index: None,
            },
        }
    }
}

impl InitBuilder<InitBuilderWithSep> {
    /// Specify the unique bytes.
    pub fn with_unique(mut self, unique: Vec<u8>) -> Self {
        self.state.unique = Some(unique);
        self
    }

    /// Specify the should index value.
    pub fn with_should_index(mut self, should_index: bool) -> Self {
        self.state.should_index = Some(should_index);
        self
    }

    /// Specify the data.
    pub fn with_data(mut self, data: Ipld) -> Self {
        self.state.data = Some(data);
        self
    }
    /// Build the event.
    pub fn build(self) -> unvalidated::init::Payload<Ipld> {
        let header = unvalidated::init::Header::new(
            vec![self.state.controller],
            self.state.sep.key,
            self.state.sep.value,
            self.state.should_index,
            self.state.unique,
        );
        let payload = unvalidated::init::Payload::new(header, self.state.data);
        payload
    }
}

/// Builder for constructing an [`unvalidated::data::Payload`].
#[derive(Default)]
#[allow(private_bounds)]
pub struct DataBuilder<S: crate::unvalidated::builder::DataBuilderState> {
    state: S,
}

trait DataBuilderState {}

struct DataBuilderEmpty;
impl crate::unvalidated::builder::DataBuilderState
    for crate::unvalidated::builder::DataBuilderEmpty
{
}

struct DataBuilderWithId {
    id: Cid,
}
impl DataBuilderState for DataBuilderWithId {}
impl DataBuilder<DataBuilderEmpty> {
    /// Specify the id.
    pub fn with_id(self, id: Cid) -> DataBuilder<DataBuilderWithId> {
        DataBuilder {
            state: DataBuilderWithId { id },
        }
    }
}

struct DataBuilderWithPrev {
    id: Cid,
    prev: Cid,
}
impl DataBuilderState for crate::unvalidated::builder::DataBuilderWithPrev {}
impl DataBuilder<DataBuilderWithId> {
    /// Specify the prev.
    pub fn with_prev(
        self,
        prev: Cid,
    ) -> DataBuilder<crate::unvalidated::builder::DataBuilderWithPrev> {
        DataBuilder {
            state: crate::unvalidated::builder::DataBuilderWithPrev {
                id: self.state.id,
                prev,
            },
        }
    }
}

struct DataBuilderWithData {
    id: Cid,
    prev: Cid,
    data: Ipld,
    should_index: Option<bool>,
}
impl DataBuilderState for crate::unvalidated::builder::DataBuilderWithData {}
impl DataBuilder<DataBuilderWithPrev> {
    /// Specify the data.
    pub fn with_data(
        self,
        data: Ipld,
    ) -> DataBuilder<crate::unvalidated::builder::DataBuilderWithData> {
        DataBuilder {
            state: crate::unvalidated::builder::DataBuilderWithData {
                id: self.state.id,
                prev: self.state.prev,
                data,
                should_index: None,
            },
        }
    }
}

impl DataBuilder<DataBuilderWithData> {
    /// Specify should_index.
    pub fn with_should_index(mut self, should_index: bool) -> Self {
        self.state.should_index = Some(should_index);
        self
    }

    /// Build the event.
    pub fn build(self) -> unvalidated::data::Payload<Ipld> {
        let header = self
            .state
            .should_index
            .map(|si| unvalidated::data::Header::new(Some(si)));
        let payload = unvalidated::data::Payload::new(
            self.state.id,
            self.state.prev,
            header,
            self.state.data,
        );
        payload
    }
}

#[cfg(test)]
mod tests {
    use ceramic_core::StreamId;
    use expect_test::expect;
    use multibase;

    use super::*;
    use cid::Cid;
    use std::str::FromStr;

    const SIGNED_INIT_EVENT_CID: &str =
        "bagcqcerar2aga7747dm6fota3iipogz4q55gkaamcx2weebs6emvtvie2oha";
    const SIGNED_INIT_EVENT_PAYLOAD_CID: &str =
        "bafyreiaroclcgqih242byss6pneufencrulmeex2ttfdzefst67agwq3im";
    const SIGNED_INIT_EVENT_CAR: &str = "
        uO6Jlcm9vdHOB2CpYJgABhQESII6AYH_8-NniumDaEPcbPId6ZQAMFfViEDLxGVnVBNOOZ3ZlcnNpb24
        B0QEBcRIgEXCWI0EH1zQcSl57SUKRoo0WwhL6nMo8kLKfvgNaG0OiZGRhdGGhZXN0ZXBoGQFNZmhlYWR
        lcqRjc2VwZW1vZGVsZW1vZGVsWCjOAQIBhQESIKDoMqM144vTQLQ6DwKZvzxRWg_DPeTNeRCkPouTHo1
        YZnVuaXF1ZUxEpvE6skELu2qFaN5rY29udHJvbGxlcnOBeDhkaWQ6a2V5Ono2TWt0QnluQVBMckV5ZVM
        3cFZ0aGJpeVNjbWZ1OG41Vjdib1hneHlvNXEzU1pSUroCAYUBEiCOgGB__PjZ4rpg2hD3GzyHemUADBX
        1YhAy8RlZ1QTTjqJncGF5bG9hZFgkAXESIBFwliNBB9c0HEpee0lCkaKNFsIS-pzKPJCyn74DWhtDanN
        pZ25hdHVyZXOBomlwcm90ZWN0ZWRYgXsiYWxnIjoiRWREU0EiLCJraWQiOiJkaWQ6a2V5Ono2TWt0Qnl
        uQVBMckV5ZVM3cFZ0aGJpeVNjbWZ1OG41Vjdib1hneHlvNXEzU1pSUiN6Nk1rdEJ5bkFQTHJFeWVTN3B
        WdGhiaXlTY21mdThuNVY3Ym9YZ3h5bzVxM1NaUlIifWlzaWduYXR1cmVYQCQDjlx8fT8rbTR4088HtOE
        27LJMc38DSuf1_XtK14hDp1Q6vhHqnuiobqp5EqNOp0vNFCCzwgG-Dsjmes9jJww";

    const SIGNED_INIT_EVENT: &str = "
        uomdwYXlsb2FkWCQBcRIgEXCWI0EH1zQcSl57SUKRoo0WwhL6nMo8kLKfvgNaG0Nqc2lnbmF0dXJlc4G
        iaXByb3RlY3RlZFiBeyJhbGciOiJFZERTQSIsImtpZCI6ImRpZDprZXk6ejZNa3RCeW5BUExyRXllUzd
        wVnRoYml5U2NtZnU4bjVWN2JvWGd4eW81cTNTWlJSI3o2TWt0QnluQVBMckV5ZVM3cFZ0aGJpeVNjbWZ
        1OG41Vjdib1hneHlvNXEzU1pSUiJ9aXNpZ25hdHVyZVhAJAOOXHx9PyttNHjTzwe04TbsskxzfwNK5_X
        9e0rXiEOnVDq-Eeqe6KhuqnkSo06nS80UILPCAb4OyOZ6z2MnDA";
    const SIGNED_INIT_EVENT_PAYLOAD: &str = "uomRkYXRhoWVzdGVwaBkBTWZoZWFkZXKkY3NlcGVtb2RlbGVtb2RlbFgozgECAYUBEiCg6DKjNeOL00C0Og8Cmb88UVoPwz3kzXkQpD6Lkx6NWGZ1bmlxdWVMRKbxOrJBC7tqhWjea2NvbnRyb2xsZXJzgXg4ZGlkOmtleTp6Nk1rdEJ5bkFQTHJFeWVTN3BWdGhiaXlTY21mdThuNVY3Ym9YZ3h5bzVxM1NaUlI";

    const UNSIGNED_INIT_EVENT_CID: &str =
        "bafyreiakimdaub7m6inx2nljypdhvhu5vozjhylqukif4hjxt65qnkv6my";

    const UNSIGNED_INIT_EVENT_CAR: &str = "
        uOqJlcm9vdHOB2CpYJQABcRIgCkMGCgfs8ht9NWnDxnqenauyk-FwopBeHTefuwaqvmZndmVyc2lvbgHDAQFxEiAKQwYKB-zyG301acPGep6dq7KT4XCikF4dN5-7Bqq-ZqJkZGF0YfZmaGVhZGVypGNzZXBlbW9kZWxlbW9kZWxYKM4BAgGFARIghHTHRYxxeQXgc9Q6LUJVelzW5bnrw9TWgoBJlBIOVtdmdW5pcXVlR2Zvb3xiYXJrY29udHJvbGxlcnOBeDhkaWQ6a2V5Ono2TWt0Q0ZSY3dMUkZRQTlXYmVEUk03VzdrYkJkWlRIUTJ4blBneXhaTHExZ0NwSw";

    const UNSIGNED_INIT_EVENT_PAYLOAD: &str = "uomRkYXRh9mZoZWFkZXKkY3NlcGVtb2RlbGVtb2RlbFgozgECAYUBEiCEdMdFjHF5BeBz1DotQlV6XNbluevD1NaCgEmUEg5W12Z1bmlxdWVHZm9vfGJhcmtjb250cm9sbGVyc4F4OGRpZDprZXk6ejZNa3RDRlJjd0xSRlFBOVdiZURSTTdXN2tiQmRaVEhRMnhuUGd5eFpMcTFnQ3BL";

    // Data Event for a stream with a signed init event
    const DATA_EVENT_CAR: &str = "
        uO6Jlcm9vdHOB2CpYJgABhQESICddBxl5Sk2e7I20pzX9kDLf0jj6WvIQ1KqbM3WQiClDZ3ZlcnNpb24
        BqAEBcRIgdtssXEgR7sXQQQA1doBpxUpTn4pcAaVFZfQjyo-03SGjYmlk2CpYJgABhQESII6AYH_8-Nn
        iumDaEPcbPId6ZQAMFfViEDLxGVnVBNOOZGRhdGGBo2JvcGdyZXBsYWNlZHBhdGhmL3N0ZXBoZXZhbHV
        lGQFOZHByZXbYKlgmAAGFARIgjoBgf_z42eK6YNoQ9xs8h3plAAwV9WIQMvEZWdUE0466AgGFARIgJ10
        HGXlKTZ7sjbSnNf2QMt_SOPpa8hDUqpszdZCIKUOiZ3BheWxvYWRYJAFxEiB22yxcSBHuxdBBADV2gGn
        FSlOfilwBpUVl9CPKj7TdIWpzaWduYXR1cmVzgaJpcHJvdGVjdGVkWIF7ImFsZyI6IkVkRFNBIiwia2l
        kIjoiZGlkOmtleTp6Nk1rdEJ5bkFQTHJFeWVTN3BWdGhiaXlTY21mdThuNVY3Ym9YZ3h5bzVxM1NaUlI
        jejZNa3RCeW5BUExyRXllUzdwVnRoYml5U2NtZnU4bjVWN2JvWGd4eW81cTNTWlJSIn1pc2lnbmF0dXJ
        lWECym-Kwb5ti-T5dCygt4zf8Lr6MescAbkk_DILoy3fFjYG8fZVUCGKDQiTTHbNbzOk1yze7-2hA3AK
        dBfzJY1kA";

    const DATA_EVENT_PAYLOAD: &str = "uo2JpZNgqWCYAAYUBEiCOgGB__PjZ4rpg2hD3GzyHemUADBX1YhAy8RlZ1QTTjmRkYXRhgaNib3BncmVwbGFjZWRwYXRoZi9zdGVwaGV2YWx1ZRkBTmRwcmV22CpYJgABhQESII6AYH_8-NniumDaEPcbPId6ZQAMFfViEDLxGVnVBNOO";

    // Assumes Mainnet network
    const DATA_EVENT_ID: &str =
        "ce010500aa5773c7d75777e1deb6cb4af0e69eebd504d38e0185011220275d0719794a4d9eec8db4a735fd9032dfd238fa5af210d4aa9b337590882943";

    // Data Event for a stream with an unsigned init event
    const DATA_EVENT_CAR_UNSIGNED_INIT: &str = "
        uO6Jlcm9vdHOB2CpYJgABhQESIAlT-MndVmni9jiwS6JPtXtvYAa1-4tjruqLftM6BxvTZ3ZlcnNpb24B-gEBcRIguZ-ORAzcRLjL2LKcFJX2lC3Cv_4bywuG4Q8gEc5dbYajYmlk2CpYJQABcRIgCkMGCgfs8ht9NWnDxnqenauyk-FwopBeHTefuwaqvmZkZGF0YYSjYm9wY2FkZGRwYXRoZC9vbmVldmFsdWVjZm9vo2JvcGNhZGRkcGF0aGQvdHdvZXZhbHVlY2JhcqNib3BjYWRkZHBhdGhmL3RocmVlZXZhbHVlZmZvb2JhcqNib3BjYWRkZHBhdGhnL215RGF0YWV2YWx1ZQFkcHJldtgqWCUAAXESIApDBgoH7PIbfTVpw8Z6np2rspPhcKKQXh03n7sGqr5mugIBhQESIAlT-MndVmni9jiwS6JPtXtvYAa1-4tjruqLftM6BxvTomdwYXlsb2FkWCQBcRIguZ-ORAzcRLjL2LKcFJX2lC3Cv_4bywuG4Q8gEc5dbYZqc2lnbmF0dXJlc4GiaXByb3RlY3RlZFiBeyJhbGciOiJFZERTQSIsImtpZCI6ImRpZDprZXk6ejZNa3RDRlJjd0xSRlFBOVdiZURSTTdXN2tiQmRaVEhRMnhuUGd5eFpMcTFnQ3BLI3o2TWt0Q0ZSY3dMUkZRQTlXYmVEUk03VzdrYkJkWlRIUTJ4blBneXhaTHExZ0NwSyJ9aXNpZ25hdHVyZVhAZSJEw5QkFrYhbLYdLgnBn5SIbGAgm5i2jHhntWwe8nDkyKcCu4OvLMvFyGpjPloYVOr0JKwXlQfbgccHtbJpDw";

    const TIME_EVENT_CAR: &str = "
        uOqJlcm9vdHOB2CpYJQABcRIgcmqgb7eHSgQ32hS1NGVKZruLJGcKDI1f4lqOyNYn3eVndmVyc2lvbgG
        3AQFxEiByaqBvt4dKBDfaFLU0ZUpmu4skZwoMjV_iWo7I1ifd5aRiaWTYKlgmAAGFARIgjoBgf_z42eK
        6YNoQ9xs8h3plAAwV9WIQMvEZWdUE045kcGF0aGEwZHByZXbYKlgmAAGFARIgJ10HGXlKTZ7sjbSnNf2
        QMt_SOPpa8hDUqpszdZCIKUNlcHJvb2bYKlglAAFxEiAFKLx3fi7-yD1aPNyqnblI_r_5XllReVz55jB
        MvMxs9q4BAXESIAUovHd-Lv7IPVo83KqduUj-v_leWVF5XPnmMEy8zGz2pGRyb2902CpYJQABcRIgfWt
        bF-FQN6GN6ZL8OtHvp2YrGlmLbZwkOl6UY-3AUNFmdHhIYXNo2CpYJgABkwEbIBv-WU6fLnsyo5_lDST
        C_T-xUlW95brOAUDByGHJzbCRZnR4VHlwZWpmKGJ5dGVzMzIpZ2NoYWluSWRvZWlwMTU1OjExMTU1MTE
        xeQFxEiB9a1sX4VA3oY3pkvw60e-nZisaWYttnCQ6XpRj7cBQ0YPYKlgmAAGFARIgJ10HGXlKTZ7sjbS
        nNf2QMt_SOPpa8hDUqpszdZCIKUP22CpYJQABcRIgqVOMo-IVjo08Mk0cim3Z8flNyHY7c9g7uGMqeS0
        PFHA";

    #[test]
    fn build_init_payload() {
        let model =
            StreamId::from_str("kjzl6hvfrbw6c90uwoyz8j519gxma787qbsfjtrarkr1huq1g1s224k7hopvsyg")
                .unwrap();
        let model = model.to_vec();
        let unique = vec![68, 166, 241, 58, 178, 65, 11, 187, 106, 133, 104, 222];
        let data = ipld_core::ipld!({"steph": 333});

        let event = Builder::init()
            .with_controller("did:key:z6MktBynAPLrEyeS7pVthbiyScmfu8n5V7boXgxyo5q3SZRR".to_string())
            .with_sep("model".to_string(), model)
            .with_unique(unique) // optional
            .with_data(data) // optional
            .build();

        let dagcbor_str = multibase::encode(
            multibase::Base::Base64Url,
            &serde_ipld_dagcbor::to_vec(&event).unwrap(),
        );
        assert_eq!(SIGNED_INIT_EVENT_PAYLOAD, dagcbor_str);
    }

    #[test]
    fn build_data_payload() {
        let data = ipld_core::ipld!([{"op":"replace","path":"/steph","value":334}]);
        let id =
            Cid::from_str("bagcqcerar2aga7747dm6fota3iipogz4q55gkaamcx2weebs6emvtvie2oha").unwrap();
        let prev =
            Cid::from_str("bagcqcerar2aga7747dm6fota3iipogz4q55gkaamcx2weebs6emvtvie2oha").unwrap();

        let event = Builder::data()
            .with_id(id)
            .with_prev(prev)
            .with_data(data)
            .build();

        let dagcbor_str = multibase::encode(
            multibase::Base::Base64Url,
            &serde_ipld_dagcbor::to_vec(&event).unwrap(),
        );
        assert_eq!(DATA_EVENT_PAYLOAD, dagcbor_str);
    }
}
