use crate::unvalidated;
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
            Some(self.state.sep.value),
            self.state.should_index,
            self.state.unique,
        );
        let payload = unvalidated::init::Payload::new(header, self.state.data);
        payload
    }
}

#[cfg(test)]
mod tests {
    use ceramic_core::StreamId;
    use expect_test::expect;
    use multibase;

    use super::*;
    use std::str::FromStr;

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
        expect!["uomRkYXRhoWVzdGVwaBkBTWZoZWFkZXKkY3NlcGVtb2RlbGVtb2RlbFgozgECAYUBEiCg6DKjNeOL00C0Og8Cmb88UVoPwz3kzXkQpD6Lkx6NWGZ1bmlxdWVMRKbxOrJBC7tqhWjea2NvbnRyb2xsZXJzgXg4ZGlkOmtleTp6Nk1rdEJ5bkFQTHJFeWVTN3BWdGhiaXlTY21mdThuNVY3Ym9YZ3h5bzVxM1NaUlI"].assert_eq(&dagcbor_str);
    }
}
