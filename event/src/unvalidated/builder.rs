use crate::bytes::Bytes;
use crate::unvalidated;
use ceramic_core::StreamId;
use ipld_core::ipld::Ipld;

pub struct Builder;

impl Builder {
    /// Create builder for init events
    pub fn init() -> InitBuilder {
        InitBuilder::default()
    }
}

pub struct Separator {
    key: String,
    value: Bytes,
}

#[derive(Default)]
pub struct InitBuilder {
    sep: Option<Separator>,
    controller: Option<String>,
    unique: Option<Bytes>,
    data: Option<Ipld>,
    should_index: Option<bool>,
}

impl InitBuilder {
    pub fn build(self) -> unvalidated::init::Payload<Ipld> {
        let Separator {
            key: sep_key,
            value: sep_value,
        } = self.sep.unwrap();
        let header = unvalidated::init::Header::new(
            vec![self.controller.unwrap()],
            sep_key,
            Some(sep_value),
            self.should_index,
            self.unique,
        );
        let payload = unvalidated::init::Payload::new(header, self.data);
        payload
    }

    pub fn with_sep(mut self, key: String, value: Bytes) -> self {
        self.sep = Some(Separator { key, value });
        self
    }

    pub fn with_controller(mut self, controller: String) -> Self {
        self.controller = Some(controller);
        self
    }

    pub fn with_unique(mut self, unique: Bytes) -> Self {
        self.unique = Some(unique);
        self
    }

    pub fn with_should_index(mut self, should_index: bool) -> Self {
        self.should_index = Some(should_index);
        self
    }

    pub fn with_data(mut self, data: Ipld) -> Self {
        self.data = Some(data);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn build_init_payload() {
        let model =
            StreamId::from_str("kjzl6hvfrbw6c90uwoyz8j519gxma787qbsfjtrarkr1huq1g1s224k7hopvsyg")
                .unwrap();
        let model: Bytes = model.to_vec().into();
        let unique = Bytes(vec![68, 166, 241, 58, 178, 65, 11, 187, 106, 133, 104, 222]);
        let data = ipld_core::ipld!({"steph": 333});

        let event = Builder::init()
            .with_controller("did:key:z6MktBynAPLrEyeS7pVthbiyScmfu8n5V7boXgxyo5q3SZRR".to_string())
            .with_sep("model".to_string(), model)
            .with_should_index(true) // optional
            .with_unique(unique) // optional
            .with_data(data) // optional
            .build();

        // assert byte string equals SIGNED_INIT_EVENT_PAYLOAD
    }
}
