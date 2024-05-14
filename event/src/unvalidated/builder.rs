use crate::unvalidated::{data, init, Value, ValueMap};
use cid::Cid;

/// Add additional fields for events
pub trait Additional {
    /// Add additional field for key
    fn with_additional(self, key: String, value: Value) -> Self;
}

/// Add controllers for events
pub trait Controllers {
    /// Add controller
    fn with_controller(self, controller: String) -> Self;
}

/// Add separator for events
pub trait Sep {
    /// Add separator
    fn with_sep(self, sep: String) -> Self;
}

/// Builder for creating unvalidated events
#[derive(Default)]
pub struct Builder {
    sep: Option<String>,
    additional: ValueMap,
}

impl Additional for Builder {
    fn with_additional(mut self, key: String, value: Value) -> Self {
        self.additional.insert(key, value);
        self
    }
}

impl Sep for Builder {
    fn with_sep(mut self, sep: String) -> Self {
        self.sep = Some(sep);
        self
    }
}

impl Builder {
    /// Create builder for init events
    pub fn init(self) -> InitBuilder {
        InitBuilder {
            sep: self.sep,
            controllers: vec![],
            additional: self.additional,
        }
    }

    /// Create builder for data events
    pub fn data<T>(self, id: Cid, prev: Cid, data: T) -> DataBuilder<T> {
        DataBuilder {
            id,
            prev,
            data,
            sep: self.sep,
            additional: self.additional,
        }
    }
}

/// Builder for unsigned unvalidated events
pub struct InitBuilder {
    sep: Option<String>,
    controllers: Vec<String>,
    additional: ValueMap,
}

impl Additional for InitBuilder {
    fn with_additional(mut self, key: String, value: Value) -> Self {
        self.additional.insert(key, value);
        self
    }
}

impl Controllers for InitBuilder {
    fn with_controller(mut self, controller: String) -> Self {
        self.controllers.push(controller);
        self
    }
}

impl Sep for InitBuilder {
    fn with_sep(mut self, sep: String) -> Self {
        self.sep = Some(sep);
        self
    }
}

impl InitBuilder {
    /// Create unsigned init event without data
    pub async fn build(self) -> anyhow::Result<init::Payload<()>> {
        if self.controllers.is_empty() {
            anyhow::bail!("controllers must not be empty");
        }
        let sep = self
            .sep
            .ok_or_else(|| anyhow::anyhow!("sep must be specified"))?;
        let header = init::Header::new(self.controllers, sep, self.additional);
        let payload = init::Payload::new(header, None);
        Ok(payload)
    }

    /// Create builder for init event with data
    pub fn with_data<T>(self, data: T) -> InitWithDataBuilder<T> {
        InitWithDataBuilder {
            data,
            controllers: self.controllers,
            sep: self.sep,
            additional: self.additional,
        }
    }
}

/// Builder for init event with data
pub struct InitWithDataBuilder<D> {
    data: D,
    controllers: Vec<String>,
    sep: Option<String>,
    additional: ValueMap,
}

impl<D> Additional for InitWithDataBuilder<D> {
    fn with_additional(mut self, key: String, value: Value) -> Self {
        self.additional.insert(key, value);
        self
    }
}

impl<D> Controllers for InitWithDataBuilder<D> {
    fn with_controller(mut self, controller: String) -> Self {
        self.controllers.push(controller);
        self
    }
}

impl<D> Sep for InitWithDataBuilder<D> {
    fn with_sep(mut self, sep: String) -> Self {
        self.sep = Some(sep);
        self
    }
}

impl<D> InitWithDataBuilder<D> {
    /// Build unsigned init event with data
    pub async fn build(self) -> anyhow::Result<init::Payload<D>> {
        if self.controllers.is_empty() {
            anyhow::bail!("controllers must not be empty");
        }
        let sep = self
            .sep
            .ok_or_else(|| anyhow::anyhow!("sep must be specified"))?;
        let header = init::Header::new(self.controllers, sep, self.additional);
        let payload = init::Payload::new(header, Some(self.data));
        Ok(payload)
    }
}

/// Builder for data event
pub struct DataBuilder<T> {
    id: Cid,
    prev: Cid,
    data: T,
    sep: Option<String>,
    additional: ValueMap,
}

impl<T> Additional for DataBuilder<T> {
    fn with_additional(mut self, key: String, value: Value) -> Self {
        self.additional.insert(key, value);
        self
    }
}

impl<T> Sep for DataBuilder<T> {
    fn with_sep(mut self, sep: String) -> Self {
        self.sep = Some(sep);
        self
    }
}

impl<T> DataBuilder<T> {
    /// Build data event
    pub async fn build(self) -> anyhow::Result<data::Payload<T>> {
        let header = data::Header::new(vec![], self.additional);
        let payload = data::Payload::new(self.id, self.prev, Some(header), self.data);
        Ok(payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use expect_test::expect;

    const TEST_CID: &str = "bafyreiaroclcgqih242byss6pneufencrulmeex2ttfdzefst67agwq3im";

    #[tokio::test]
    async fn should_build_init_event_without_data() {
        let event = Builder::default()
            .with_sep("sep".to_string())
            .init()
            .with_controller("controller".to_string())
            .build()
            .await
            .expect("failed to build event");
        expect![[r#"
            {
              "header": {
                "controllers": [
                  "controller"
                ],
                "sep": "sep"
              }
            }"#]]
        .assert_eq(&crate::tests::serialize_to_pretty_json(&event));
    }

    #[tokio::test]
    async fn should_build_init_event_with_data() {
        let event = Builder::default()
            .init()
            .with_sep("sep".to_string())
            .with_controller("controller".to_string())
            .with_data("data".to_string())
            .build()
            .await
            .expect("failed to build event");
        expect![[r#"
            {
              "header": {
                "controllers": [
                  "controller"
                ],
                "sep": "sep"
              },
              "data": "data"
            }"#]]
        .assert_eq(&crate::tests::serialize_to_pretty_json(&event));
    }

    #[tokio::test]
    async fn should_build_data_event() {
        let event = Builder::default()
            .data(
                Cid::try_from(TEST_CID).unwrap(),
                Cid::try_from(TEST_CID).unwrap(),
                "data",
            )
            .with_sep("sep".to_string())
            .with_additional("model".to_string(), "blah".to_string().into())
            .build()
            .await
            .expect("failed to build event");
        expect![[r#"
            {
              "id": [
                1,
                113,
                18,
                32,
                17,
                112,
                150,
                35,
                65,
                7,
                215,
                52,
                28,
                74,
                94,
                123,
                73,
                66,
                145,
                162,
                141,
                22,
                194,
                18,
                250,
                156,
                202,
                60,
                144,
                178,
                159,
                190,
                3,
                90,
                27,
                67
              ],
              "prev": [
                1,
                113,
                18,
                32,
                17,
                112,
                150,
                35,
                65,
                7,
                215,
                52,
                28,
                74,
                94,
                123,
                73,
                66,
                145,
                162,
                141,
                22,
                194,
                18,
                250,
                156,
                202,
                60,
                144,
                178,
                159,
                190,
                3,
                90,
                27,
                67
              ],
              "header": {
                "model": "blah"
              },
              "data": "data"
            }"#]]
        .assert_eq(&crate::tests::serialize_to_pretty_json(&event));
    }
}
