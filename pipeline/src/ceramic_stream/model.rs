use anyhow::{anyhow, bail, Result};
use ceramic_core::StreamId;
use schemars::{JsonSchema, Schema};
use serde::{Deserialize, Serialize};
use tracing::instrument;

pub use super::model_versions::*;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "version")]
#[serde(rename_all = "camelCase")]
/// This is the payload of an event with stream type 2 (Model)
pub enum ModelDefinition {
    // rename_all doesn't work on enums names when defined inline so
    // some things must be explicitly renamed (e.g. accountRelation)
    #[serde(rename = "1.0")]
    V1(ModelDefinitionV1),
    #[serde(rename = "2.0")]
    V2(ModelDefinitionV2),
}

impl ModelDefinition {
    fn schema_for<T: JsonSchema>() -> Schema {
        let settings = schemars::generate::SchemaSettings::default().with(|s| {
            s.meta_schema = Some("https://json-schema.org/draft/2020-12/schema".to_string());
            s.option_nullable = true;
            s.option_add_null_type = false;
        });
        let gen = settings.into_generator();
        gen.into_root_schema_for::<T>()
    }
    /// Create a new definition for a type that implements `GetSchema`
    pub fn new_v2<T: JsonSchema>(
        name: String,
        description: Option<String>,
        interface: bool,
        implements: Option<Vec<StreamId>>,
        immutable_fields: Option<Vec<String>>,
        account_relation: ModelAccountRelationV2,
    ) -> anyhow::Result<Self> {
        let schema = Self::schema_for::<T>();
        Ok(Self::V2(ModelDefinitionV2 {
            name,
            description,
            interface,
            implements: implements.unwrap_or_default(),
            schema,
            immutable_fields,
            account_relation,
            //TODO expose these features
            relations: Default::default(),
            views: Default::default(),
        }))
    }
}

impl ModelDefinition {
    /// Get the JSON schema for the model
    pub fn schema(&self) -> &schemars::Schema {
        match self {
            ModelDefinition::V1(v1) => &v1.schema,
            ModelDefinition::V2(v2) => &v2.schema,
        }
    }

    /// Whether or not the model is an interface
    pub fn is_interface(&self) -> bool {
        match self {
            ModelDefinition::V1 { .. } => false,
            ModelDefinition::V2(v2) => v2.interface,
        }
    }

    /// Whether or not the model's account relation type is deterministic
    pub fn is_deterministic_account_relation(&self) -> bool {
        let relation: ModelAccountRelation = match self {
            ModelDefinition::V1(v1) => (&v1.account_relation).into(),
            ModelDefinition::V2(v2) => (&v2.account_relation).into(),
        };
        relation.is_deterministic()
    }

    /// Converts the ModelAccountRelationV1/V2 types into a shared type (without fields) to simplify checks
    pub fn account_relation(&self) -> ModelAccountRelation {
        match self {
            ModelDefinition::V1(v1) => (&v1.account_relation).into(),
            ModelDefinition::V2(v2) => (&v2.account_relation).into(),
        }
    }

    /// Make sure the model conforms to all the rules before it's persisted
    /// Requires the schemas of all interfaces implemented in order to verify its schema against them
    /// This is the model-handler validation in js-ceramic
    #[instrument(skip_all)]
    pub fn validate(
        &self,
        previous: Option<&ModelDefinition>,
        interfaces: Option<&[ModelDefinition]>,
    ) -> Result<()> {
        // TODO: some applyGenesis/applySigned checks in js-ceramic require the header and are not validated in c1 currently
        // these checks should maybe be included in a stream type validation in the event validation code, as it currently
        // only validates envelope structure, validity to stream/event log and signatures. examples:
        // - header.model must be META_MODEL (e.g. kh4q0ozorrgaq2mezktnrmdwleo1d)
        // - any data event for a model stream should be rejected somewhere (not enforced)

        // completeness, keys, version, relations (including set fields), json schema structure are verified by deserialization
        // but we still need to validate more schema, view, iterface information below

        match self {
            ModelDefinition::V1(v1) => {
                let ModelDefinitionV1 { views, schema, .. } = v1;
                Self::validate_schema(schema)?;
                if !views.is_empty() {
                    Self::validate_views(views.keys(), schema)?;
                }
                if previous.is_some() {
                    bail!("cannot update version 1 models")
                }
            }
            ModelDefinition::V2(v2) => {
                let ModelDefinitionV2 {
                    interface,
                    views,
                    schema,
                    ..
                } = v2;
                Self::validate_schema(schema)?;

                if !views.is_empty() {
                    Self::validate_views(views.keys(), schema)?;
                }
                if *interface {
                    let s = schema.as_object().unwrap();
                    let num_properties = s
                        .get("properties")
                        .map_or(0, |p| p.as_object().map_or(0, |a| a.len()));
                    let num_views = views.len();
                    if num_properties == 0 && num_views == 0 {
                        bail!("Invalid interface: a least one propery or view must be present");
                    }
                }
                if let Some(interfaces) = interfaces {
                    v2.validate_implementated_interfaces(interfaces)?;
                }
                if let Some(previous) = previous {
                    if let ModelDefinition::V2(previous) = previous {
                        if v2.interface {
                            bail!("cannot update interface models")
                        }
                        if previous.interface != v2.interface {
                            bail!("cannot change model to an interface")
                        }
                        if previous.immutable_fields != v2.immutable_fields {
                            bail!("cannot change a model's immutable fields")
                        }
                        if previous.account_relation != v2.account_relation {
                            bail!("cannot change a model's account relation")
                        }
                        if previous.relations != v2.relations {
                            bail!("cannot change a model's relations")
                        }
                        if previous.views != v2.views {
                            bail!("cannot change a model's views")
                        }
                        // Validate schema changes
                        let changes = json_schema_diff::diff(
                            previous.schema.as_value().clone(),
                            v2.schema.as_value().clone(),
                        )?;
                        // TODO validate is_breaking is the correct logic we want.
                        if changes.iter().any(|change| change.change.is_breaking()) {
                            // TODO include information about the change
                            bail!("breaking change made to model schema")
                        }
                        // NOTE: This leaves changing the name, description, implements, and
                        // schema fields as the only valid changes.
                    } else {
                        bail!("cannot change model version from 1 to 2")
                    }
                }
            }
        }
        Ok(())
    }

    // Checks that the model JSON schema is an object and every included object has additionalProperties: false
    fn validate_schema(schema: &schemars::Schema) -> Result<()> {
        let s = schema
            .as_object()
            .ok_or_else(|| anyhow!("schema should be an object"))?;
        Self::verify_schema_objects_disable_additional_properites(s)?;
        Ok(())
    }

    fn verify_schema_objects_disable_additional_properites(
        map: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<()> {
        if map.get("type").is_some_and(|t| t == "object") {
            if map
                .get("additionalProperties")
                .is_none_or(|p| p.as_bool().is_none_or(|p| p))
            {
                bail!("All objects in schema need to have additional properties disabled")
            }

            for val in map.values() {
                if let Some(v) = val.as_object() {
                    Self::verify_schema_objects_disable_additional_properites(v)?
                }
            }
        }
        Ok(())
    }

    fn validate_views<V>(
        keys: std::collections::hash_map::Keys<'_, String, V>,
        schema: &schemars::Schema,
    ) -> Result<()> {
        // in the JS `validateViews` code, we have `schema.properties[key] !== undefined` which implies
        // it must exist as an object. however, the next function call to `validateInterface` includes
        // `Object.keys(model.schema?.properties ?? {}).length === 0` which would work with undefined.
        // since we have the same if checks about views existing, I'm assuming this won't panic.
        let schema_properties = schema
            .as_object()
            .expect("JSON schema must be an object")
            .get("properties")
            .expect("JSON schema must have properties")
            .as_object()
            .expect("JSON schema properties must be an object");
        for key in keys {
            if schema_properties.contains_key(key) {
                bail!("view definition used with a property also present in schema ({key})")
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// conversion type for v1 and v2 since we enforce the same rules for each it simplies
/// the matches and number of functions we need
pub enum ModelAccountRelation {
    Single,
    List,
    None,
    // Drop fields as we don't need them for current checks
    Set,
}

impl ModelAccountRelation {
    fn is_deterministic(&self) -> bool {
        matches!(self, Self::Single | Self::Set)
    }
}

impl From<&ModelAccountRelationV1> for ModelAccountRelation {
    fn from(value: &ModelAccountRelationV1) -> Self {
        match value {
            ModelAccountRelationV1::Single => Self::Single,
            ModelAccountRelationV1::List => Self::List,
        }
    }
}

impl From<&ModelAccountRelationV2> for ModelAccountRelation {
    fn from(value: &ModelAccountRelationV2) -> Self {
        match value {
            ModelAccountRelationV2::Single => Self::Single,
            ModelAccountRelationV2::List => Self::List,
            ModelAccountRelationV2::None => Self::None,
            ModelAccountRelationV2::Set { .. } => Self::Set,
        }
    }
}

#[cfg(test)]
mod test {
    use ceramic_core::StreamId;

    use super::*;

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct ModelWrapper {
        stream_id: StreamId,
        state: DocState,
    }
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct DocState {
        r#type: i32,
        content: ModelDefinition,
    }

    fn assert_model_roundtrips(m: serde_json::Value) {
        let parsed = serde_json::from_value::<ModelWrapper>(m.clone())
            .map_err(|e| format!("from_value {e} for {:?}", m))
            .unwrap();
        parsed
            .state
            .content
            .validate(None, None)
            .expect("ModelDefinition should validate");
        let back = serde_json::to_value(parsed)
            .map_err(|e| format!("to_value {e} for {:?}", m))
            .unwrap();

        assert_eq!(back, m);
    }

    #[test]
    fn gitcoin_attestation() {
        let model = serde_json::from_str::<serde_json::Value>(include_str!(
            "./test_models/gc_attestation_interface.json"
        ))
        .unwrap();
        assert_model_roundtrips(model);
    }

    #[test]
    fn gitcoin_stamp_wrapper_interface() {
        let model = serde_json::from_str::<serde_json::Value>(include_str!(
            "./test_models/gc_passport_stamp_wrapper_interface.json"
        ))
        .unwrap();
        assert_model_roundtrips(model);
    }

    #[test]
    fn gitcoin_vc_interface() {
        let model = serde_json::from_str::<serde_json::Value>(include_str!(
            "./test_models/vc_interface.json"
        ))
        .unwrap();

        assert_model_roundtrips(model);
    }

    #[test]
    fn gitcoin_passport_stamp() {
        let model = serde_json::from_str::<serde_json::Value>(include_str!(
            "./test_models/gc_passport_stamp.json"
        ))
        .unwrap();
        assert_model_roundtrips(model);
    }

    #[test]
    fn gitcoin_stamp_wrapper() {
        let model = serde_json::from_str::<serde_json::Value>(include_str!(
            "./test_models/gc_passport_stamp_wrapper.json"
        ))
        .unwrap();
        assert_model_roundtrips(model);
    }

    #[test]
    fn test_interface_impl() {
        let model = serde_json::from_str::<ModelWrapper>(include_str!(
            "./test_models/gc_passport_stamp.json"
        ))
        .unwrap();

        let interface1 =
            serde_json::from_str::<ModelWrapper>(include_str!("./test_models/vc_interface.json"))
                .unwrap()
                .state
                .content;

        let interface2 = serde_json::from_str::<ModelWrapper>(include_str!(
            "./test_models/gc_attestation_interface.json"
        ))
        .unwrap()
        .state
        .content;

        model
            .state
            .content
            .validate(None, Some(&[interface1, interface2]))
            .unwrap()
    }
}
