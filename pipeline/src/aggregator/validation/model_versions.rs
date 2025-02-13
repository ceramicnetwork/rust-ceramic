use std::collections::HashMap;

use ceramic_core::StreamId;
use serde::{Deserialize, Serialize};

use crate::aggregator::{result::ValidationResult, validation::InterfaceUtil};

use super::model::ModelDefinition;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelDefinitionV1 {
    pub(crate) name: String,
    #[serde(skip)]
    pub(crate) description: Option<String>,
    pub(crate) schema: schemars::Schema,
    #[serde(rename = "accountRelation")]
    pub(crate) account_relation: ModelAccountRelationV1,
    #[serde(default)]
    pub(crate) relations: ModelRelationsDefinitionV1,
    #[serde(default)]
    pub(crate) views: ModelViewsDefinitionV1,
    pub(crate) version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelDefinitionV2 {
    pub(crate) name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
    pub(crate) interface: bool,
    pub(crate) implements: Vec<StreamId>,
    pub(crate) schema: schemars::Schema,
    #[serde(rename = "immutableFields")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) immutable_fields: Option<Vec<String>>,
    #[serde(rename = "accountRelation")]
    pub(crate) account_relation: ModelAccountRelationV2,
    #[serde(default)]
    pub(crate) relations: ModelRelationsDefinitionV2,
    #[serde(default)]
    pub(crate) views: ModelViewsDefinitionV2,
    pub(crate) version: String,
}

impl ModelDefinitionV2 {
    /// Validates that this model correctly implements the expected interfaces
    /// Does NOT yet correctly verify the JSON schema is a truly correct implementation.
    /// Does validate that the other fields (relations, accountRelations, views) do, however,
    /// there are is an open question about what happens when the interface includes a `Document {None}`
    /// relation and what the implementor includes (e.g. None, anything)
    pub fn validate_implementated_interfaces(
        &self,
        interfaces: &[ModelDefinition],
    ) -> ValidationResult {
        let mut result = ValidationResult::Pass(());
        for interface_model in interfaces {
            result = result.merge(self.validate_interface_impl(interface_model));
        }
        result
    }

    fn validate_interface_impl(&self, interface: &ModelDefinition) -> ValidationResult {
        // This function is currently trivially implemented
        maybe_fail!(InterfaceUtil::validate_schema_implementation(
            interface.schema(),
            &self.schema
        ));

        let mut errs = Vec::new();
        let interface_id = "TODO";
        match interface {
            // v1 only checks schema conformance AFAICT
            ModelDefinition::V1(_) => ValidationResult::Pass(()),
            ModelDefinition::V2(expected) => {
                // todo
                if !InterfaceUtil::is_valid_relations_implementation(
                    &expected.relations,
                    &self.relations,
                ) {
                    errs.push(format!(
                        "Invalid relations implementation of interface {interface_id}"
                    ))
                }

                if !InterfaceUtil::is_valid_views_implementation(&expected.views, &self.views) {
                    errs.push(format!(
                        "Invalid views implementation of interface {interface_id}"
                    ))
                }

                if !InterfaceUtil::is_valid_immutability_implementation(expected, self) {
                    errs.push(format!(
                        "Invalid immutable fields implementation of interface {interface_id}"
                    ))
                }

                errs.into()
            }
        }
    }

    // Event height after which fields are locked.
    pub(crate) fn locked_height(&self) -> u32 {
        match self.account_relation {
            // These relations need to allow a single data event after the init event that can set
            // the initial data.
            ModelAccountRelationV2::Single | ModelAccountRelationV2::Set { .. } => 1,
            // These relations lock the fields after the init event.
            ModelAccountRelationV2::List | ModelAccountRelationV2::None => 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ModelAccountRelationV1 {
    Single,
    List,
}

/// Defines the relationship between models, instances and an account.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ModelAccountRelationV2 {
    /// At most a single instance of the model may exist per account.
    Single,
    /// Any number of instances of the model may exist per account.
    List,
    /// No explicit account relation has been defined.
    None,
    /// At most a single instance of the model per unique set of fields may exist per account.
    Set {
        /// Names of fields in the model that make up the unique set.
        /// These fields are immutable for a given model instance stream.
        fields: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ModelRelationDefinitionV1 {
    Account,
    Document { model: StreamId },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ModelRelationDefinitionV2 {
    Account,
    Document { model: Option<StreamId> },
}

// The inteface validation code is all implemented in js-ceramic as V1 and we can trivially convert, so we do
impl From<ModelRelationDefinitionV1> for ModelRelationDefinitionV2 {
    fn from(value: ModelRelationDefinitionV1) -> Self {
        match value {
            ModelRelationDefinitionV1::Account => Self::Account,
            ModelRelationDefinitionV1::Document { model } => Self::Document { model: Some(model) },
        }
    }
}

pub type ModelRelationsDefinitionV1 = HashMap<String, ModelRelationDefinitionV1>;
pub type ModelRelationsDefinitionV2 = HashMap<String, ModelRelationDefinitionV2>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ModelDocumentMetadataViewDefinition {
    #[serde(rename = "documentAccount")]
    Account,
    #[serde(rename = "documentVersion")]
    Version,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged, rename_all = "camelCase")]
pub enum ModelViewDefinitionV1 {
    Metadata(ModelDocumentMetadataViewDefinition),
    Relation(ModelRelationViewDefinitionV1),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged, rename_all = "camelCase")]
pub enum ModelViewDefinitionV2 {
    Metadata(ModelDocumentMetadataViewDefinition),
    Relation(ModelRelationViewDefinitionV2),
}

pub type ModelViewsDefinitionV1 = HashMap<String, ModelViewDefinitionV1>;
pub type ModelViewsDefinitionV2 = HashMap<String, ModelViewDefinitionV2>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ModelRelationViewDefinitionV1 {
    #[serde(rename = "relationDocument")]
    Document { model: StreamId, property: String },
    #[serde(rename = "relationFrom")]
    From { model: StreamId, property: String },
    #[serde(rename = "relationCountFrom")]
    CountFrom { model: StreamId, property: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ModelRelationViewDefinitionV2 {
    #[serde(rename = "relationDocument")]
    Document {
        model: Option<StreamId>,
        property: String,
    },
    #[serde(rename = "relationFrom")]
    From { model: StreamId, property: String },
    #[serde(rename = "relationCountFrom")]
    CountFrom { model: StreamId, property: String },
    #[serde(rename = "relationSetFrom")]
    SetFrom { model: StreamId, property: String },
}
