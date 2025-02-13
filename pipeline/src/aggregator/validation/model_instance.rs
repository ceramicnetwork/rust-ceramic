use std::str::FromStr;

use anyhow::Context as _;
use ceramic_core::{StreamId, StreamIdType};
use cid::Cid;
use json_patch::Patch;
use serde::{Deserialize, Serialize};
use tracing::{instrument, Level};

use crate::aggregator::result::{OptionValidation as _, ResultValidation as _, ValidationResult};

use super::{
    model::{
        ModelAccountRelation, ModelAccountRelationV2, ModelDefinition, ModelDefinitionV2,
        ModelRelationDefinitionV2,
    },
    schema_validator::SchemaValidator,
};

#[derive(Debug, Serialize, Deserialize)]
/// This is the payload of an event with stream type 3 (MID or ModelInstanceDocument)
pub struct ModelInstance {
    /// JSON payload after the patch has been applied.
    /// It will be validated to conform to the schema of the associated Model and more.
    content: Option<serde_json::Value>,
}

impl ModelInstance {
    /// Validate that a ModelInstanceDocument payload (after patch applied) is valid for the stream.
    /// Patch must be the patch that produced the current state or None for an init or time event.
    #[instrument(skip(self,validator), ret(level = Level::DEBUG))]
    pub fn validate(
        &self,
        validator: &SchemaValidator,
        patch: Option<&Patch>,
        model_version: &Cid,
        model: &ModelDefinition,
        event_height: u32,
        unique: Option<&[u8]>,
    ) -> ValidationResult {
        if model.is_interface() {
            fail!("ModelInstanceDocument Streams cannot be created on interface Models. Use a different model than {}", 
                // Here we assume that the interface model has not been updated in order to
                // construct a stream id of the model.
                // This is a valid assumption as the model validation rules do not allow interfaces
                // to be updated.
                StreamId {
                    r#type: StreamIdType::Model,
                    cid:*model_version
                });
        }

        maybe_fail!(self.validate_account_relation(model, patch, event_height, unique));

        maybe_fail!(self.validate_immutable_fields_update(model, patch, event_height));

        // content/data must conform to schema -> validate_schema
        if let Some(mid_data) = &self.content {
            maybe_fail!(validator.validate_mid_conforms_to_model(mid_data, model_version, model));
        }

        maybe_fail!(self.validate_relations(model));

        ValidationResult::Pass(())
    }

    /// Validate the event and it patch follow the account relation rules, ensuring the integrity
    /// of the relation.
    fn validate_account_relation(
        &self,
        model: &ModelDefinition,
        patch: Option<&Patch>,
        event_height: u32,
        unique: Option<&[u8]>,
    ) -> ValidationResult {
        if event_height == 0 && self.content.is_some() && model.is_deterministic_account_relation()
        {
            fail!(
                "ModelInstanceDocuments with an account relation {} must not have initial content",
                model.account_relation()
            )
        }
        // Check unique is set/unset appriopriately
        let account_relation = model.account_relation();
        match account_relation {
            ModelAccountRelation::Single => {
                if unique.is_some() {
                    fail!("ModelInstanceDocuments for models with SINGLE accountRelations must be created deterministically")
                }
            }
            ModelAccountRelation::List => {
                if unique.is_none() {
                    fail!("ModelInstanceDocuments for models with LIST accountRelations must be created with a unique field");
                }
            }
            ModelAccountRelation::Set => {
                if unique.is_none() {
                    fail!("ModelInstanceDocuments for models with SET accountRelations must be created with a unique field containing data from the fields providing the set semantics");
                }
            }
            ModelAccountRelation::None => {}
        };
        // Check that set relation fields did not change.
        if let ModelDefinition::V2(
            v2 @ ModelDefinitionV2 {
                account_relation: ModelAccountRelationV2::Set { ref fields },
                ..
            },
        ) = model
        {
            let modified_fields =
                self.report_modified_locked_fields(v2, fields, patch, event_height);
            if !modified_fields.is_empty() {
                fail!("Set account relation fields {modified_fields:?} cannot be modified")
            }
        }
        ValidationResult::Pass(())
    }

    // Validate the the immutable fields of the model did not change with this patch.
    // When there is no patch this is a noop.
    fn validate_immutable_fields_update(
        &self,
        model: &ModelDefinition,
        patch: Option<&Patch>,
        event_height: u32,
    ) -> ValidationResult {
        match model {
            ModelDefinition::V1(_) => {}
            ModelDefinition::V2(v2) => {
                if let Some(immutable) = &v2.immutable_fields {
                    let modified_fields =
                        self.report_modified_locked_fields(v2, immutable, patch, event_height);
                    if !modified_fields.is_empty() {
                        fail!(modified_fields
                            .into_iter()
                            .map(|field| format!("Immutable field '{field}' cannot be modified"))
                            .collect::<Vec<String>>())
                    }
                }
            }
        }
        ValidationResult::Pass(())
    }

    // Return any of the listed fields that changed with this patch.
    // When there is no patch this is a noop.
    fn report_modified_locked_fields(
        &self,
        model: &ModelDefinitionV2,
        fields: &[String],
        patch: Option<&Patch>,
        event_height: u32,
    ) -> Vec<String> {
        let mut modified_fields = Vec::with_capacity(fields.len());
        if event_height > model.locked_height() {
            if let Some(patch) = patch {
                // use a loop for a better error
                for modified in patch.0.iter().flat_map(|p| p.path().front()) {
                    // can't use contains with &String and &str https://github.com/rust-lang/rust/issues/42671
                    if fields.iter().any(|i| i == modified.decoded().as_ref()) {
                        modified_fields.push(modified.to_string());
                    }
                }
            }
        }
        modified_fields
    }

    /// This validates the model relations are correctly used in MID data
    /// In js-ceramic, we would load the related MIDs and verify they are for the correct model.
    /// We no longer do this, currently we simply validate the defintion Document { model } is
    /// for a model stream type, and the field containing the related streamID is a StreamID of type MID
    fn validate_relations(&self, model: &ModelDefinition) -> ValidationResult {
        let mut result = ValidationResult::Pass(());
        match model {
            ModelDefinition::V1(m) => {
                for (key, relation) in m.relations.iter() {
                    let relation = relation.to_owned().into();
                    result = result.merge(self.validate_relation(key, &relation));
                }
            }
            ModelDefinition::V2(m) => {
                for (key, relation) in m.relations.iter() {
                    result = result.merge(self.validate_relation(key, relation));
                }
            }
        };
        result
    }

    fn validate_relation(
        &self,
        field_name: &str,
        relation: &ModelRelationDefinitionV2,
    ) -> ValidationResult {
        match relation {
            ModelRelationDefinitionV2::Account => ValidationResult::Pass(()),
            ModelRelationDefinitionV2::Document { model } => {
                // We verify the related model stream ID is of type model and check that the
                // included value in the payload is actually a MID stream type
                if let Some(stream_id) = model.as_ref() {
                    if !stream_id.is_model() {
                        fail!("Model relation of type Document with a model Stream ID ({stream_id}) must be of stream type 2 (Model) not {:?}", stream_id.r#type)
                    }
                }
                if let Some(target_mid) = self
                    .content
                    .as_ref()
                    .and_then(|c| c.as_object().and_then(|c| c.get(field_name)))
                {
                    let mid = maybe_fail!(target_mid
                        .as_str()
                        .ok_or_validation_failure("document relation field must be a string"));
                    let mid = maybe_fail!(StreamId::from_str(mid)
                        .context("document relation must be a stream id")
                        .map_to_validation_failure());
                    if !mid.is_document() {
                        fail!("Model relation of type Document with a target Stream ID ({mid}) must be of stream type 3 (MID) not {:?}", mid.r#type)
                    }
                }

                ValidationResult::Pass(())
            }
        }
    }
}
