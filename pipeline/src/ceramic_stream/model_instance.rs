use std::str::FromStr;

use anyhow::{anyhow, bail, Context as _, Result};
use ceramic_core::StreamId;
use cid::Cid;
use serde::{Deserialize, Serialize};
use tracing::{instrument, Level};

use super::{
    model::{ModelAccountRelation, ModelDefinition, ModelRelationDefinitionV2},
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
    /// Not sure about `stream_event_number` yet, but some checks need to know if this is the init event (0) or if it's the
    /// first data event. Getting which number event this is answers both questions (assuming 0 index, might be 1 index in reality or another solution)
    ///
    /// TODO: There are some checks in js-ceramic that require the event header and should possibly be
    /// part of the initial event validation (i.e. write path) to check stream type structure. for example,
    ///   - should be signed or no data/content
    ///   - unique header is correctly included/excluded (see [`ModelDefinition::validate_unique`])
    ///   - things we may no longer want (exactly one controller)
    /// these checks do different things for init/data events as headers (other than shouldIndex) are fixed
    /// for the life of the stream currently. For now, we ignore those checks here but still need to know
    /// if it's an init event
    #[instrument(skip(validator),err(level = Level::DEBUG))]
    pub fn validate(
        &self,
        validator: &SchemaValidator,
        previous: Option<&ModelInstance>,
        model_version: &Cid,
        model: &ModelDefinition,
    ) -> Result<()> {
        let is_init = previous.is_none();

        if is_init {
            if model.is_interface() {
                bail!("ModelInstanceDocument Streams cannot be created on interface Models. Use a different model than {}", "TODO: model stream ID?")
            }
            // can't do this because we don't have header
            // self.validate_header(model, unique)?;
        } else {
            // in js code, header is validated that only shouldIndex can change here
            self.validate_unique(model)?;
        }

        if is_init && self.content.is_some() && model.is_deterministic_account_relation() {
            bail!("Deterministic init events for ModelInstanceDocuments must not have content")
        }
        // skip content length check (16_000_000 bytes in js-ceramic)

        // content/data must conform to schema -> validate_schema
        if let Some(mid_data) = &self.content {
            validator.validate_mid_conforms_to_model(mid_data, model_version, model)?;
        }

        self.validate_relations(model)?;

        self.validate_locked_fields_update(model, previous)?;

        Ok(())
    }

    /// This uses the updated fields (via JSON Patch) to validate the locked fields were not modified
    fn validate_locked_fields_update(
        &self,
        model: &ModelDefinition,
        previous: Option<&ModelInstance>,
    ) -> Result<()> {
        match model {
            ModelDefinition::V1(_) => Ok(()),
            ModelDefinition::V2(v2) => {
                if let Some(immutable) = &v2.immutable_fields {
                    if let Some(current) = &self.content {
                        // If we do not have a previous there then locked fields can be set the
                        // first time.
                        // Is this true? Or is setting the locked fields the first time considered
                        // a patch to the empty object.
                        if let Some(previous) = previous.and_then(|p| p.content.as_ref()) {
                            let patch = json_patch::diff(previous, current);
                            // use a loop for a better error
                            for modified in patch.0.iter().flat_map(|p| p.path().front()) {
                                // can't use contains with &String and &str https://github.com/rust-lang/rust/issues/42671
                                if immutable.iter().any(|i| i == modified.decoded().as_ref()) {
                                    bail!("Immutable field {modified} cannot be updated")
                                }
                            }
                        }
                    }
                }
                Ok(())
            }
        }
    }

    /// This validates the model relations are correctly used in MID data
    /// In js-ceramic, we would load the related MIDs and verify they are for the correct model.
    /// We no longer do this, currently we simply validate the defintion Document { model } is
    /// for a model stream type, and the field containing the related streamID is a StreamID of type MID
    fn validate_relations(&self, model: &ModelDefinition) -> Result<()> {
        match model {
            ModelDefinition::V1(m) => {
                for (key, relation) in m.relations.iter() {
                    let relation = relation.to_owned().into();
                    self.validate_relation(key, &relation)?;
                }
            }
            ModelDefinition::V2(m) => {
                for (key, relation) in m.relations.iter() {
                    self.validate_relation(key, relation)?;
                }
            }
        };

        Ok(())
    }

    fn validate_relation(
        &self,
        field_name: &str,
        relation: &ModelRelationDefinitionV2,
    ) -> Result<()> {
        match relation {
            ModelRelationDefinitionV2::Account => Ok(()),
            ModelRelationDefinitionV2::Document { model } => {
                // js-ceramic first validates the stream ID is valid if it exists, but we do that
                // during deserialization.

                // As we're not planning to load the target MID and compare against the model (see commented code below),
                // we simply verify the related model stream ID is of type model (maybe add this to deserializer) and check that the
                // included value in the payload is actually a MID stream type
                if let Some(stream_id) = model.as_ref() {
                    if !stream_id.is_model() {
                        bail!("Model relation of type Document with a model Stream ID ({stream_id}) must be of stream type 2 (Model) not {:?}", stream_id.r#type)
                    }
                }
                if let Some(target_mid) = self
                    .content
                    .as_ref()
                    .and_then(|c| c.as_object().and_then(|c| c.get(field_name)))
                {
                    let mid = target_mid.as_str().ok_or_else(|| {
                        anyhow!("Document relation field must be a string stream ID")
                    })?;
                    let mid =
                        StreamId::from_str(mid).context("Document relation must be a stream id")?;
                    if !mid.is_document() {
                        bail!("Model relation of type Document with a target Stream ID ({mid}) must be of stream type 3 (MID) not {:?}", mid.r#type)
                    }
                }

                Ok(())

                /*
                  for each relation of type document with streamID {
                    // Ensure linked stream can be loaded and is a MID
                    const linkedMid = await ModelInstanceDocument.load(ceramic, midStreamId)

                    // Check for expected model the MID must use
                    const expectedModelStreamId = relationDefinition.model
                    if (expectedModelStreamId == null) {
                      continue
                    }

                    const foundModelStreamId = linkedMid.metadata.model.toString()
                    if (foundModelStreamId === expectedModelStreamId) {
                      // Exact model used
                      continue
                    }

                    // Other model used, check if it implements the expected interface
                    const linkedModel = await Model.load(ceramic, foundModelStreamId)
                    if (
                      linkedModel.content.version !== '1.0' &&
                      linkedModel.content.implements.includes(expectedModelStreamId)
                    ) {
                      continue
                    }

                    throw new Error(
                      `Relation on field ${fieldName} points to Stream ${midStreamId.toString()}, which belongs to Model ${foundModelStreamId}, but this Stream's Model (${model.id.toString()}) specifies that this relation must be to a Stream in the Model ${expectedModelStreamId}`
                    )
                }
                  */
            }
        }
    }

    /// "Validates the ModelInstanceDocument header against the Model definition"
    /// Only used when checking init events in js-ceramic and only checks accountRelation and unique
    /// As we don't have the header (specially unique value), we're not going to use this right now
    fn validate_header(&self, model: &ModelDefinition, unique: Option<&[u8]>) -> Result<()> {
        let account_relation = model.account_relation();
        match account_relation {
            ModelAccountRelation::Single => {
                if unique.is_some() {
                    bail!("ModelInstanceDocuments for models with SINGLE accountRelations must be created deterministically")
                } else {
                    Ok(())
                }
            }
            ModelAccountRelation::List => {
                if unique.is_none() {
                    bail!("ModelInstanceDocuments for models with LIST accountRelations must be created with a unique field");
                } else {
                    Ok(())
                }
            }
            ModelAccountRelation::Set => {
                if unique.is_none() {
                    bail!("ModelInstanceDocuments for models with SET accountRelations must be created with a unique field containing data from the fields providing the set semantics");
                } else {
                    Ok(())
                }
            }
            ModelAccountRelation::None => Ok(()),
        }
    }

    /// "Validates the ModelInstanceDocument unique constraints against the Model definition"
    /// Only used in js-ceramic on data events to validate that Set account relations use correctly include unique
    /// TODO: requires header, probably doesn't belong here but we do what we can
    fn validate_unique(&self, model: &ModelDefinition) -> Result<()> {
        let account_relation = model.account_relation();
        if matches!(account_relation, ModelAccountRelation::Set) {
            // if metadata.unique.is_none() => bail!("Missing unique metadata value")
            if self.content.is_none() {
                bail!("Missing content")
            }

            /*
                const unique = model.content.accountRelation.fields
                  .map((field) => {
                    const value = content[field]
                    return value ? String(value) : ''
                  })
                  .join('|')
                if (unique !== toString(metadata.unique)) {
                  throw new Error(
                    'Unique content fields value does not match metadata. If you are trying to change the value of these fields, this is causing this error: these fields values are not mutable.'
                  )
                }
            */
            Ok(())
        } else {
            Ok(())
        }
    }
}
