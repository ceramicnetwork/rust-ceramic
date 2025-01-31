use anyhow::Context as _;
use schemars::Schema;

use crate::aggregator::result::{OptionValidation as _, ResultValidation, ValidationResult};

use super::model::{
    ModelDefinitionV2, ModelRelationDefinitionV2, ModelRelationViewDefinitionV2,
    ModelRelationsDefinitionV2, ModelViewDefinitionV2, ModelViewsDefinitionV2,
};

pub struct InterfaceUtil {}

/// Helpers to validate interface details.
impl InterfaceUtil {
    /// TODO: this is basically unimplemented, but https://github.com/getsentry/json-schema-diff looks super promising!
    /// only chekcing they actually parse as json schemas for a given draft and are objects rn
    /// the JS code compares the schemas, resolving $ref keys, making sure rules are kept in
    /// the implementator (e.g. types match, rules for numbers, enums, etc match), special keys
    /// like allOf, anyOf are adhered to and more.
    pub fn validate_schema_implementation(
        expected: &Schema,
        implemented: &Schema,
    ) -> ValidationResult {
        let _ = maybe_fail!(jsonschema::validator_for(expected.as_value())
            .context("cannot get validator for expected schema")
            .map_to_validation_internal_err());
        let _ = maybe_fail!(jsonschema::validator_for(implemented.as_value())
            .context("cannot get validator for implemented schema")
            .map_to_validation_internal_err());

        let _expected = maybe_fail!(expected
            .as_object()
            .ok_or_validation_internal_err("interface schema must be an object"));
        let _implemented = maybe_fail!(implemented
            .as_object()
            .ok_or_validation_internal_err("model schema must be an object"));

        ValidationResult::Pass(())
    }

    // TODO: review the is_match functions.. can maybe just use equality on the elements? unclear the serialization of optional values
    // JS code uses lodash isMatch, so it could matter if we serialize as null/undefined/missing
    // e.g. model: null => impl must be null
    //      model: undefined => impl must be undefined
    //      model key skipped => any value works
    pub fn is_valid_relations_implementation(
        expected: &ModelRelationsDefinitionV2,
        implemented: &ModelRelationsDefinitionV2,
    ) -> bool {
        expected.iter().all(|(key, expected_value)| {
            implemented.get(key).map_or(false, |implemented_value| {
                Self::relations_match(implemented_value, expected_value)
            })
        })
    }

    /// Validates that the views on the expected (interface) are all covered by the model (implemented)
    /// Same question as [`Self::is_valid_relations_implementation`] related to handling None (must be None or is anything allowed?)
    pub fn is_valid_views_implementation(
        expected: &ModelViewsDefinitionV2,
        implemented: &ModelViewsDefinitionV2,
    ) -> bool {
        expected.iter().all(|(key, expected_value)| {
            implemented.get(key).map_or(false, |implemented_value| {
                Self::views_match(implemented_value, expected_value)
            })
        })
    }

    /// Validates that all the immutableFields from the expected (interface) are included by the model (implemented)
    /// Same question as [`Self::is_valid_relations_implementation`] related to handling None (must be None or is anything allowed?)
    pub fn is_valid_immutability_implementation(
        expected: &ModelDefinitionV2,
        implemented: &ModelDefinitionV2,
    ) -> bool {
        // use v2
        match &expected.immutable_fields {
            Some(expected_fields) => {
                let impld_fields = match &implemented.immutable_fields {
                    Some(v) => v,
                    None => return false,
                };

                expected_fields.iter().all(|f| impld_fields.contains(f))
            }
            None => true,
        }
    }

    /// If None on Document { model: Option<StreamID> } must equal None, we don't need this and can simply use equality instead
    fn relations_match(
        implemented: &ModelRelationDefinitionV2,
        expected: &ModelRelationDefinitionV2,
    ) -> bool {
        match (implemented, expected) {
            (
                ModelRelationDefinitionV2::Document { model: impl_model },
                ModelRelationDefinitionV2::Document {
                    model: expected_model,
                },
            ) => {
                // TODO: If expected is None, it means we don't care about the model field, right? Or does it have to be None in both?
                expected_model
                    .as_ref()
                    .map_or(true, |exp_id| impl_model.as_ref() == Some(exp_id))
            }
            (a, b) => a == b,
        }
    }

    /// If None on Document { model: Option<StreamID> } must equal None, we don't need this and can simply use equality instead
    fn views_match(implemented: &ModelViewDefinitionV2, expected: &ModelViewDefinitionV2) -> bool {
        match (implemented, expected) {
            (ModelViewDefinitionV2::Relation(imp), ModelViewDefinitionV2::Relation(exp)) => {
                match (imp, exp) {
                    (
                        ModelRelationViewDefinitionV2::Document {
                            model: impl_model,
                            property: impl_prop,
                        },
                        ModelRelationViewDefinitionV2::Document {
                            model: expected_model,
                            property: expected_prop,
                        },
                    ) => {
                        // TODO: If expected is None, it means we don't care about the model field?
                        impl_prop == expected_prop
                            && expected_model.as_ref().map_or(true, |expected_id| {
                                impl_model.as_ref() == Some(expected_id)
                            })
                    }
                    // for everything else, we can just check if they're equal as there's no optional subtyping
                    (imp, exp) => imp == exp,
                }
            }
            (a, b) => a == b,
        }
    }
}
