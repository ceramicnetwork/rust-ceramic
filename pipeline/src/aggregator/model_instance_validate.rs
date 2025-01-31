//! Validate model instances reporting a list of validation errors.
//! An empty list implies the model is valid.

use std::{any::Any, sync::Arc};

use anyhow::Context as _;
use arrow::{
    array::{Array as _, AsArray as _, BinaryArray, ListBuilder, StringBuilder, UInt32Array},
    datatypes::Int32Type,
};
use arrow_schema::Field;
use cid::Cid;
use datafusion::{
    arrow::datatypes::DataType,
    common::{
        cast::{as_binary_array, as_int32_array, as_uint32_array},
        exec_err,
    },
    logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility},
};
use json_patch::Patch;

use super::{
    dict_or_array::DictionaryOrArray,
    result::{OptionValidation as _, ResultValidation as _, ValidationResult},
    validation::{
        model::ModelDefinition, model_instance::ModelInstance, schema_validator::SchemaValidator,
    },
    EventDataContainer,
};

make_udf_expr_and_func!(
    ModelInstanceValidate,
    model_instance_validate,
    instance patch model_version model_definition event_height unique,
    "computes a list of validation errors for the model instance.",
    model_instance_validate_udf
);

#[derive(Debug)]
pub struct ModelInstanceValidate {
    signature: Signature,
    validator: SchemaValidator,
}

impl Default for ModelInstanceValidate {
    fn default() -> Self {
        Self::new()
    }
}

impl ModelInstanceValidate {
    /// Construct new instance
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![
                        DataType::Binary,
                        DataType::Binary,
                        DataType::Binary,
                        DataType::Binary,
                        DataType::UInt32,
                        DataType::Binary,
                    ]),
                    // Special case the unique parameter as its common
                    // for it to be dictionary encoded.
                    TypeSignature::Exact(vec![
                        DataType::Binary,
                        DataType::Binary,
                        DataType::Binary,
                        DataType::Binary,
                        DataType::UInt32,
                        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Binary)),
                    ]),
                ],
                Volatility::Immutable,
            ),
            validator: SchemaValidator::new(1_000, true),
        }
    }
    // We could place all of the columns into a struct but that doesn't
    fn validate_instance(&self, i: usize, columns: Columns) -> ValidationResult {
        let instance = maybe_fail!(maybe_fail!(columns
            .model_instance
            .is_valid(i)
            .then(|| serde_json::from_slice::<ModelInstance>(columns.model_instance.value(i)))
            .ok_or_validation_internal_err("cannot validate null instance document"))
        .context("instance payload not a valid json documnet")
        .map_to_validation_failure());
        let patch = maybe_fail!(columns
            .patch
            .is_valid(i)
            .then(|| serde_json::from_slice::<EventDataContainer<Patch>>(columns.patch.value(i)))
            .transpose()
            .context("instance patch is not a valid json patch document")
            // This is an error because to get to this point we know that the patch has already
            // been applied.
            .map_to_validation_internal_err());
        let model_version = maybe_fail!(maybe_fail!(columns
            .model_versions
            .is_valid(i)
            .then(|| Cid::read_bytes(columns.model_versions.value(i)))
            .ok_or_validation_internal_err(
                "cannot validate instance against an unknown model version"
            ))
        .context("model version must be a valid CID")
        .map_to_validation_internal_err());
        let model_definition = maybe_fail!(maybe_fail!(columns
            .model_definitions
            .is_valid(i)
            .then(|| {
                serde_json::from_slice::<EventDataContainer<ModelDefinition>>(
                    columns.model_definitions.value(i),
                )
            })
            .ok_or_validation_internal_err(
                "cannot validate instance against an unknown model definition"
            ))
        .context("model definition is not a valid model definition")
        .map_to_validation_internal_err());
        let event_height = maybe_fail!(columns
            .event_heights
            .is_valid(i)
            .then(|| columns.event_heights.value(i))
            .ok_or_validation_internal_err(
                "cannot validate an instance with an unknown event event_height"
            ));

        let unique = columns.uniques.value(i);
        instance.validate(
            &self.validator,
            patch.as_ref().map(|p| &p.content),
            &model_version,
            &model_definition.content,
            event_height,
            unique,
        )
    }
}

#[derive(Clone)]
struct Columns<'a> {
    model_instance: &'a BinaryArray,
    patch: &'a BinaryArray,
    model_versions: &'a BinaryArray,
    model_definitions: &'a BinaryArray,
    event_heights: &'a UInt32Array,
    uniques: DictionaryOrArray<'a>,
}

impl ScalarUDFImpl for ModelInstanceValidate {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "ceramic_model_validate"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::List(
            Field::new_list_field(DataType::Utf8, true).into(),
        ))
    }
    fn invoke_batch<'b>(
        &self,
        args: &[ColumnarValue],
        number_rows: usize,
    ) -> datafusion::common::Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        let model_instance = as_binary_array(&args[0])?;
        let patch = as_binary_array(&args[1])?;
        let model_versions = as_binary_array(&args[2])?;
        let model_definitions = as_binary_array(&args[3])?;
        let event_heights = as_uint32_array(&args[4])?;
        let uniques = &args[5];
        let uniques = if let Some(uniques) = uniques.as_dictionary_opt::<Int32Type>() {
            let keys = as_int32_array(uniques.keys())?;
            let values = as_binary_array(uniques.values())?;
            DictionaryOrArray::Dictionary { keys, values }
        } else {
            let uniques = as_binary_array(uniques)?;
            DictionaryOrArray::Array(uniques)
        };
        let columns = Columns {
            model_instance,
            patch,
            model_versions,
            model_definitions,
            event_heights,
            uniques,
        };
        let mut validation_errors = ListBuilder::new(StringBuilder::new());

        for i in 0..number_rows {
            let result = self.validate_instance(i, columns.clone());
            match result {
                ValidationResult::Pass(_) => {}
                ValidationResult::Fail(errs) => {
                    for err in errs {
                        validation_errors.values().append_value(&err);
                    }
                }
                ValidationResult::InternalError(err) => {
                    return exec_err!("failed to validate model instance: {err}");
                }
            }
            // Always append (empty or otherwise) to the validation_errors column as NULL values are not considered
            // empty.
            validation_errors.append(true);
        }
        Ok(ColumnarValue::Array(Arc::new(validation_errors.finish())))
    }
}
