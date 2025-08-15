//! Validate models reporting a list of validation errors.
//! An empty list implies the model is valid.

use std::{any::Any, sync::Arc};

use anyhow::Context as _;
use arrow::{
    array::{Array as _, AsArray as _, BinaryArray, ListBuilder, StringBuilder},
    datatypes::Int32Type,
};
use arrow_schema::Field;
use ceramic_core::StreamId;
use datafusion::{
    arrow::datatypes::DataType,
    common::{
        cast::{as_binary_array, as_int32_array},
        exec_err,
    },
    logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility},
};

use super::{
    dict_or_array::DictionaryOrArray,
    result::{OptionValidation, ResultValidation, ValidationResult},
    validation::model::ModelDefinition,
    EventDataContainer,
};

make_udf_expr_and_func!(
    ModelValidate,
    model_validate,
    models previous model,
    "computes a list of validation errors for the model.",
    model_validate_udf
);

#[derive(Debug)]
pub struct ModelValidate {
    signature: Signature,
}

impl Default for ModelValidate {
    fn default() -> Self {
        Self::new()
    }
}

impl ModelValidate {
    /// Construct new instance
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![
                        DataType::Binary,
                        DataType::Binary,
                        DataType::Binary,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Binary,
                        DataType::Binary,
                        // Special case the model being a dictionary
                        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Binary)),
                    ]),
                ],
                Volatility::Immutable,
            ),
        }
    }
    fn validate_model(
        &self,
        i: usize,
        model_definitions: &BinaryArray,
        previous_model_definitions: &BinaryArray,
        models: DictionaryOrArray,
    ) -> ValidationResult {
        let model_definition = maybe_fail!(maybe_fail!(model_definitions
            .is_valid(i)
            .then(|| {
                serde_json::from_slice::<EventDataContainer<ModelDefinition>>(
                    model_definitions.value(i),
                )
            })
            .ok_or_validation_internal_err("cannot validate null model definition"))
        .context("payload not a valid model definition")
        .map_to_validation_failure());

        let previous = maybe_fail!(previous_model_definitions
            .is_valid(i)
            .then(|| {
                serde_json::from_slice::<EventDataContainer<ModelDefinition>>(
                    previous_model_definitions.value(i),
                )
            })
            .transpose()
            .context("previous model definition is not a valid json document")
            // This is failure because to get to this point we know that the previous model
            // definition has been validated but not that it passed validation.
            .map_to_validation_failure());

        let model = maybe_fail!(models
            .value(i)
            .map(StreamId::try_from)
            .transpose()
            .context("failed to parse model as stream id")
            .map_to_validation_failure());

        model_definition
            .content
            .validate(previous.as_ref().map(|p| &p.content), None, model)
    }
}

impl ScalarUDFImpl for ModelValidate {
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
    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let number_rows = args.number_rows;
        let args = ColumnarValue::values_to_arrays(&args.args)?;
        let model_definitions = as_binary_array(&args[0])?;
        let previous_model_definitions = as_binary_array(&args[1])?;
        let models = args[2].clone();
        let models = if let Some(models) = models.as_dictionary_opt::<Int32Type>() {
            let keys = as_int32_array(models.keys())?;
            let values = as_binary_array(models.values())?;
            DictionaryOrArray::Dictionary { keys, values }
        } else {
            let models = as_binary_array(&models)?;
            DictionaryOrArray::Array(models)
        };
        let mut validation_errors = ListBuilder::new(StringBuilder::new());
        for i in 0..number_rows {
            let result = self.validate_model(
                i,
                model_definitions,
                previous_model_definitions,
                models.clone(),
            );
            match result {
                ValidationResult::Pass(_) => {}
                ValidationResult::Fail(errs) => {
                    for err in errs {
                        validation_errors.values().append_value(err);
                    }
                }
                ValidationResult::InternalError(err) => {
                    return exec_err!("failed to validate model: {err}")
                }
            }
            // Always append (empty or otherwise) to the validation_errors column as NULL values are not considered
            // empty.
            validation_errors.append(true);
        }
        Ok(ColumnarValue::Array(Arc::new(validation_errors.finish())))
    }
}
