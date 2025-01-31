//! Validate models reporting a list of validation errors.
//! An empty list implies the model is valid.

use std::{any::Any, sync::Arc};

use arrow::array::{Array as _, ListBuilder, StringBuilder};
use arrow_schema::Field;
use datafusion::{
    arrow::datatypes::DataType,
    common::cast::as_binary_array,
    logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility},
};

use crate::ceramic_stream::model::ModelDefinition;

use super::EventDataContainer;

make_udf_expr_and_func!(
    ModelValidate,
    model_validate,
    models previous,
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
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Binary, DataType::Binary]),
                Volatility::Immutable,
            ),
        }
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
    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        number_rows: usize,
    ) -> datafusion::common::Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        let model_definitions = as_binary_array(&args[0])?;
        let previous_model_definitions = as_binary_array(&args[1])?;
        let mut validation_errors = ListBuilder::new(StringBuilder::new());
        for (i, model_definition) in model_definitions.into_iter().enumerate() {
            let model_definition = model_definition
                .map(|m| serde_json::from_slice::<EventDataContainer<ModelDefinition>>(m));
            let previous = previous_model_definitions
                .is_valid(i)
                .then(|| {
                    serde_json::from_slice::<EventDataContainer<ModelDefinition>>(
                        previous_model_definitions.value(i),
                    )
                })
                .transpose()
                // TODO do we need an explicit error condition here?
                .ok()
                .flatten();
            let is_valid = match model_definition {
                None => false,
                Some(Ok(model_definition)) => {
                    if let Err(err) = model_definition
                        .content
                        .validate(previous.as_ref().map(|p| &p.content), None)
                    {
                        validation_errors.values().append_value(err.to_string());
                        true
                    } else {
                        // TODO does datafusion empty() function treat null as empty?
                        false
                    }
                }
                Some(Err(err)) => {
                    validation_errors.values().append_value(err.to_string());
                    true
                }
            };
            validation_errors.append(is_valid);
        }
        Ok(ColumnarValue::Array(Arc::new(validation_errors.finish())))
    }
}
