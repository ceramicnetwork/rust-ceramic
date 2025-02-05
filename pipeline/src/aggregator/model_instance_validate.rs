//! Validate model instances reporting a list of validation errors.
//! An empty list implies the model is valid.

use std::{any::Any, sync::Arc};

use arrow::{
    array::{Array as _, AsArray as _, ListBuilder, StringBuilder},
    datatypes::Int32Type,
};
use arrow_schema::Field;
use cid::Cid;
use datafusion::{
    arrow::datatypes::DataType,
    common::cast::{as_binary_array, as_int32_array},
    logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility},
};

use crate::ceramic_stream::{
    model::ModelDefinition, model_instance::ModelInstance, schema_validator::SchemaValidator,
};

use super::EventDataContainer;

make_udf_expr_and_func!(
    ModelInstanceValidate,
    model_instance_validate,
    instance previous model_version model_definition,
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
                    ]),
                    // Special case the model_version parameter as its common for be dictionary
                    // encoded.
                    TypeSignature::Exact(vec![
                        DataType::Binary,
                        DataType::Binary,
                        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Binary)),
                        DataType::Binary,
                    ]),
                ],
                Volatility::Immutable,
            ),
            validator: SchemaValidator::new(1_000, true),
        }
    }
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
    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        number_rows: usize,
    ) -> datafusion::common::Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        let model_instance = as_binary_array(&args[0])?;
        let previous = as_binary_array(&args[1])?;

        let model_version_cid =
            if let Some(model_versions) = args[2].as_dictionary_opt::<Int32Type>() {
                tracing::debug!("dictionary binary model_version");
                let keys = as_int32_array(model_versions.keys())?;
                let values = as_binary_array(model_versions.values())?;
                Box::new(|idx| {
                    keys.is_valid(idx)
                        .then(|| Cid::read_bytes(values.value(keys.value(idx) as usize)))
                }) as Box<dyn Fn(usize) -> Option<cid::Result<Cid>>>
            } else {
                let model_versions = as_binary_array(&args[2])?;
                tracing::debug!("binary model_version");
                Box::new(|idx| {
                    model_versions
                        .is_valid(idx)
                        .then(|| Cid::read_bytes(model_versions.value(idx)))
                }) as Box<dyn Fn(usize) -> Option<cid::Result<Cid>>>
            };
        let model_definitions = as_binary_array(&args[3])?;
        let mut validation_errors = ListBuilder::new(StringBuilder::new());
        for (i, instance) in model_instance.into_iter().enumerate() {
            let instance = instance.map(|m| serde_json::from_slice::<ModelInstance>(m));
            let previous = previous
                .is_valid(i)
                .then(|| serde_json::from_slice::<ModelInstance>(previous.value(i)))
                .transpose()
                // TODO do we need an explicit error condition here?
                .ok()
                .flatten();
            let model_version = model_version_cid(i)
                .transpose()
                // TODO do we need an explicit error condition here?
                .ok()
                .flatten()
                //TODO: must have model version
                .unwrap();
            let model_definition = model_definitions
                .is_valid(i)
                .then(|| {
                    serde_json::from_slice::<EventDataContainer<ModelDefinition>>(
                        model_definitions.value(i),
                    )
                })
                .transpose()
                // TODO do we need an explicit error condition here?
                .ok()
                .flatten()
                //TODO: must have model definition
                .unwrap();
            let is_valid = match instance {
                None => false,
                Some(Ok(instance)) => {
                    if let Err(err) = instance.validate(
                        &self.validator,
                        previous.as_ref(),
                        &model_version,
                        &model_definition.content,
                    ) {
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
