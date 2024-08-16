use std::{any::Any, sync::Arc};

use arrow::{
    array::BinaryArray,
    datatypes::{DataType, Field},
};
use datafusion::{
    common::{cast::as_binary_array, Result},
    logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility},
};

/// Applies a Ceramic data event to a document state returning the new document state.
#[derive(Debug)]
pub struct CeramicPatch {
    signature: Signature,
}

impl CeramicPatch {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![
                    DataType::Binary,
                    DataType::List(Arc::new(Field::new_list_field(DataType::Binary, true))),
                ]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for CeramicPatch {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "ceramic_patch"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }
    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        let events = as_binary_array(&args[0])?;
        let _states = as_binary_array(&args[0])?;
        Ok(ColumnarValue::Array(Arc::new(
            events.into_iter().collect::<BinaryArray>(),
        )))
    }
}
