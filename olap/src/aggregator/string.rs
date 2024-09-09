use std::{any::Any, sync::Arc};

use arrow::{
    array::{BinaryBuilder, StringBuilder},
    datatypes::DataType,
};
use datafusion::{
    common::{cast::as_binary_array, exec_datafusion_err},
    logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility},
};

/// UDF that extracts the car event data.
#[derive(Debug)]
pub struct StrFromUtf8 {
    signature: Signature,
}

impl StrFromUtf8 {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Binary]),
                Volatility::Immutable,
            ),
        }
    }
    pub fn new_udf() -> ScalarUDF {
        ScalarUDF::new_from_impl(Self::new())
    }
}

impl ScalarUDFImpl for StrFromUtf8 {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "str_from_utf8"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::common::Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        let byte_strings = as_binary_array(&args[0])?;
        let mut strs = StringBuilder::new();
        for byte_string in byte_strings {
            if let Some(byte_string) = byte_string {
                strs.append_value(
                    std::str::from_utf8(byte_string)
                        .map_err(|err| exec_datafusion_err!("{err}"))?,
                );
            } else {
                strs.append_null()
            }
        }
        Ok(ColumnarValue::Array(Arc::new(strs.finish())))
    }
}
