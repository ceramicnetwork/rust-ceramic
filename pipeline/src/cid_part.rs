//! Provides a scalar udf implementation that computes a partition value from CID bytes.
//! The current implementation keeps only the last byte.

use std::{any::Any, sync::Arc};

use arrow::array::Int32Builder;
use datafusion::{
    arrow::datatypes::DataType,
    common::cast::as_binary_array,
    logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility},
};

/// Scalar UDF that computes the a partition value from CID bytes.
///
/// The current implementation retruns the last bytes of the CID.
#[derive(Debug)]
pub struct CidPart {
    signature: Signature,
}

impl Default for CidPart {
    fn default() -> Self {
        Self::new()
    }
}

impl CidPart {
    /// Construct new instance
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Binary]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for CidPart {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "cid_part"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Int32)
    }
    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        number_rows: usize,
    ) -> datafusion::common::Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        let cids = as_binary_array(&args[0])?;
        let mut parts = Int32Builder::with_capacity(number_rows);
        for cid in cids {
            if let Some(cid) = cid {
                parts.append_value(cid[cid.len() - 1] as i32);
            } else {
                parts.append_null()
            }
        }
        Ok(ColumnarValue::Array(Arc::new(parts.finish())))
    }
}
