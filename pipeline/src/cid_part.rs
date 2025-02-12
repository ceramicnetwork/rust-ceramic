//! Provides a scalar udf implementation that computes a partition value from CID bytes.
//! The current implementation keeps only the last byte.

use std::{any::Any, sync::Arc};

use arrow::{
    array::{AsArray as _, GenericByteArray, Int32Builder, PrimitiveArray},
    datatypes::{GenericBinaryType, Int32Type},
};
use datafusion::{
    arrow::datatypes::DataType,
    common::cast::as_binary_array,
    logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility},
};

make_udf_expr_and_func!(
    CidPart,
    cid_part,
    cids,
    "compute a partition value from a cid.",
    cid_part_udf
);

/// Scalar UDF that computes a partition value from CID bytes.
///
/// The current implementation returns the last bytes of the CID.
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
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Binary]),
                    // Enumerate all possible dictionary key types.
                    // We handle dictionary types explicitly because we can transform the
                    // dictionary values directly.
                    //
                    TypeSignature::Exact(vec![DataType::Dictionary(
                        Box::new(DataType::Int8),
                        Box::new(DataType::Binary),
                    )]),
                    TypeSignature::Exact(vec![DataType::Dictionary(
                        Box::new(DataType::Int16),
                        Box::new(DataType::Binary),
                    )]),
                    TypeSignature::Exact(vec![DataType::Dictionary(
                        Box::new(DataType::Int32),
                        Box::new(DataType::Binary),
                    )]),
                    TypeSignature::Exact(vec![DataType::Dictionary(
                        Box::new(DataType::Int64),
                        Box::new(DataType::Binary),
                    )]),
                    TypeSignature::Exact(vec![DataType::Dictionary(
                        Box::new(DataType::UInt8),
                        Box::new(DataType::Binary),
                    )]),
                    TypeSignature::Exact(vec![DataType::Dictionary(
                        Box::new(DataType::UInt16),
                        Box::new(DataType::Binary),
                    )]),
                    TypeSignature::Exact(vec![DataType::Dictionary(
                        Box::new(DataType::UInt32),
                        Box::new(DataType::Binary),
                    )]),
                    TypeSignature::Exact(vec![DataType::Dictionary(
                        Box::new(DataType::UInt64),
                        Box::new(DataType::Binary),
                    )]),
                ],
                Volatility::Immutable,
            ),
        }
    }
    fn map_cids(
        &self,
        cids: &GenericByteArray<GenericBinaryType<i32>>,
        number_rows: usize,
    ) -> datafusion::common::Result<PrimitiveArray<Int32Type>> {
        let mut parts = Int32Builder::with_capacity(number_rows);
        for cid in cids {
            if let Some(cid) = cid {
                parts.append_value(cid[cid.len() - 1] as i32);
            } else {
                parts.append_null()
            }
        }
        Ok(parts.finish())
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
    fn return_type(&self, args: &[DataType]) -> datafusion::common::Result<DataType> {
        if let DataType::Dictionary(key, _value) = &args[0] {
            Ok(DataType::Dictionary(
                key.to_owned(),
                Box::new(DataType::Int32),
            ))
        } else {
            Ok(DataType::Int32)
        }
    }
    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        number_rows: usize,
    ) -> datafusion::common::Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        if let Some(dict) = args[0].as_any_dictionary_opt() {
            // Map over the dictionary values
            //
            // We could also construct a new dictionary with new keys as its likely that
            // many cids map to the same partition value.
            // However its generally assumed that if a dictionary is being used it is
            // already low cardinality. Therefore any potential gains are likely not
            // significant.
            let cids = as_binary_array(dict.values())?;
            let ids = self.map_cids(cids, number_rows)?;
            Ok(ColumnarValue::Array(dict.with_values(Arc::new(ids))))
        } else {
            let cids = as_binary_array(&args[0])?;
            let ids = self.map_cids(cids, number_rows)?;
            Ok(ColumnarValue::Array(Arc::new(ids)))
        }
    }
}
