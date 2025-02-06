//! Provides a scalar udf implementation that converts CID bytes to a utf8 string.

use std::{any::Any, sync::Arc};

use arrow::array::{Array as _, GenericListArray};
use arrow_schema::Field;
use cid::Cid;
use datafusion::{
    arrow::{array::StringBuilder, datatypes::DataType},
    common::{
        cast::{as_binary_array, as_list_array},
        exec_datafusion_err,
    },
    logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility},
};

// Length of a typical Ceramic CID as a UTF8 string in bytes.
const CID_STRING_BYTES: usize = 60;

/// ScalarUDF to convert a binary CID into a string for easier inspection.
#[derive(Debug)]
pub struct CidString {
    signature: Signature,
}

impl Default for CidString {
    fn default() -> Self {
        Self::new()
    }
}

impl CidString {
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

impl ScalarUDFImpl for CidString {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "cid_string"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        number_rows: usize,
    ) -> datafusion::error::Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        let cids = as_binary_array(&args[0])?;
        let mut strs = StringBuilder::with_capacity(number_rows, CID_STRING_BYTES * number_rows);
        for cid in cids {
            if let Some(cid) = cid {
                strs.append_value(
                    Cid::read_bytes(cid)
                        .map_err(|err| exec_datafusion_err!("Error {err}"))?
                        .to_string(),
                );
            } else {
                strs.append_null()
            }
        }
        Ok(ColumnarValue::Array(Arc::new(strs.finish())))
    }
}

/// ScalarUDF to convert a a list of binary CIDs into a list of strings for easier inspection.
#[derive(Debug)]
pub struct CidStringList {
    signature: Signature,
}

impl Default for CidStringList {
    fn default() -> Self {
        Self::new()
    }
}

impl CidStringList {
    /// Construct new instance
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::new_list(DataType::Binary, true)]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for CidStringList {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_cid_string"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::new_list(DataType::Utf8, true))
    }
    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        _number_rows: usize,
    ) -> datafusion::error::Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        let all_cids = as_list_array(&args[0])?;
        // The list structure is not modified.
        // We can map over the values array and reuse the list offsets.
        let values = all_cids.values();
        let cid_count = values.len();
        let mut new_values = StringBuilder::with_capacity(cid_count, CID_STRING_BYTES * cid_count);
        let cids = as_binary_array(&values)?;
        for cid in cids {
            if let Some(cid) = cid {
                new_values.append_value(
                    Cid::read_bytes(cid)
                        .map_err(|err| exec_datafusion_err!("Error {err}"))?
                        .to_string(),
                );
            } else {
                new_values.append_null()
            }
        }
        let new_list = GenericListArray::try_new(
            Arc::new(Field::new_list_field(DataType::Utf8, true)),
            all_cids.offsets().to_owned(),
            Arc::new(new_values.finish()),
            all_cids.nulls().cloned(),
        )?;

        Ok(ColumnarValue::Array(Arc::new(new_list)))
    }
}

#[cfg(test)]
mod tests {
    use super::{CidString, CidStringList};

    use std::{str::FromStr as _, sync::Arc};

    use arrow::{
        array::{ArrayRef, ListBuilder},
        util::pretty::pretty_format_batches,
    };
    use cid::Cid;
    use datafusion::{
        arrow::array::{BinaryBuilder, StructArray},
        logical_expr::{expr::ScalarFunction, ScalarUDF},
        prelude::{col, Expr, SessionContext},
    };
    use expect_test::expect;
    use test_log::test;

    #[test(tokio::test)]
    async fn cid_string() -> anyhow::Result<()> {
        let mut cids = BinaryBuilder::new();
        cids.append_value(
            Cid::from_str("baeabeihqujx543mwoqik7lltjjtobjmdewsygp5s34mydy3web3dxhoide")?
                .to_bytes(),
        );
        cids.append_value(
            Cid::from_str("baeabeih6c7vkvijmvu22oqwwirsukfci46evfp6rrydq4bavmoxw5yrzaq")?
                .to_bytes(),
        );
        cids.append_value(
            Cid::from_str("baeabeiayakacgjkantxde2puuxqfrrq72cih4gykxov7gpdiypoz4d3oya")?
                .to_bytes(),
        );
        cids.append_value(
            Cid::from_str("baeabeiaclaqgmmybteclwcb6p6hth24pwezy3chaghryslttst34d2lrny")?
                .to_bytes(),
        );
        cids.append_value(
            Cid::from_str("baeabeibou6io3hgsapzk5kzdj6gxw5whbsa4oruhndgfonei35bugpqswm")?
                .to_bytes(),
        );

        let batch = StructArray::try_from(vec![("cid", Arc::new(cids.finish()) as ArrayRef)])?;
        let cid_string = Arc::new(ScalarUDF::from(CidString::new()));
        let ctx = SessionContext::new();
        let output = ctx
            .read_batch(batch.into())?
            .select(vec![Expr::ScalarFunction(ScalarFunction::new_udf(
                cid_string,
                vec![col("cid")],
            ))])?
            .collect()
            .await?;
        let output = pretty_format_batches(&output)?;
        expect![[r#"
            +-------------------------------------------------------------+
            | cid_string(?table?.cid)                                     |
            +-------------------------------------------------------------+
            | baeabeihqujx543mwoqik7lltjjtobjmdewsygp5s34mydy3web3dxhoide |
            | baeabeih6c7vkvijmvu22oqwwirsukfci46evfp6rrydq4bavmoxw5yrzaq |
            | baeabeiayakacgjkantxde2puuxqfrrq72cih4gykxov7gpdiypoz4d3oya |
            | baeabeiaclaqgmmybteclwcb6p6hth24pwezy3chaghryslttst34d2lrny |
            | baeabeibou6io3hgsapzk5kzdj6gxw5whbsa4oruhndgfonei35bugpqswm |
            +-------------------------------------------------------------+"#]]
        .assert_eq(&output.to_string());
        Ok(())
    }
    #[test(tokio::test)]
    async fn cid_string_list() -> anyhow::Result<()> {
        let mut cids = ListBuilder::new(BinaryBuilder::new());
        cids.values().append_value(
            Cid::from_str("baeabeihqujx543mwoqik7lltjjtobjmdewsygp5s34mydy3web3dxhoide")?
                .to_bytes(),
        );
        cids.values().append_value(
            Cid::from_str("baeabeih6c7vkvijmvu22oqwwirsukfci46evfp6rrydq4bavmoxw5yrzaq")?
                .to_bytes(),
        );
        cids.values().append_value(
            Cid::from_str("baeabeiayakacgjkantxde2puuxqfrrq72cih4gykxov7gpdiypoz4d3oya")?
                .to_bytes(),
        );
        cids.append(true);
        cids.values().append_value(
            Cid::from_str("baeabeiaclaqgmmybteclwcb6p6hth24pwezy3chaghryslttst34d2lrny")?
                .to_bytes(),
        );
        cids.values().append_value(
            Cid::from_str("baeabeibou6io3hgsapzk5kzdj6gxw5whbsa4oruhndgfonei35bugpqswm")?
                .to_bytes(),
        );
        cids.append(true);
        let batch = StructArray::try_from(vec![("cids", Arc::new(cids.finish()) as ArrayRef)])?;
        let cid_string_list = Arc::new(ScalarUDF::from(CidStringList::new()));
        let ctx = SessionContext::new();
        let output = ctx
            .read_batch(batch.into())?
            .select(vec![Expr::ScalarFunction(ScalarFunction::new_udf(
                cid_string_list,
                vec![col("cids")],
            ))])?
            .collect()
            .await?;
        let output = pretty_format_batches(&output)?;
        expect![[r#"
            +-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
            | array_cid_string(?table?.cids)                                                                                                                                                          |
            +-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
            | [baeabeihqujx543mwoqik7lltjjtobjmdewsygp5s34mydy3web3dxhoide, baeabeih6c7vkvijmvu22oqwwirsukfci46evfp6rrydq4bavmoxw5yrzaq, baeabeiayakacgjkantxde2puuxqfrrq72cih4gykxov7gpdiypoz4d3oya] |
            | [baeabeiaclaqgmmybteclwcb6p6hth24pwezy3chaghryslttst34d2lrny, baeabeibou6io3hgsapzk5kzdj6gxw5whbsa4oruhndgfonei35bugpqswm]                                                              |
            +-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+"#]].assert_eq(&output.to_string());
        Ok(())
    }
}
