//! Provides single method that can extract the dimensions of an event.
//! This is in practice just an alias for array_extract(map_extract(dimensions, <name>), 1).

use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::datatypes::DataType,
    functions_array::{extract::array_element_udf, map_extract::map_extract_udf},
    logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature},
    scalar::ScalarValue,
};

/// ScalarUDF to convert a binary CID into a string for easier inspection.
#[derive(Debug)]
pub struct DimensionExtract {
    map_extract: Arc<ScalarUDF>,
    array_extract: Arc<ScalarUDF>,
}

impl Default for DimensionExtract {
    fn default() -> Self {
        Self::new()
    }
}

impl DimensionExtract {
    /// Construct new instance
    pub fn new() -> Self {
        Self {
            map_extract: map_extract_udf(),
            array_extract: array_element_udf(),
        }
    }
}

impl ScalarUDFImpl for DimensionExtract {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "dimension_extract"
    }
    fn signature(&self) -> &Signature {
        &self.map_extract.signature()
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> datafusion::error::Result<Vec<DataType>> {
        self.map_extract.coerce_types(arg_types)
    }
    fn return_type(&self, args: &[DataType]) -> datafusion::common::Result<DataType> {
        let ret = self.map_extract.inner().return_type(args)?;
        if let DataType::List(field) = ret {
            Ok(field.data_type().clone())
        } else {
            Err(datafusion::error::DataFusionError::Internal(
                "map_extract should always return a list datatype".to_owned(),
            ))
        }
    }
    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        number_rows: usize,
    ) -> datafusion::error::Result<ColumnarValue> {
        let mapped = self.map_extract.invoke_batch(args, number_rows)?;
        self.array_extract.invoke_batch(
            &[mapped, ColumnarValue::Scalar(ScalarValue::Int64(Some(1)))],
            number_rows,
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::stream_id_string::StreamIdString;

    use super::DimensionExtract;

    use std::{str::FromStr as _, sync::Arc};

    use arrow::{
        array::{ArrayRef, BinaryDictionaryBuilder, MapBuilder, MapFieldNames, StringBuilder},
        datatypes::Int32Type,
        util::pretty::pretty_format_batches,
    };
    use ceramic_core::StreamId;
    use datafusion::{
        arrow::array::StructArray,
        logical_expr::{expr::ScalarFunction, ScalarUDF},
        prelude::{col, lit, Expr, SessionContext},
    };
    use expect_test::expect;
    use test_log::test;

    #[test(tokio::test)]
    async fn dimension_extract() -> anyhow::Result<()> {
        let mut dimensions = MapBuilder::new(
            Some(MapFieldNames {
                entry: "entries".to_string(),
                key: "key".to_string(),
                value: "value".to_string(),
            }),
            StringBuilder::new(),
            BinaryDictionaryBuilder::<Int32Type>::new(),
        );
        dimensions.keys().append_value("model");
        dimensions.values().append_value(
            StreamId::from_str("k2t6wzhjp5kk57kldpzlnneq20q440sae8azvedfbq5alg66hzl9u5estye4fp")?
                .to_vec(),
        );
        dimensions.append(true).unwrap();

        dimensions.keys().append_value("model");
        dimensions.values().append_value(
            StreamId::from_str("k2t6wzhjp5kk3c0kf11cq8b23nh85yrriywu6vhoxwsai53nc53wxr7kw31f8y")?
                .to_vec(),
        );
        dimensions.append(true).unwrap();

        dimensions.keys().append_value("model");
        dimensions.values().append_value(
            StreamId::from_str("k2t6wzhjp5kk4iow3p6qvu06bxhjt7exsnwzz5advmpyvzsrsefus9bmxjywtg")?
                .to_vec(),
        );
        dimensions.append(true).unwrap();

        dimensions.keys().append_value("model");
        dimensions.values().append_value(
            StreamId::from_str("k2t6wzhjp5kk5jcv308pv15q50yors7io0t58mtwvjpudkqt05gmjbrr14s3zi")?
                .to_vec(),
        );
        dimensions.append(true).unwrap();

        let batch = StructArray::try_from(vec![(
            "dimensions",
            Arc::new(dimensions.finish()) as ArrayRef,
        )])?;
        let stream_id_string = Arc::new(ScalarUDF::from(StreamIdString::new()));
        let dimension_extract = Arc::new(ScalarUDF::from(DimensionExtract::new()));
        let ctx = SessionContext::new();
        let output = ctx
            .read_batch(batch.into())?
            .select(vec![Expr::ScalarFunction(ScalarFunction::new_udf(
                stream_id_string,
                vec![Expr::ScalarFunction(ScalarFunction::new_udf(
                    dimension_extract,
                    vec![col("dimensions"), lit("model")],
                ))],
            ))])?
            .collect()
            .await?;
        let output = pretty_format_batches(&output)?;
        expect![[r#"
            +-----------------------------------------------------------------------+
            | stream_id_string(dimension_extract(?table?.dimensions,Utf8("model"))) |
            +-----------------------------------------------------------------------+
            | k2t6wzhjp5kk57kldpzlnneq20q440sae8azvedfbq5alg66hzl9u5estye4fp        |
            | k2t6wzhjp5kk3c0kf11cq8b23nh85yrriywu6vhoxwsai53nc53wxr7kw31f8y        |
            | k2t6wzhjp5kk4iow3p6qvu06bxhjt7exsnwzz5advmpyvzsrsefus9bmxjywtg        |
            | k2t6wzhjp5kk5jcv308pv15q50yors7io0t58mtwvjpudkqt05gmjbrr14s3zi        |
            +-----------------------------------------------------------------------+"#]]
        .assert_eq(&output.to_string());
        Ok(())
    }
}
