use super::*;
use crate::types::{ConclusionData, ConclusionEvent, ConclusionInit};
use arrow::{array::StringBuilder, datatypes::DataType, util::pretty::pretty_format_batches};
use cid::Cid;
use datafusion::{
    common::{cast::as_binary_array, exec_datafusion_err},
    execution::context::SessionContext,
    functions_aggregate::expr_fn::array_agg,
    logical_expr::{
        col, expr::ScalarFunction, ColumnarValue, Expr, ScalarUDF, ScalarUDFImpl, Signature,
        TypeSignature, Volatility,
    },
};
use expect_test::expect;
use std::{any::Any, str::FromStr, sync::Arc};

#[derive(Debug)]
pub struct CidString {
    signature: Signature,
}

impl CidString {
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
    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::common::Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        let cids = as_binary_array(&args[0])?;
        let mut strs = StringBuilder::new();
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

/// Tests the conversion of ConclusionEvents to Arrow RecordBatch.
///
/// This test creates mock ConclusionEvents (both Data and Time events),
/// converts them to a RecordBatch using the conclusion_events_to_record_batch function,
/// and then verifies that the resulting RecordBatch contains the expected data.
///
/// The test checks:
/// 1. The number of rows in the RecordBatch
/// 2. The schema of the RecordBatch
/// 3. The content of each column in the RecordBatch
#[tokio::test]
async fn test_conclusion_events_to_record_batch() {
    // Create mock ConclusionEvents
    let events = vec![
        ConclusionEvent::Data(ConclusionData {
            event_cid: Cid::from_str("baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu")
                .unwrap(),
            init: ConclusionInit {
                stream_cid: Cid::from_str(
                    "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                )
                .unwrap(),
                controller: "did:key:test1".to_string(),
                dimensions: vec![],
            },
            previous: vec![],
            data: vec![1, 2, 3],
            index: 0,
        }),
        ConclusionEvent::Data(ConclusionData {
            event_cid: Cid::from_str("baeabeid2w5pgdsdh25nah7batmhxanbj3x2w2is3atser7qxboyojv236q")
                .unwrap(),
            init: ConclusionInit {
                stream_cid: Cid::from_str(
                    "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                )
                .unwrap(),
                controller: "did:key:test1".to_string(),
                dimensions: vec![],
            },
            previous: vec![Cid::from_str(
                "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
            )
            .unwrap()],
            data: vec![4, 5, 6],
            index: 1,
        }),
        ConclusionEvent::Time(ConclusionTime {
            event_cid: Cid::from_str("baeabeidtub3bnbojbickf6d4pqscaw6xpt5ksgido7kcsg2jyftaj237di")
                .unwrap(),
            previous: vec![Cid::from_str(
                "baeabeid2w5pgdsdh25nah7batmhxanbj3x2w2is3atser7qxboyojv236q",
            )
            .unwrap()],
            init: ConclusionInit {
                stream_cid: Cid::from_str(
                    "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                )
                .unwrap(),
                controller: "did:key:test1".to_string(),
                dimensions: vec![],
            },
            index: 2,
        }),
        ConclusionEvent::Data(ConclusionData {
            event_cid: Cid::from_str("baeabeiewqcj4bwhcssizv5kcyvsvm57bxghjpqshnbzkc6rijmwb4im4yq")
                .unwrap(),
            init: ConclusionInit {
                stream_cid: Cid::from_str(
                    "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                )
                .unwrap(),
                controller: "did:key:test1".to_string(),
                dimensions: vec![],
            },
            previous: vec![
                Cid::from_str("baeabeidtub3bnbojbickf6d4pqscaw6xpt5ksgido7kcsg2jyftaj237di")
                    .unwrap(),
                Cid::from_str("baeabeid2w5pgdsdh25nah7batmhxanbj3x2w2is3atser7qxboyojv236q")
                    .unwrap(),
            ],
            data: vec![7, 8, 9],
            index: 3,
        }),
    ];
    // Convert events to RecordBatch
    let record_batch = conclusion_events_to_record_batch(&events).unwrap();

    let ctx = SessionContext::new();
    ctx.register_batch("conclusion_feed", record_batch).unwrap();

    let cid_string = Arc::new(ScalarUDF::from(CidString::new()));

    let doc_state = ctx
        .table("conclusion_feed")
        .await
        .unwrap()
        .unnest_columns(&["previous"])
        .unwrap()
        .select(vec![
            col("index"),
            col("event_type"),
            Expr::ScalarFunction(ScalarFunction::new_udf(
                cid_string.clone(),
                vec![col("stream_cid")],
            ))
            .alias("stream_cid"),
            col("controller"),
            Expr::ScalarFunction(ScalarFunction::new_udf(
                cid_string.clone(),
                vec![col("event_cid")],
            ))
            .alias("event_cid"),
            col("data"),
            Expr::ScalarFunction(ScalarFunction::new_udf(cid_string, vec![col("previous")]))
                .alias("previous"),
        ])
        .unwrap()
        .aggregate(
            vec![
                col("index"),
                col("event_type"),
                col("stream_cid"),
                col("controller"),
                col("event_cid"),
                col("data"),
            ],
            vec![array_agg(col("previous")).alias("previous")],
        )
        .unwrap()
        .sort(vec![col("index").sort(true, true)])
        .unwrap()
        .collect()
        .await
        .unwrap();

    let formatted = pretty_format_batches(&doc_state).unwrap().to_string();

    // Use expect_test to validate the output
    expect![[r#"
        +-------+------------+-------------------------------------------------------------+---------------+-------------------------------------------------------------+--------+----------------------------------------------------------------------------------------------------------------------------+
        | index | event_type | stream_cid                                                  | controller    | event_cid                                                   | data   | previous                                                                                                                   |
        +-------+------------+-------------------------------------------------------------+---------------+-------------------------------------------------------------+--------+----------------------------------------------------------------------------------------------------------------------------+
        | 0     | 0          | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | did:key:test1 | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 010203 | []                                                                                                                         |
        | 1     | 0          | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | did:key:test1 | baeabeid2w5pgdsdh25nah7batmhxanbj3x2w2is3atser7qxboyojv236q | 040506 | [baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu]                                                              |
        | 2     | 1          | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | did:key:test1 | baeabeidtub3bnbojbickf6d4pqscaw6xpt5ksgido7kcsg2jyftaj237di |        | [baeabeid2w5pgdsdh25nah7batmhxanbj3x2w2is3atser7qxboyojv236q]                                                              |
        | 3     | 0          | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | did:key:test1 | baeabeiewqcj4bwhcssizv5kcyvsvm57bxghjpqshnbzkc6rijmwb4im4yq | 070809 | [baeabeidtub3bnbojbickf6d4pqscaw6xpt5ksgido7kcsg2jyftaj237di, baeabeid2w5pgdsdh25nah7batmhxanbj3x2w2is3atser7qxboyojv236q] |
        +-------+------------+-------------------------------------------------------------+---------------+-------------------------------------------------------------+--------+----------------------------------------------------------------------------------------------------------------------------+"#]].assert_eq(&formatted);
}
