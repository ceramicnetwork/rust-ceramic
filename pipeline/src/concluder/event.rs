use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow::datatypes::{DataType, Field, Int32Type};
use ceramic_core::METAMODEL_STREAM_ID;
use ceramic_event::{unvalidated, StreamId, StreamIdType};
use cid::Cid;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::array::{
    ArrayRef, BinaryBuilder, BinaryDictionaryBuilder, ListBuilder, MapBuilder, MapFieldNames,
    PrimitiveBuilder, StringBuilder, StructArray, UInt64Builder, UInt8Builder,
};
use int_enum::IntEnum;
use ipld_core::ipld::Ipld;
use serde::{Deserialize, Serialize};

/// A Ceramic event annotated with conclusions about the event.
///
/// Conclusions included for all events:
///     1. An event's signature has been verified
///     2. An event's previous events will have an conclusion_event_order less than the event's
///        conclusion_event_order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConclusionEvent {
    /// An event that contains data for a stream.
    Data(ConclusionData),
    /// An event that contains temporal information for a stream.
    Time(ConclusionTime),
}

impl AsRef<ConclusionEvent> for ConclusionEvent {
    fn as_ref(&self) -> &ConclusionEvent {
        self
    }
}

impl ConclusionEvent {
    /// Report the conclusion_event_order of the event.
    pub fn order(&self) -> u64 {
        match self {
            ConclusionEvent::Data(data) => data.order,
            ConclusionEvent::Time(time) => time.order,
        }
    }
    pub(crate) fn event_type_as_int(&self) -> u8 {
        match self {
            ConclusionEvent::Data(_) => 0,
            ConclusionEvent::Time(_) => 1,
        }
    }
    /// Report the CID of this event
    pub fn event_cid(&self) -> Cid {
        match self {
            ConclusionEvent::Data(event) => event.event_cid,
            ConclusionEvent::Time(event) => event.event_cid,
        }
    }
    /// Report the init data of this event
    pub fn init(&self) -> &ConclusionInit {
        match self {
            ConclusionEvent::Data(event) => &event.init,
            ConclusionEvent::Time(event) => &event.init,
        }
    }
}

/// ConclusionInit is static metadata about a stream.
/// All events within a stream have the same ConclusionInit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConclusionInit {
    /// The CID of the init event of the stream. Can be used as a unique identifier for the stream.
    /// This is not the StreamId as it does not contain the StreamType.
    pub stream_cid: Cid,
    /// The type of the stream.
    pub stream_type: u8,
    /// DID controller of the stream.
    pub controller: String,
    /// Order set of key value pairs that annotate the stream.
    pub dimensions: Vec<(String, Vec<u8>)>,
}

/// ConclusionData represents a Ceramic event that contained data.
///
/// Additionally we have concluded to which stream the event belongs and its associated metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConclusionData {
    /// Stream order value of the event. See [`ConclusionEvent`] for invariants about the stream
    /// order.
    pub order: u64,
    /// The CID of the event itself. Can be used as a unique identifier for the event.
    pub event_cid: Cid,
    /// The stream metadata of the event.
    pub init: ConclusionInit,
    /// Ordered list of previous events this event references.
    pub previous: Vec<Cid>,
    /// Raw bytes of the event payload.
    pub data: Vec<u8>,
}

/// ConclusionTime represents a Ceramic event that contains time relevant information.
///
/// Additionally we have concluded to which stream the event belongs and its associated metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConclusionTime {
    /// Index of the event. See [`ConclusionEvent`] for invariants about the stream order.
    pub order: u64,
    /// The CID of the event itself. Can be used as a unique identifier for the event.
    pub event_cid: Cid,
    /// The stream metadata of the event.
    pub init: ConclusionInit,
    /// Ordered list of previous events this event references.
    pub previous: Vec<Cid>,
    /// Proof of time event anchoring
    pub time_proof: TimeProof,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Time event proof has been validated and resulted in a proof of time event anchoring
pub struct TimeProof {
    /// It is known that the time event existed before this time as a Unix timestamp in seconds.
    pub before: u64,
    /// Chain ID where this time event was anchored.
    pub chain_id: String,
}

impl<'a> TryFrom<&'a unvalidated::Event<Ipld>> for ConclusionInit {
    type Error = anyhow::Error;

    fn try_from(event: &'a unvalidated::Event<Ipld>) -> Result<Self> {
        // Extract the init payload from the event
        let init_payload = event
            .init_payload()
            .ok_or_else(|| anyhow!("malformed event: no init payload found"))?;

        // Get the model from the init header
        // The model indicates the creator of the stream
        let model = init_payload.header().model();

        // Convert the model to a StreamId
        let stream_id = StreamId::try_from(model)?;

        // Determine the stream type:
        // If the stream_id matches the metamodel, it's a Model stream
        // Otherwise, it's a ModelInstanceDocument stream
        let stream_type = if stream_id == METAMODEL_STREAM_ID {
            StreamIdType::Model
        } else {
            StreamIdType::ModelInstanceDocument
        };

        // Construct and return the ConclusionInit
        Ok(ConclusionInit {
            stream_cid: *event.stream_cid(),
            stream_type: stream_type.int_value() as u8,
            controller: init_payload
                .header()
                .controllers()
                .first()
                .ok_or_else(|| anyhow!("no controller found"))?
                .to_string(),
            dimensions: vec![
                ("model".to_string(), init_payload.header().model().to_vec()),
                (
                    "controller".to_string(),
                    init_payload
                        .header()
                        .controllers()
                        .first()
                        .cloned()
                        .unwrap_or_default()
                        .into_bytes(),
                ),
                (
                    "context".to_string(),
                    init_payload
                        .header()
                        .context()
                        .map(|context| context.to_vec())
                        .unwrap_or_default(),
                ),
                (
                    "unique".to_string(),
                    init_payload
                        .header()
                        .unique()
                        .map(|unique| unique.to_vec())
                        .unwrap_or_default(),
                ),
            ],
        })
    }
}

/// Construct a [`RecordBatch`] from an iterator of [`ConclusionEvent`]s.
pub struct ConclusionEventBuilder {
    order: UInt64Builder,
    event_type: UInt8Builder,
    stream_cid: BinaryBuilder,
    stream_type: UInt8Builder,
    controller: StringBuilder,
    // TODO: Specify there is a limit on dimensions in the spec.
    //
    // We need a large enough key type to handle all unique dimension values which will be a
    // product of the numner of dimensions on the stream and the total number of streams.
    // Given a small limit (i.e. <100) on the total number of dimensions per stream starting with i32 (for about 4B unique values) should be
    // sufficient.
    dimensions: MapBuilder<StringBuilder, BinaryDictionaryBuilder<Int32Type>>,
    event_cid: BinaryBuilder,
    data: BinaryBuilder,
    previous: ListBuilder<BinaryBuilder>,
    before: UInt64Builder,
    chain_id: StringBuilder,
}

impl Default for ConclusionEventBuilder {
    fn default() -> Self {
        Self {
            order: UInt64Builder::new(),
            event_type: PrimitiveBuilder::new(),
            stream_cid: BinaryBuilder::new(),
            stream_type: PrimitiveBuilder::new(),
            controller: StringBuilder::new(),
            dimensions: MapBuilder::new(
                Some(MapFieldNames {
                    entry: "entries".to_string(),
                    key: "key".to_string(),
                    value: "value".to_string(),
                }),
                StringBuilder::new(),
                // Use dictionary builder as we expect low cardinality values across multiple
                // conclusion events.
                BinaryDictionaryBuilder::new(),
            ),
            event_cid: BinaryBuilder::new(),
            data: BinaryBuilder::new(),
            previous: ListBuilder::new(BinaryBuilder::new())
                .with_field(Field::new_list_field(DataType::Binary, false)),
            before: UInt64Builder::new(),
            chain_id: StringBuilder::new(),
        }
    }
}

impl ConclusionEventBuilder {
    fn append(&mut self, event: &ConclusionEvent) {
        let init = match event {
            ConclusionEvent::Data(data_event) => {
                self.event_cid.append_value(data_event.event_cid.to_bytes());
                self.data.append_value(&data_event.data);
                for cid in &data_event.previous {
                    self.previous.values().append_value(cid.to_bytes());
                }
                self.previous.append(!data_event.previous.is_empty());
                self.order.append_value(data_event.order);
                self.before.append_null();
                self.chain_id.append_null();
                &data_event.init
            }
            ConclusionEvent::Time(time_event) => {
                self.event_cid.append_value(time_event.event_cid.to_bytes());
                self.data.append_null();
                for cid in &time_event.previous {
                    self.previous.values().append_value(cid.to_bytes());
                }
                self.previous.append(!time_event.previous.is_empty());
                self.order.append_value(time_event.order);

                // Add time proof data
                self.before.append_value(time_event.time_proof.before);
                self.chain_id
                    .append_value(time_event.time_proof.chain_id.clone());

                &time_event.init
            }
        };
        self.event_type.append_value(event.event_type_as_int());
        self.stream_cid.append_value(init.stream_cid.to_bytes());
        self.controller.append_value(&init.controller);
        self.stream_type.append_value(init.stream_type);
        for (k, v) in &init.dimensions {
            self.dimensions.keys().append_value(k);
            self.dimensions.values().append_value(v);
        }
        let _ = self.dimensions.append(!init.dimensions.is_empty());
    }

    fn finish(&mut self) -> StructArray {
        StructArray::try_from(vec![
            (
                "conclusion_event_order",
                Arc::new(self.order.finish()) as ArrayRef,
            ),
            ("stream_cid", Arc::new(self.stream_cid.finish()) as ArrayRef),
            (
                "stream_type",
                Arc::new(self.stream_type.finish()) as ArrayRef,
            ),
            ("controller", Arc::new(self.controller.finish()) as ArrayRef),
            ("dimensions", Arc::new(self.dimensions.finish()) as ArrayRef),
            ("event_cid", Arc::new(self.event_cid.finish()) as ArrayRef),
            ("event_type", Arc::new(self.event_type.finish()) as ArrayRef),
            ("data", Arc::new(self.data.finish()) as ArrayRef),
            ("previous", Arc::new(self.previous.finish()) as ArrayRef),
            ("before", Arc::new(self.before.finish()) as ArrayRef),
            ("chain_id", Arc::new(self.chain_id.finish()) as ArrayRef),
        ])
        .expect("unreachable, we should always construct a well formed struct array")
    }
}

impl<'a> Extend<&'a ConclusionEvent> for ConclusionEventBuilder {
    fn extend<T>(&mut self, events: T)
    where
        T: IntoIterator<Item = &'a ConclusionEvent>,
    {
        events.into_iter().for_each(|event| {
            self.append(event);
        });
    }
}

/// Converts a collection of ConclusionEvents into an Arrow RecordBatch.
///
/// This function takes an iterable of ConclusionEvents and converts them into a single
/// Arrow RecordBatch, which can be used for efficient storage or processing of event data.
///
/// # Arguments
///
/// * `events` - An iterable collection of items that can be converted to ConclusionEvent references.
///
/// # Returns
///
/// * `Result<RecordBatch>` - An Arrow RecordBatch containing the event data, or an error if the conversion fails.
///
/// # Example
///
/// ```
/// use anyhow::Result;
/// use ceramic_pipeline::{
///     conclusion_events_to_record_batch,
///     ConclusionEvent, ConclusionData, ConclusionInit
/// };
/// use cid::Cid;
///
/// fn main() -> Result<()> {
///     let events = vec![
///         ConclusionEvent::Data(ConclusionData {
///             order: 0,
///             event_cid: Cid::default(),
///             init: ConclusionInit {
///                 stream_cid: Cid::default(),
///                 stream_type: 0,
///                 controller: "did:key:test1".to_string(),
///                 dimensions: vec![],
///             },
///             previous: vec![],
///             data: vec![1, 2, 3],
///         }),
///         ConclusionEvent::Data(ConclusionData {
///             order: 1,
///             event_cid: Cid::default(),
///             init: ConclusionInit {
///                 stream_cid: Cid::default(),
///                stream_type: 2,
///                 controller: "did:key:test2".to_string(),
///                 dimensions: vec![],
///             },
///             previous: vec![Cid::default()],
///             data: vec![4, 5, 6],
///         }),
///     ];
///
///     let record_batch = conclusion_events_to_record_batch(&events)?;
///     assert_eq!(record_batch.num_rows(), 2);
///     Ok(())
/// }
/// ```
pub fn conclusion_events_to_record_batch<'a, I>(events: I) -> Result<RecordBatch>
where
    I: IntoIterator<Item = &'a ConclusionEvent>,
{
    let mut builder = ConclusionEventBuilder::default();
    builder.extend(events);
    let struct_array = builder.finish();
    let record_batch = RecordBatch::from(&struct_array);
    Ok(record_batch)
}

#[cfg(test)]
mod tests {
    use crate::cid_string::{CidString, CidStringList};

    use super::*;

    use std::{str::FromStr, sync::Arc};

    use arrow::util::pretty::pretty_format_batches;
    use ceramic_event::StreamIdType;
    use cid::Cid;
    use datafusion::{
        arrow::{datatypes::DataType, record_batch::RecordBatch},
        dataframe::DataFrame,
        execution::context::SessionContext,
        logical_expr::{col, expr::ScalarFunction, Cast, Expr, ScalarUDF},
    };
    use expect_test::expect;
    use test_log::test;

    // Tests the conversion of ConclusionEvents to Arrow RecordBatch.
    //
    // This test creates mock ConclusionEvents (both Data and Time events),
    // converts them to a RecordBatch using the conclusion_events_to_record_batch function,
    // and then verifies that the resulting RecordBatch contains the expected data.
    //
    // The test checks:
    // 1. The number of rows in the RecordBatch
    // 2. The schema of the RecordBatch
    // 3. The content of each column in the RecordBatch
    #[test(tokio::test)]
    async fn test_conclusion_events_to_record_batch() {
        // Create mock ConclusionEvents
        let events = vec![
            ConclusionEvent::Data(ConclusionData {
                event_cid: Cid::from_str(
                    "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                )
                .unwrap(),
                init: ConclusionInit {
                    stream_cid: Cid::from_str(
                        "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                    )
                    .unwrap(),
                    stream_type: StreamIdType::Model as u8,
                    controller: "did:key:test1".to_string(),
                    dimensions: vec![
                        ("controller".to_string(), b"did:key:test1".to_vec()),
                        ("model".to_string(), b"model".to_vec()),
                    ],
                },
                previous: vec![],
                data: b"123".into(),
                order: 0,
            }),
            ConclusionEvent::Data(ConclusionData {
                event_cid: Cid::from_str(
                    "baeabeid2w5pgdsdh25nah7batmhxanbj3x2w2is3atser7qxboyojv236q",
                )
                .unwrap(),
                init: ConclusionInit {
                    stream_cid: Cid::from_str(
                        "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                    )
                    .unwrap(),
                    stream_type: StreamIdType::Model as u8,
                    controller: "did:key:test1".to_string(),
                    dimensions: vec![
                        ("controller".to_string(), b"did:key:test1".to_vec()),
                        ("model".to_string(), b"model".to_vec()),
                    ],
                },
                previous: vec![Cid::from_str(
                    "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                )
                .unwrap()],
                data: b"456".into(),
                order: 1,
            }),
            ConclusionEvent::Time(ConclusionTime {
                event_cid: Cid::from_str(
                    "baeabeidtub3bnbojbickf6d4pqscaw6xpt5ksgido7kcsg2jyftaj237di",
                )
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
                    stream_type: StreamIdType::Model as u8,
                    controller: "did:key:test1".to_string(),
                    dimensions: vec![
                        ("controller".to_string(), b"did:key:test1".to_vec()),
                        ("model".to_string(), b"model".to_vec()),
                    ],
                },
                order: 2,
                time_proof: TimeProof {
                    before: 1744383131980,
                    chain_id: "test:chain".to_owned(),
                },
            }),
            ConclusionEvent::Data(ConclusionData {
                event_cid: Cid::from_str(
                    "baeabeiewqcj4bwhcssizv5kcyvsvm57bxghjpqshnbzkc6rijmwb4im4yq",
                )
                .unwrap(),
                init: ConclusionInit {
                    stream_cid: Cid::from_str(
                        "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                    )
                    .unwrap(),
                    stream_type: StreamIdType::Model as u8,
                    controller: "did:key:test1".to_string(),
                    dimensions: vec![
                        ("controller".to_string(), b"did:key:test1".to_vec()),
                        ("model".to_string(), b"model".to_vec()),
                    ],
                },
                previous: vec![
                    Cid::from_str("baeabeidtub3bnbojbickf6d4pqscaw6xpt5ksgido7kcsg2jyftaj237di")
                        .unwrap(),
                    Cid::from_str("baeabeid2w5pgdsdh25nah7batmhxanbj3x2w2is3atser7qxboyojv236q")
                        .unwrap(),
                ],
                data: b"789".into(),
                order: 3,
            }),
        ];
        // Convert events to RecordBatch
        let record_batch = conclusion_events_to_record_batch(&events).unwrap();
        let record_batch = pretty_conclusion_events_from_batch(record_batch).await;
        let formatted = pretty_format_batches(&record_batch).unwrap().to_string();

        // Use expect_test to validate the output
        expect![[r#"
            +------------------------+-------------------------------------------------------------+-------------+---------------+-------------------------------------------------------------+-------------------------------------------------------------+------------+------+----------------------------------------------------------------------------------------------------------------------------+---------------+------------+
            | conclusion_event_order | stream_cid                                                  | stream_type | controller    | dimensions                                                  | event_cid                                                   | event_type | data | previous                                                                                                                   | before        | chain_id   |
            +------------------------+-------------------------------------------------------------+-------------+---------------+-------------------------------------------------------------+-------------------------------------------------------------+------------+------+----------------------------------------------------------------------------------------------------------------------------+---------------+------------+
            | 0                      | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:test1 | {controller: 6469643a6b65793a7465737431, model: 6d6f64656c} | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 0          | 123  |                                                                                                                            |               |            |
            | 1                      | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:test1 | {controller: 6469643a6b65793a7465737431, model: 6d6f64656c} | baeabeid2w5pgdsdh25nah7batmhxanbj3x2w2is3atser7qxboyojv236q | 0          | 456  | [baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu]                                                              |               |            |
            | 2                      | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:test1 | {controller: 6469643a6b65793a7465737431, model: 6d6f64656c} | baeabeidtub3bnbojbickf6d4pqscaw6xpt5ksgido7kcsg2jyftaj237di | 1          |      | [baeabeid2w5pgdsdh25nah7batmhxanbj3x2w2is3atser7qxboyojv236q]                                                              | 1744383131980 | test:chain |
            | 3                      | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:test1 | {controller: 6469643a6b65793a7465737431, model: 6d6f64656c} | baeabeiewqcj4bwhcssizv5kcyvsvm57bxghjpqshnbzkc6rijmwb4im4yq | 0          | 789  | [baeabeidtub3bnbojbickf6d4pqscaw6xpt5ksgido7kcsg2jyftaj237di, baeabeid2w5pgdsdh25nah7batmhxanbj3x2w2is3atser7qxboyojv236q] |               |            |
            +------------------------+-------------------------------------------------------------+-------------+---------------+-------------------------------------------------------------+-------------------------------------------------------------+------------+------+----------------------------------------------------------------------------------------------------------------------------+---------------+------------+"#]].assert_eq(&formatted);
    }

    // Applies various transformations on a record batch of conclusion_events data to make it easier to
    // read.
    // Useful in conjunction with expect_test.
    async fn pretty_conclusion_events_from_batch(batch: RecordBatch) -> Vec<RecordBatch> {
        let ctx = SessionContext::new();
        ctx.register_batch("conclusion_events", batch).unwrap();

        pretty_conclusion_events(ctx.table("conclusion_events").await.unwrap()).await
    }

    // Applies various transformations on a dataframe of conclusion_events data to make it easier to
    // read.
    // Useful in conjunction with expect_test.
    async fn pretty_conclusion_events(conclusion_events: DataFrame) -> Vec<RecordBatch> {
        let cid_string = Arc::new(ScalarUDF::from(CidString::new()));
        let cid_string_list = Arc::new(ScalarUDF::from(CidStringList::new()));
        conclusion_events
            .select(vec![
                col("conclusion_event_order"),
                Expr::ScalarFunction(ScalarFunction::new_udf(
                    cid_string.clone(),
                    vec![col("stream_cid")],
                ))
                .alias("stream_cid"),
                col("stream_type"),
                col("controller"),
                col("dimensions"),
                Expr::ScalarFunction(ScalarFunction::new_udf(
                    cid_string.clone(),
                    vec![col("event_cid")],
                ))
                .alias("event_cid"),
                col("event_type"),
                Expr::Cast(Cast::new(Box::new(col("data")), DataType::Utf8)).alias("data"),
                Expr::ScalarFunction(ScalarFunction::new_udf(
                    cid_string_list,
                    vec![col("previous")],
                ))
                .alias("previous"),
                col("before"),
                col("chain_id"),
            ])
            .unwrap()
            .sort(vec![col("conclusion_event_order").sort(true, true)])
            .unwrap()
            .collect()
            .await
            .unwrap()
    }
}
