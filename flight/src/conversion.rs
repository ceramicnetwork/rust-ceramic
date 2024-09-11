use crate::types::*;
use anyhow::Result;
use arrow::array::{
    ArrayRef, BinaryBuilder, ListBuilder, PrimitiveBuilder, StringBuilder, StructArray,
    UInt64Builder, UInt8Builder,
};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// Construct a [`RecordBatch`] from an iterator of [`ConclusionEvent`]s.
#[derive(Debug)]
pub struct ConclusionEventBuilder {
    index: UInt64Builder,
    event_type: UInt8Builder,
    stream_cid: BinaryBuilder,
    stream_type: UInt8Builder,
    event_cid: BinaryBuilder,
    controller: StringBuilder,
    event_cid: BinaryBuilder,
    data: BinaryBuilder,
    previous: ListBuilder<BinaryBuilder>,
}

impl Default for ConclusionEventBuilder {
    fn default() -> Self {
        Self {
            index: UInt64Builder::new(),
            event_type: PrimitiveBuilder::new(),
            stream_cid: BinaryBuilder::new(),
            stream_type: PrimitiveBuilder::new(),
            event_cid: BinaryBuilder::new(),
            controller: StringBuilder::new(),
            event_cid: BinaryBuilder::new(),
            data: BinaryBuilder::new(),
            previous: ListBuilder::new(BinaryBuilder::new())
                .with_field(Field::new_list_field(DataType::Binary, false)),
        }
    }
}

impl ConclusionEventBuilder {
    fn append(&mut self, event: &ConclusionEvent) {
        self.event_type.append_value(event.event_type_as_int());
        match event {
            ConclusionEvent::Data(data_event) => {
                self.stream_cid
                    .append_value(data_event.init.stream_cid.to_bytes());
                self.stream_type.append_value(data_event.init.stream_type);
                self.event_cid.append_value(data_event.event_cid.to_bytes());
                self.controller.append_value(&data_event.init.controller);
                self.data.append_value(&data_event.data);
                for cid in &data_event.previous {
                    self.previous.values().append_value(cid.to_bytes());
                }
                self.previous.append(!data_event.previous.is_empty());
                self.index.append_value(data_event.index);
            }
            ConclusionEvent::Time(time_event) => {
                self.stream_cid
                    .append_value(time_event.init.stream_cid.to_bytes());
                self.stream_type.append_value(time_event.init.stream_type);
                self.controller.append_value(&time_event.init.controller);
                self.event_cid.append_value(time_event.event_cid.to_bytes());
                self.data.append_null();
                for cid in &time_event.previous {
                    self.previous.values().append_value(cid.to_bytes());
                }
                self.previous.append(!time_event.previous.is_empty());
                self.index.append_value(time_event.index);
            }
        }
    }

    fn finish(&mut self) -> StructArray {
        let event_type = Arc::new(self.event_type.finish()) as ArrayRef;
        let event_type_field = Arc::new(Field::new("event_type", DataType::UInt8, false));

        let stream_cid = Arc::new(self.stream_cid.finish()) as ArrayRef;
        let stream_cid_field = Arc::new(Field::new("stream_cid", DataType::Binary, false));

        let stream_type = Arc::new(self.stream_type.finish()) as ArrayRef;
        let stream_type_field = Arc::new(Field::new("stream_type", DataType::UInt8, false));

        let controller = Arc::new(self.controller.finish()) as ArrayRef;
        let controller_field = Arc::new(Field::new("controller", DataType::Utf8, false));

        let event_cid = Arc::new(self.event_cid.finish()) as ArrayRef;
        let event_cid_field = Arc::new(Field::new("event_cid", DataType::Binary, false));

        // Data can be empty for ConclusionTime
        let data = Arc::new(self.data.finish()) as ArrayRef;
        let data_field = Arc::new(Field::new("data", DataType::Binary, true));

        let previous = Arc::new(self.previous.finish()) as ArrayRef;
        let previous_field = Arc::new(Field::new(
            "previous",
            DataType::List(Arc::new(Field::new_list_field(DataType::Binary, false))),
            true,
        ));

        let index = Arc::new(self.index.finish()) as ArrayRef;
        let index_field = Arc::new(Field::new("index", DataType::UInt64, false));

        StructArray::from(vec![
            (index_field, index),
            (event_type_field, event_type),
            (stream_cid_field, stream_cid),
            (stream_type_field, stream_type),
            (controller_field, controller),
            (event_cid_field, event_cid),
            (data_field, data),
            (previous_field, previous),
        ])
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
/// use ceramic_flight::{ConclusionEvent, ConclusionData, ConclusionInit};
/// use ceramic_flight::conclusion_events_to_record_batch;
/// use cid::Cid;
///
/// fn main() -> Result<()> {
///     let events = vec![
///         ConclusionEvent::Data(ConclusionData {
///             index: 0,
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
///             index: 1,
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
    use super::*;

    use std::str::FromStr;

    use arrow::util::pretty::pretty_format_batches;
    use ceramic_arrow_test::pretty_feed_from_batch;
    use cid::Cid;
    use expect_test::expect;

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
    #[tokio::test]
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
                    controller: "did:key:test1".to_string(),
                    dimensions: vec![],
                },
                previous: vec![],
                data: b"123".into(),
                index: 0,
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
                    controller: "did:key:test1".to_string(),
                    dimensions: vec![],
                },
                previous: vec![Cid::from_str(
                    "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                )
                .unwrap()],
                data: b"456".into(),
                index: 1,
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
                    controller: "did:key:test1".to_string(),
                    dimensions: vec![],
                },
                index: 2,
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
                    controller: "did:key:test1".to_string(),
                    dimensions: vec![],
                },
                previous: vec![
                    Cid::from_str("baeabeidtub3bnbojbickf6d4pqscaw6xpt5ksgido7kcsg2jyftaj237di")
                        .unwrap(),
                    Cid::from_str("baeabeid2w5pgdsdh25nah7batmhxanbj3x2w2is3atser7qxboyojv236q")
                        .unwrap(),
                ],
                data: b"789".into(),
                index: 3,
            }),
        ];
        // Convert events to RecordBatch
        let record_batch = conclusion_events_to_record_batch(&events).unwrap();
        let record_batch = pretty_feed_from_batch(record_batch).await;
        let formatted = pretty_format_batches(&record_batch).unwrap().to_string();

        // Use expect_test to validate the output
        expect![[r#"
        +-------+------------+-------------------------------------------------------------+---------------+-------------------------------------------------------------+------+----------------------------------------------------------------------------------------------------------------------------+
        | index | event_type | stream_cid                                                  | controller    | event_cid                                                   | data | previous                                                                                                                   |
        +-------+------------+-------------------------------------------------------------+---------------+-------------------------------------------------------------+------+----------------------------------------------------------------------------------------------------------------------------+
        | 0     | 0          | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | did:key:test1 | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 123  | []                                                                                                                         |
        | 1     | 0          | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | did:key:test1 | baeabeid2w5pgdsdh25nah7batmhxanbj3x2w2is3atser7qxboyojv236q | 456  | [baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu]                                                              |
        | 2     | 1          | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | did:key:test1 | baeabeidtub3bnbojbickf6d4pqscaw6xpt5ksgido7kcsg2jyftaj237di |      | [baeabeid2w5pgdsdh25nah7batmhxanbj3x2w2is3atser7qxboyojv236q]                                                              |
        | 3     | 0          | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | did:key:test1 | baeabeiewqcj4bwhcssizv5kcyvsvm57bxghjpqshnbzkc6rijmwb4im4yq | 789  | [baeabeidtub3bnbojbickf6d4pqscaw6xpt5ksgido7kcsg2jyftaj237di, baeabeid2w5pgdsdh25nah7batmhxanbj3x2w2is3atser7qxboyojv236q] |
        +-------+------------+-------------------------------------------------------------+---------------+-------------------------------------------------------------+------+----------------------------------------------------------------------------------------------------------------------------+"#]].assert_eq(&formatted);
    }
}
