use crate::types::*;
use anyhow::Result;
use arrow::array::{
    ArrayRef, BinaryBuilder, ListBuilder, PrimitiveBuilder, StringBuilder, StructArray,
    UInt8Builder,
};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

#[derive(Debug)]
pub struct ConclusionEventBuilder {
    event_type: UInt8Builder,
    stream_cid: BinaryBuilder,
    stream_type: UInt8Builder,
    event_cid: BinaryBuilder,
    controller: StringBuilder,
    data: BinaryBuilder,
    previous: ListBuilder<BinaryBuilder>,
}

impl Default for ConclusionEventBuilder {
    fn default() -> Self {
        Self {
            event_type: PrimitiveBuilder::new(),
            stream_cid: BinaryBuilder::new(),
            stream_type: PrimitiveBuilder::new(),
            event_cid: BinaryBuilder::new(),
            controller: StringBuilder::new(),
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
                self.previous.append(true);
            }
            ConclusionEvent::Time(time_event) => {
                self.stream_cid
                    .append_value(time_event.init.stream_cid.to_bytes());
                self.stream_type.append_value(time_event.init.stream_type);
                self.controller.append_value(&time_event.init.controller);
                self.previous.append_null();
                for cid in &time_event.previous {
                    self.previous.values().append_value(cid.to_bytes());
                }
                self.previous.append(false);
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

        StructArray::from(vec![
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
///             event_cid: Cid::default(),
///             init: ConclusionInit {
///                 stream_cid: Cid::default(),
///                 stream_type: 2,
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
