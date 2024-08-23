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
    stream_id: BinaryBuilder,
    stream_type: UInt8Builder,
    controller: StringBuilder,
    data: BinaryBuilder,
    previous: ListBuilder<BinaryBuilder>,
}

impl Default for ConclusionEventBuilder {
    fn default() -> Self {
        Self {
            event_type: PrimitiveBuilder::new(),
            stream_id: BinaryBuilder::new(),
            stream_type: PrimitiveBuilder::new(),
            controller: StringBuilder::new(),
            data: BinaryBuilder::new(),
            previous: ListBuilder::new(BinaryBuilder::new())
                .with_field(Field::new_list_field(DataType::Binary, false)),
        }
    }
}

impl ConclusionEventBuilder {
    fn append(&mut self, event: &ConclusionEvent) {
        match event {
            ConclusionEvent::Data(data_event) => {
                self.event_type.append_value(0);
                self.stream_id.append_value(data_event.id.to_bytes());
                self.stream_type.append_value(data_event.init.stream_type);
                self.controller.append_value(&data_event.init.controller);
                self.data.append_value(&data_event.data);
                for cid in &data_event.previous {
                    self.previous.values().append_value(cid.to_bytes());
                }
                self.previous.append(true);
            }
            ConclusionEvent::Time(_) => {
                todo!("implement time event once we know what its structure looks like");
            }
        }
    }

    fn finish(&mut self) -> StructArray {
        let event_type = Arc::new(self.event_type.finish()) as ArrayRef;
        let event_type_field = Arc::new(Field::new("event_type", DataType::UInt8, false));

        let stream_id = Arc::new(self.stream_id.finish()) as ArrayRef;
        let stream_id_field = Arc::new(Field::new("stream_id", DataType::Binary, false));

        let stream_type = Arc::new(self.stream_type.finish()) as ArrayRef;
        let stream_type_field = Arc::new(Field::new("stream_type", DataType::UInt8, false));

        let controller = Arc::new(self.controller.finish()) as ArrayRef;
        let controller_field = Arc::new(Field::new("controller", DataType::Utf8, false));

        let data = Arc::new(self.data.finish()) as ArrayRef;
        let data_field = Arc::new(Field::new("data", DataType::Binary, false));

        let previous = Arc::new(self.previous.finish()) as ArrayRef;
        let previous_field = Arc::new(Field::new(
            "previous",
            DataType::List(Arc::new(Field::new_list_field(DataType::Binary, false))),
            true,
        ));

        StructArray::from(vec![
            (event_type_field, event_type),
            (stream_id_field, stream_id),
            (stream_type_field, stream_type),
            (controller_field, controller),
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
///             id: Cid::default(),
///             init: ConclusionInit {
///                 stream_type: 0,
///                 controller: "did:key:test1".to_string(),
///                 dimensions: vec![],
///             },
///             previous: vec![],
///             data: vec![1, 2, 3],
///         }),
///         ConclusionEvent::Data(ConclusionData {
///         id: Cid::default(),
///         init: ConclusionInit {
///             stream_type: 2,
///             controller: "did:key:test2".to_string(),
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
