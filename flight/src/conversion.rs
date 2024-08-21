use crate::types::*;
use anyhow::Result;
use arrow::array::{BinaryBuilder, ListBuilder, StringBuilder, UInt8Builder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// Converts a slice of ConclusionEvents into an Arrow RecordBatch.
///
/// This function takes a slice of ConclusionEvents and transforms them into an Arrow RecordBatch,
/// which is a columnar data structure that can be efficiently processed and stored.
///
/// The resulting RecordBatch has the following schema:
/// - event_type: Utf8, non-nullable
/// - stream_id: Binary, non-nullable
/// - stream_type: Utf8, non-nullable
/// - controllers: Utf8, non-nullable (comma-separated list for Data events)
/// - before: Timestamp(Nanosecond), nullable
/// - after: Timestamp(Nanosecond), nullable
/// - data: Binary, non-nullable
///
/// # Arguments
///
/// * `events` - A slice of ConclusionEvents to be converted
/// # Returns
///
/// * `Result<RecordBatch, Box<dyn std::error::Error>>` - The resulting Arrow RecordBatch or an error
///
/// # Example
///
/// ```
/// use flight::{ConclusionEvent, conclusion_events_to_record_batch};
///
/// let events: Vec<ConclusionEvent> = vec![/* ... */];
/// match conclusion_events_to_record_batch(&events) {
///     Ok(batch) => println!("Created RecordBatch with {} rows", batch.num_rows()),
///     Err(e) => eprintln!("Error creating RecordBatch: {}", e),
/// }
/// ```
pub fn conclusion_events_to_record_batch(events: &[ConclusionEvent]) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("event_type", DataType::UInt8, false),
        Field::new("stream_id", DataType::Binary, false),
        Field::new("stream_type", DataType::UInt8, false),
        Field::new("controller", DataType::Utf8, false),
        Field::new("data", DataType::Binary, false),
        // TODO: We should be able to set nullable as false in the field. But doing so breaks the ListBuilder :(
        Field::new(
            "previous",
            DataType::List(Box::new(Field::new("item", DataType::Binary, true))),
            true,
        ),
    ]));

    let mut event_type_builder = UInt8Builder::new(events.len());
    let mut stream_id_builder = BinaryBuilder::new(events.len());
    let mut stream_type_builder = UInt8Builder::new(events.len());
    let mut controller_builder = StringBuilder::new(events.len());
    let mut data_builder = BinaryBuilder::new(events.len());
    let mut previous_builder = ListBuilder::new(BinaryBuilder::new(events.len()));

    for event in events {
        match event {
            ConclusionEvent::Data(data_event) => {
                event_type_builder.append_value(0)?;
                stream_id_builder.append_value(data_event.id.to_bytes())?;
                stream_type_builder.append_value(data_event.init.stream_type.as_u8())?;
                controller_builder.append_value(&data_event.init.controller)?;
                data_builder.append_value(data_event.data.as_ref())?;
                for cid in &data_event.previous {
                    previous_builder.values().append_value(cid.to_bytes())?;
                }
                previous_builder.append(true)?;
            }
            ConclusionEvent::Time(_) => {
                todo!("implement time event once we know what its structure looks like");
                // event_type_builder.append_value(1)?;
                // stream_id_builder.append_null()?;
                // stream_type_builder.append_null()?;
                // controller_builder.append_null()?;
                // data_builder.append_null()?;
                // previous_builder.append_null()?;
            }
        }
    }

    let record_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(event_type_builder.finish()),
            Arc::new(stream_id_builder.finish()),
            Arc::new(stream_type_builder.finish()),
            Arc::new(controller_builder.finish()),
            Arc::new(data_builder.finish()),
            Arc::new(previous_builder.finish()),
        ],
    )?;

    // TODO: Remove this println
    println!("{:?}", record_batch);
    Ok(record_batch)
}
