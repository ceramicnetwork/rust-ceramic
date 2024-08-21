use crate::types::*;
use arrow::array::{BinaryArray, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
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
/// use your_crate::{ConclusionEvent, conclusion_events_to_record_batch};
///
/// let events: Vec<ConclusionEvent> = vec![/* ... */];
/// match conclusion_events_to_record_batch(&events) {
///     Ok(batch) => println!("Created RecordBatch with {} rows", batch.num_rows()),
///     Err(e) => eprintln!("Error creating RecordBatch: {}", e),
/// }
/// ```
pub fn conclusion_events_to_record_batch(
    events: &[ConclusionEvent],
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    println!("Converting {} events to a record batch", events.len());
    let schema = Arc::new(Schema::new(vec![
        Field::new("event_type", DataType::Utf8, false),
        Field::new("stream_id", DataType::Binary, false),
        Field::new("stream_type", DataType::Utf8, false),
        Field::new("controllers", DataType::Utf8, false),
        Field::new(
            "before",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "after",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("data", DataType::Binary, false),
    ]));

    println!("---------------------------------");
    println!("Schema: {:?}", schema);
    println!("---------------------------------");
    let mut event_types = Vec::new();
    let mut stream_ids = Vec::new();
    let mut stream_types = Vec::new();
    let mut controllers = Vec::new();
    let mut befores = Vec::new();
    let mut afters = Vec::new();
    let mut data = Vec::new();

    for event in events {
        match event {
            ConclusionEvent::Data(data_event) => {
                event_types.push("Data");
                stream_ids.push(data_event.id.to_bytes());
                stream_types.push(data_event.init.stream_type.clone());
                controllers.push(data_event.init.controllers.clone());
                befores.push(data_event.before);
                afters.push(data_event.after);
                data.push(data_event.data.as_ref().to_vec());
            }
            ConclusionEvent::Time(_) => {
                todo!("implement time event once we know what it's structure looks like");
                // event_types.push("Time");
                // stream_ids.push(Vec::new());
                // stream_types.push(String::new());
                // controllers.push(String::new());
                // befores.push(None);
                // afters.push(None);
                // data.push(Vec::new());
            }
        }
    }

    let record_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(event_types)),
            Arc::new(BinaryArray::from_iter(
                stream_ids.iter().map(|v| Some(v.as_slice())),
            )),
            Arc::new(StringArray::from(stream_types)),
            Arc::new(StringArray::from(controllers)),
            Arc::new(TimestampMillisecondArray::from(
                befores
                    .iter()
                    .map(|v| v.map(|t| t.timestamp_millis()))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(TimestampMillisecondArray::from(
                afters
                    .iter()
                    .map(|v| v.map(|t| t.timestamp_millis()))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(BinaryArray::from_iter(
                data.iter().map(|v| Some(v.as_slice())),
            )),
        ],
    )?;

    Ok(record_batch)
}
