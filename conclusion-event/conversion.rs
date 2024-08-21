use arrow::record_batch::RecordBatch;
use arrow::array::{StringArray, BinaryArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;
use crate::types::*;


pub fn conclusion_events_to_record_batch(events: &[ConclusionEvent]) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let schema = Schema::new(vec![
        Field::new("event_type", DataType::Utf8, false),
        Field::new("stream_id", DataType::Binary, false),
        Field::new("stream_type", DataType::Utf8, false),
        Field::new("controllers", DataType::Utf8, false),
        Field::new("before", DataType::UInt64, true),
        Field::new("after", DataType::UInt64, true),
        // Add more fields as needed
    ]);

    let mut event_types = Vec::new();
    let mut stream_ids = Vec::new();
    let mut stream_types = Vec::new();
    let mut controllers = Vec::new();
    let mut befores = Vec::new();
    let mut afters = Vec::new();

    for event in events {
        match event {
            ConclusionEvent::Data(data) => {
                event_types.push("Data");
                stream_ids.push(data.id.to_bytes());
                stream_types.push(data.init.stream_type.clone());
                controllers.push(data.init.controllers.clone());
                befores.push(data.before);
                afters.push(data.after);
            }
            ConclusionEvent::Time(_) => {
                event_types.push("Time");
                stream_ids.push(Vec::new()); // Placeholder
                stream_types.push(String::new()); // Placeholder
                controllers.push(String::new()); // Placeholder
                befores.push(None);
                afters.push(None);
            }
        }
    }

    let record_batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(StringArray::from(event_types)),
            Arc::new(BinaryArray::from(stream_ids)),
            Arc::new(StringArray::from(stream_types)),
            Arc::new(StringArray::from(controllers)),
            Arc::new(UInt64Array::from(befores.iter().map(|&x| x.map(|v| v as u64)).collect::<Vec<_>>())),
            Arc::new(UInt64Array::from(afters.iter().map(|&x| x.map(|v| v as u64)).collect::<Vec<_>>())),
        ],
    )?;

    Ok(record_batch)
}