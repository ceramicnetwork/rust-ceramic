use super::*;
use crate::types::{ConclusionData, ConclusionEvent, ConclusionInit};
use arrow::array::{Array, BinaryArray, StringArray, TimestampMillisecondArray};
use chrono::{DateTime, Utc};
use cid::Cid;
use serde_bytes::ByteBuf;
use std::time::{SystemTime, UNIX_EPOCH};

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
#[test]
fn test_conclusion_events_to_record_batch() {
    // Create mock ConclusionEvents
    let events = vec![
        ConclusionEvent::Data(ConclusionData {
            id: Cid::default(),
            init: ConclusionInit {
                stream_type: "test_stream".to_string(),
                controllers: "did:key:test1".to_string(),
                dimensions: vec![],
            },
            previous: vec![],
            before: Some(
                DateTime::<Utc>::from_timestamp_millis(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64,
                )
                .unwrap(),
            ),
            after: Some(
                DateTime::<Utc>::from_timestamp_millis(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64,
                )
                .unwrap(),
            ),
            data: ByteBuf::from(vec![1, 2, 3]),
        }),
        ConclusionEvent::Data(ConclusionData {
            id: Cid::default(),
            init: ConclusionInit {
                stream_type: "another_stream".to_string(),
                controllers: "did:key:test2".to_string(),
                dimensions: vec![],
            },
            previous: vec![],
            before: None,
            after: Some(
                DateTime::<Utc>::from_timestamp_millis(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64,
                )
                .unwrap(),
            ),
            data: ByteBuf::from(vec![4, 5, 6]),
        }),
    ];

    // Convert events to RecordBatch
    let record_batch = conclusion_events_to_record_batch(&events).unwrap();

    // Assert the number of rows
    assert_eq!(record_batch.num_rows(), 2);

    // Assert the schema
    let schema = record_batch.schema();
    assert_eq!(schema.fields().len(), 7);
    assert_eq!(schema.field(0).name(), "event_type");
    assert_eq!(schema.field(1).name(), "stream_id");
    assert_eq!(schema.field(2).name(), "stream_type");
    assert_eq!(schema.field(3).name(), "controllers");
    assert_eq!(schema.field(4).name(), "before");
    assert_eq!(schema.field(5).name(), "after");
    assert_eq!(schema.field(6).name(), "data");

    // Assert the data in each column
    let event_types = record_batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(event_types.value(0), "Data");
    assert_eq!(event_types.value(1), "Data");

    let stream_types = record_batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(stream_types.value(0), "test_stream");
    assert_eq!(stream_types.value(1), "another_stream");

    let controllers = record_batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(controllers.value(0), "did:key:test1");
    assert_eq!(controllers.value(1), "did:key:test2");

    let befores = record_batch
        .column(4)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    assert!(befores.is_valid(0));
    assert!(befores.is_null(1));

    let afters = record_batch
        .column(5)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    assert!(afters.is_valid(0));
    assert!(afters.is_valid(1));

    let data = record_batch
        .column(6)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(data.value(0), &[1, 2, 3]);
    assert_eq!(data.value(1), &[4, 5, 6]);
}
