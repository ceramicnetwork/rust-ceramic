use super::*;
use crate::types::{ConclusionData, ConclusionEvent, ConclusionInit};
use arrow::array::{Array, BinaryArray, ListArray, StringArray, UInt8Array};
use ceramic_core::StreamIdType;
use cid::Cid;

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
            event_cid: Cid::default(),
            init: ConclusionInit {
                stream_cid: Cid::default(),
                stream_type: StreamIdType::Model as u8,
                controller: "did:key:test1".to_string(),
                dimensions: vec![],
            },
            previous: vec![],
            data: vec![1, 2, 3],
        }),
        ConclusionEvent::Data(ConclusionData {
            event_cid: Cid::default(),
            init: ConclusionInit {
                stream_cid: Cid::default(),
                stream_type: StreamIdType::Model as u8,
                controller: "did:key:test2".to_string(),
                dimensions: vec![],
            },
            previous: vec![Cid::default()],
            data: vec![4, 5, 6],
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
    assert_eq!(schema.field(1).name(), "stream_cid");
    assert_eq!(schema.field(2).name(), "stream_type");
    assert_eq!(schema.field(3).name(), "controller");
    assert_eq!(schema.field(4).name(), "event_cid");
    assert_eq!(schema.field(5).name(), "data");
    assert_eq!(schema.field(6).name(), "previous");

    let event_types = record_batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();
    // Value 0 is for data event type, value 1 is for time event type.
    // Both events are data events in this test.
    assert_eq!(event_types.value(0), 0);
    assert_eq!(event_types.value(1), 0);

    let stream_ids = record_batch
        .column(1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();

    assert_eq!(stream_ids.value(0), Cid::default().to_bytes());
    assert_eq!(stream_ids.value(1), Cid::default().to_bytes());

    let stream_types = record_batch
        .column(2)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();
    assert_eq!(stream_types.value(0), 2);
    assert_eq!(stream_types.value(1), 2);

    let controllers = record_batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(controllers.value(0), "did:key:test1");
    assert_eq!(controllers.value(1), "did:key:test2");

    let event_cids = record_batch
        .column(4)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(event_cids.value(0), Cid::default().to_bytes());
    assert_eq!(event_cids.value(1), Cid::default().to_bytes());

    let data = record_batch
        .column(5)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(data.value(0), &[1, 2, 3]);
    assert_eq!(data.value(1), &[4, 5, 6]);

    let previous = record_batch
        .column(6)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(previous.value_length(0), 0);
    assert_eq!(previous.value_length(1), 1);
}
