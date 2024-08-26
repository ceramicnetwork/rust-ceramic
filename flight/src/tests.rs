use super::*;
use crate::types::{ConclusionData, ConclusionEvent, ConclusionInit};
use arrow::util::pretty::pretty_format_batches;
use ceramic_core::StreamIdType;
use cid::Cid;
use expect_test::expect;
use std::str::FromStr;

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
            event_cid: Cid::from_str("baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi")
                .unwrap(),
            init: ConclusionInit {
                stream_cid: Cid::from_str(
                    "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                )
                .unwrap(),
                stream_type: StreamIdType::Model as u8,
                controller: "did:key:test1".to_string(),
                dimensions: vec![],
            },
            previous: vec![],
            data: vec![1, 2, 3],
        }),
        ConclusionEvent::Data(ConclusionData {
            event_cid: Cid::from_str("baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi")
                .unwrap(),
            init: ConclusionInit {
                stream_cid: Cid::from_str(
                    "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                )
                .unwrap(),
                stream_type: StreamIdType::Model as u8,
                controller: "did:key:test2".to_string(),
                dimensions: vec![],
            },
            previous: vec![Cid::from_str(
                "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
            )
            .unwrap()],
            data: vec![4, 5, 6],
        }),
        ConclusionEvent::Time(ConclusionTime {
            event_cid: Cid::from_str("baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi")
                .unwrap(),
            previous: vec![Cid::from_str(
                "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
            )
            .unwrap()],
            init: ConclusionInit {
                stream_cid: Cid::from_str(
                    "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                )
                .unwrap(),
                stream_type: StreamIdType::Model as u8,
                controller: "did:key:test3".to_string(),
                dimensions: vec![],
            },
        }),
        ConclusionEvent::Data(ConclusionData {
            event_cid: Cid::from_str("baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi")
                .unwrap(),
            init: ConclusionInit {
                stream_cid: Cid::from_str(
                    "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                )
                .unwrap(),
                stream_type: StreamIdType::Model as u8,
                controller: "did:key:test4".to_string(),
                dimensions: vec![],
            },
            previous: vec![],
            data: vec![7, 8, 9],
        }),
    ];
    // Convert events to RecordBatch
    let record_batch = conclusion_events_to_record_batch(&events).unwrap();

    // Convert RecordBatch to string
    let formatted = pretty_format_batches(&[record_batch.clone()])
        .unwrap()
        .to_string();

    // Use expect_test to validate the output
    expect![[r#"
        +------------+--------------------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------+--------+----------------------------------------------------------------------------+
        | event_type | stream_cid                                                               | stream_type | controller    | event_cid                                                                | data   | previous                                                                   |
        +------------+--------------------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------+--------+----------------------------------------------------------------------------+
        | 0          | 01001220ba28cb0268f4f50cca99d952b610bfa2823941bfb12452ccf1e330725d96ae0d | 2           | did:key:test1 | 010012200b9691e769ef527bd51427fc745c8bfb8e89271e054548a780ddeed7817cbe0a | 010203 | []                                                                         |
        | 0          | 01001220ba28cb0268f4f50cca99d952b610bfa2823941bfb12452ccf1e330725d96ae0d | 2           | did:key:test2 | 010012200b9691e769ef527bd51427fc745c8bfb8e89271e054548a780ddeed7817cbe0a | 040506 | [01001220ba28cb0268f4f50cca99d952b610bfa2823941bfb12452ccf1e330725d96ae0d] |
        | 1          | 01001220ba28cb0268f4f50cca99d952b610bfa2823941bfb12452ccf1e330725d96ae0d | 2           | did:key:test3 | 010012200b9691e769ef527bd51427fc745c8bfb8e89271e054548a780ddeed7817cbe0a |        |                                                                            |
        | 0          | 01001220ba28cb0268f4f50cca99d952b610bfa2823941bfb12452ccf1e330725d96ae0d | 2           | did:key:test4 | 010012200b9691e769ef527bd51427fc745c8bfb8e89271e054548a780ddeed7817cbe0a | 070809 | []                                                                         |
        +------------+--------------------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------+--------+----------------------------------------------------------------------------+"#]].assert_eq(&formatted);
}
