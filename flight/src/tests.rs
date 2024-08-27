use super::*;
use crate::types::{ConclusionData, ConclusionEvent, ConclusionInit};
use arrow::{
    array::{Array, BinaryArray, RecordBatch},
    util::pretty::pretty_format_batches,
};
use ceramic_core::StreamIdType;
use cid::Cid;
use expect_test::expect;
use hex;
use std::str::FromStr;

fn convert_cids_to_string(record_batch: &RecordBatch) -> String {
    let mut formatted = pretty_format_batches(&[record_batch.clone()])
        .unwrap()
        .to_string();

    // Assuming that `stream_cid` and `event_cid` are the columns that need conversion
    if let Some(array) = record_batch
        .column(record_batch.schema().index_of("stream_cid").unwrap())
        .as_any()
        .downcast_ref::<BinaryArray>()
    {
        for i in 0..array.len() {
            let cid_bytes = array.value(i);
            let cid = Cid::try_from(cid_bytes).expect("Invalid CID");
            let cid_str = cid.to_string();
            formatted = formatted.replace(&hex::encode(cid_bytes), &cid_str);
        }
    }

    if let Some(array) = record_batch
        .column(record_batch.schema().index_of("event_cid").unwrap())
        .as_any()
        .downcast_ref::<BinaryArray>()
    {
        for i in 0..array.len() {
            let cid_bytes = array.value(i);
            let cid = Cid::try_from(cid_bytes).expect("Invalid CID");
            let cid_str = cid.to_string();
            formatted = formatted.replace(&hex::encode(cid_bytes), &cid_str);
        }
    }

    formatted
}

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
            previous: vec![Cid::from_str(
                "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
            )
            .unwrap()],
            data: vec![1, 2, 3],
        }),
        ConclusionEvent::Data(ConclusionData {
            event_cid: Cid::from_str("baeabeid2w5pgdsdh25nah7batmhxanbj3x2w2is3atser7qxboyojv236q")
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
                "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
            )
            .unwrap()],
            data: vec![4, 5, 6],
        }),
        ConclusionEvent::Time(ConclusionTime {
            event_cid: Cid::from_str("baeabeidtub3bnbojbickf6d4pqscaw6xpt5ksgido7kcsg2jyftaj237di")
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
                controller: "did:key:test3".to_string(),
                dimensions: vec![],
            },
        }),
        ConclusionEvent::Data(ConclusionData {
            event_cid: Cid::from_str("baeabeiewqcj4bwhcssizv5kcyvsvm57bxghjpqshnbzkc6rijmwb4im4yq")
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
            previous: vec![Cid::from_str(
                "baeabeidtub3bnbojbickf6d4pqscaw6xpt5ksgido7kcsg2jyftaj237di",
            )
            .unwrap()],
            data: vec![7, 8, 9],
        }),
    ];
    // Convert events to RecordBatch
    let record_batch = conclusion_events_to_record_batch(&events).unwrap();

    // Convert RecordBatch to string
    // let formatted = pretty_format_batches(&[record_batch.clone()])
    //     .unwrap();
    let formatted = convert_cids_to_string(&record_batch);

    // Use expect_test to validate the output
    expect![[r#"
        +------------+--------------------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------+--------+----------------------------------------------------------------------------+
        | event_type | stream_cid                                                               | stream_type | controller    | event_cid                                                                | data   | previous                                                                   |
        +------------+--------------------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------+--------+----------------------------------------------------------------------------+
        | 0          | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:test1 | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 010203 | [baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu] |
        | 0          | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:test2 | baeabeid2w5pgdsdh25nah7batmhxanbj3x2w2is3atser7qxboyojv236q | 040506 | [baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi] |
        | 1          | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:test3 | baeabeidtub3bnbojbickf6d4pqscaw6xpt5ksgido7kcsg2jyftaj237di |        | [baeabeid2w5pgdsdh25nah7batmhxanbj3x2w2is3atser7qxboyojv236q] |
        | 0          | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:test4 | baeabeiewqcj4bwhcssizv5kcyvsvm57bxghjpqshnbzkc6rijmwb4im4yq | 070809 | [baeabeidtub3bnbojbickf6d4pqscaw6xpt5ksgido7kcsg2jyftaj237di] |
        +------------+--------------------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------+--------+----------------------------------------------------------------------------+"#]].assert_eq(&formatted);
}
