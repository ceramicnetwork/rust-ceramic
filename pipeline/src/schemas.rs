//! Expose the schema for each of the tables in the pipeline.
use std::sync::{Arc, OnceLock};

use datafusion::arrow::datatypes::{DataType, Field, Fields, SchemaBuilder, SchemaRef};

static DOC_STATE: OnceLock<SchemaRef> = OnceLock::new();

/// The `doc_state` table contains the aggregated state for each event for each stream.
pub fn doc_state() -> SchemaRef {
    Arc::clone(DOC_STATE.get_or_init(|| {
        Arc::new(
            SchemaBuilder::from(&Fields::from([
                Arc::new(Field::new("index", DataType::UInt64, false)),
                Arc::new(Field::new("stream_cid", DataType::Binary, false)),
                Arc::new(Field::new("event_type", DataType::UInt8, false)),
                Arc::new(Field::new("controller", DataType::Utf8, false)),
                Arc::new(Field::new(
                    "dimensions",
                    DataType::Map(
                        Field::new(
                            "entries",
                            DataType::Struct(
                                vec![
                                    Field::new("key", DataType::Utf8, false),
                                    Field::new(
                                        "value",
                                        DataType::Dictionary(
                                            Box::new(DataType::Int32),
                                            Box::new(DataType::Binary),
                                        ),
                                        true,
                                    ),
                                ]
                                .into(),
                            ),
                            false,
                        )
                        .into(),
                        false,
                    ),
                    true,
                )),
                Arc::new(Field::new("event_cid", DataType::Binary, false)),
                Arc::new(Field::new("state", DataType::Utf8, true)),
            ]))
            .finish(),
        )
    }))
}
