//! Expose the schema for each of the tables in the pipeline.
use std::sync::{Arc, OnceLock};

use datafusion::arrow::datatypes::{DataType, Field, Fields, SchemaBuilder, SchemaRef};

static CONCLUSION_EVENTS: OnceLock<SchemaRef> = OnceLock::new();
static EVENT_STATES: OnceLock<SchemaRef> = OnceLock::new();

/// The `conclusion_events` table contains the raw events annotated with conclusions about each
/// event.
pub fn conclusion_events() -> SchemaRef {
    Arc::clone(CONCLUSION_EVENTS.get_or_init(|| {
        Arc::new(
            SchemaBuilder::from(&Fields::from(vec![
                Field::new("index", DataType::UInt64, false),
                Field::new("stream_cid", DataType::Binary, false),
                Field::new("stream_type", DataType::UInt8, false),
                Field::new("controller", DataType::Utf8, false),
                Field::new(
                    // NOTE: The entire dimensions map may be null or values for a given key may
                    // be null. No other aspect of dimensions may be null.
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
                ),
                Field::new("event_cid", DataType::Binary, false),
                Field::new("event_type", DataType::UInt8, false),
                Field::new("data", DataType::Binary, true),
                Field::new(
                    "previous",
                    DataType::List(Arc::new(Field::new("item", DataType::Binary, false))),
                    true,
                ),
            ]))
            .finish(),
        )
    }))
}

/// The `event_states` table contains the aggregated state for each event for each stream.
pub fn event_states() -> SchemaRef {
    Arc::clone(EVENT_STATES.get_or_init(|| {
        Arc::new(
            SchemaBuilder::from(&Fields::from([
                Arc::new(Field::new("index", DataType::UInt64, false)),
                Arc::new(Field::new("stream_cid", DataType::Binary, false)),
                Arc::new(Field::new("stream_type", DataType::UInt8, false)),
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
                Arc::new(Field::new("event_type", DataType::UInt8, false)),
                Arc::new(Field::new("state", DataType::Binary, true)),
            ]))
            .finish(),
        )
    }))
}
