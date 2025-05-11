//! Expose the schema for each of the tables in the pipeline.
use std::sync::{Arc, OnceLock};

use datafusion::arrow::datatypes::{DataType, Field, Fields, SchemaBuilder, SchemaRef};

static CONCLUSION_EVENTS: OnceLock<SchemaRef> = OnceLock::new();
static EVENT_STATES: OnceLock<SchemaRef> = OnceLock::new();
static EVENT_STATES_PARTITIONED: OnceLock<SchemaRef> = OnceLock::new();
static PENDING_EVENT_STATES: OnceLock<SchemaRef> = OnceLock::new();
static STREAM_TIPS: OnceLock<SchemaRef> = OnceLock::new();
static STREAM_TIPS_PARTITIONED: OnceLock<SchemaRef> = OnceLock::new();
static STREAM_STATES: OnceLock<SchemaRef> = OnceLock::new();
static _STREAM_STATES_PARTITIONED: OnceLock<SchemaRef> = OnceLock::new();

/// The `conclusion_events` table contains the raw events annotated with conclusions about each
/// event.
pub fn conclusion_events() -> SchemaRef {
    Arc::clone(CONCLUSION_EVENTS.get_or_init(|| {
        Arc::new(
            SchemaBuilder::from(&Fields::from(vec![
                // An order that ensures an event row comes after all rows in its stream
                // reachable from the row itself.
                Field::new("conclusion_event_order", DataType::UInt64, false),
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
                Field::new("before", DataType::UInt64, true),
                Field::new("chain_id", DataType::Utf8, true),
            ]))
            .finish(),
        )
    }))
}

/// The `event_states` table contains the aggregated state for each event for each stream.
pub fn event_states() -> SchemaRef {
    Arc::clone(EVENT_STATES.get_or_init(|| {
        Arc::new(
            SchemaBuilder::from(&Fields::from(vec![
                // An order that ensures an event row comes after all rows in its stream
                // reachable from the row itself.
                Field::new("conclusion_event_order", DataType::UInt64, false),
                // An order that ensures an event state row comes after all rows needed to
                // construct its state. This order is stream type dependent, but holds for all
                // stream types.
                Field::new("event_state_order", DataType::UInt64, false),
                Field::new("stream_cid", DataType::Binary, false),
                Field::new("stream_type", DataType::UInt8, false),
                Field::new("controller", DataType::Utf8, false),
                Field::new(
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
                Field::new("event_height", DataType::UInt32, true),
                Field::new("data", DataType::Binary, true),
                Field::new(
                    "validation_errors",
                    DataType::List(Field::new_list_field(DataType::Utf8, true).into()),
                    true,
                ),
                Field::new("before", DataType::UInt64, true),
                Field::new("chain_id", DataType::Utf8, true),
            ]))
            .finish(),
        )
    }))
}

/// The `event_states` table contains the aggregated state for each event for each stream.
/// This schema includes the partition columns of the table.
pub fn event_states_partitioned() -> SchemaRef {
    Arc::clone(EVENT_STATES_PARTITIONED.get_or_init(|| {
        Arc::new(
            arrow_schema::SchemaBuilder::from(&arrow_schema::Fields::from(
                // Append partition fields to the end of the unpartitioned schema
                event_states()
                    .fields()
                    .into_iter()
                    .cloned()
                    .chain(vec![Arc::new(arrow_schema::Field::new(
                        "event_cid_partition",
                        DataType::Int32,
                        false,
                    ))])
                    .collect::<Vec<_>>(),
            ))
            .finish(),
        )
    }))
}

/// The `pending_event_states` table contains the intermmediate events for mids where their model
/// event is not yet processed.
pub fn pending_event_states() -> SchemaRef {
    Arc::clone(PENDING_EVENT_STATES.get_or_init(|| {
        Arc::new(
            SchemaBuilder::from(&Fields::from(vec![
                // An order that ensures an event row comes after all rows in its stream
                // reachable from the row itself.
                Field::new("conclusion_event_order", DataType::UInt64, false),
                Field::new("stream_cid", DataType::Binary, false),
                Field::new("stream_type", DataType::UInt8, false),
                Field::new("controller", DataType::Utf8, false),
                Field::new(
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
                Field::new("event_height", DataType::UInt32, true),
                Field::new("data", DataType::Binary, true),
                Field::new("patch", DataType::Binary, true),
                Field::new("model_version", DataType::Binary, true),
                Field::new("before", DataType::UInt64, true),
                Field::new("chain_id", DataType::Utf8, true),
            ]))
            .finish(),
        )
    }))
}

/// The `stream_tips` table contains the set of tips for each stream
pub fn stream_tips() -> SchemaRef {
    Arc::clone(STREAM_TIPS.get_or_init(|| {
        Arc::new(
            SchemaBuilder::from(&Fields::from(vec![
                // Used to know how far through the event_states table the stream_tips table has
                // processed.
                Field::new("event_state_order", DataType::UInt64, false),
                // An order that ensures an stream tip row comes after all rows needed to
                // compute the tips of a stream. This order is stream type dependent, but holds for all
                // stream types.
                Field::new("stream_tip_order", DataType::UInt64, false),
                Field::new("stream_cid", DataType::Binary, false),
                Field::new("stream_type", DataType::UInt8, false),
                Field::new("controller", DataType::Utf8, false),
                Field::new(
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
                Field::new("tips", DataType::new_list(DataType::Binary, false), false),
            ]))
            .finish(),
        )
    }))
}

/// The `stream_tips_partitioned` table contains the set of tips for a stream.
/// This schema includes the partition columns of the table.
pub fn stream_tips_partitioned() -> SchemaRef {
    Arc::clone(STREAM_TIPS_PARTITIONED.get_or_init(|| {
        Arc::new(
            arrow_schema::SchemaBuilder::from(&arrow_schema::Fields::from(
                // Append partition fields to the end of the unpartitioned schema
                event_states()
                    .fields()
                    .into_iter()
                    .cloned()
                    .chain(vec![Arc::new(arrow_schema::Field::new(
                        "stream_cid_partition",
                        DataType::Int32,
                        false,
                    ))])
                    .collect::<Vec<_>>(),
            ))
            .finish(),
        )
    }))
}

/// The `stream_states` table contains the canonical tip for each stream
pub fn stream_states() -> SchemaRef {
    Arc::clone(STREAM_STATES.get_or_init(|| {
        Arc::new(
            SchemaBuilder::from(&Fields::from(vec![
                // Used to know how far through the stream_tips table the stream_state table has
                // processed.
                Field::new("stream_tip_order", DataType::UInt64, false),
                // An order that ensures each new stream state comes after all previous states of
                // that stream. This order is stream type dependent, but holds for all
                // stream types.
                Field::new("stream_state_order", DataType::UInt64, false),
                Field::new("stream_cid", DataType::Binary, false),
                Field::new("stream_type", DataType::UInt8, false),
                Field::new("controller", DataType::Utf8, false),
                Field::new(
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
                Field::new("event_height", DataType::UInt32, true),
                Field::new("data", DataType::Binary, true),
            ]))
            .finish(),
        )
    }))
}

/// The `stream_states_partitioned` table contains the canonical tip for each stream.
/// This schema includes the partition columns of the table.
pub fn stream_states_partitioned() -> SchemaRef {
    Arc::clone(STREAM_TIPS_PARTITIONED.get_or_init(|| {
        Arc::new(
            arrow_schema::SchemaBuilder::from(&arrow_schema::Fields::from(
                // Append partition fields to the end of the unpartitioned schema
                event_states()
                    .fields()
                    .into_iter()
                    .cloned()
                    .chain(vec![Arc::new(arrow_schema::Field::new(
                        "stream_cid_partition",
                        DataType::Int32,
                        false,
                    ))])
                    .collect::<Vec<_>>(),
            ))
            .finish(),
        )
    }))
}
