# Querying the Pipeline Tables

There are several pipeline table, these tables provide access to Ceramic stream states at various point along the processing pipeline.
See the [design](./DESIGN.md) doc for more details.

## Tables

These tables are available

* conclusion_events
* conclusion_events_feed
* event_states
* event_states_feed

The `*_feed` table contain the same data as their respective table and additionally contain all future data.
Meaning querying those tables returns a streaming query response that never terminates (unless an explicit LIMIT is used).
These `*_feed` tables are the main mechanism for subscribing to the feed of new events within Ceramic.

### conclusion_events

The conclusion_events table contains a row for each event in a stream and represents a raw input event after various conclusions have been made.

| Column                 | Type              | Description                                                                               |
| ------                 | ----              | -----------                                                                               |
| conclusion_event_order | u64               | Order of this event. Value is always greater than any previous event in the same stream   |
| stream_cid             | bytes             | Cid of the stream                                                                         |
| stream_type            | u8                | Type of the stream, see [stream type values](#stream-types)                               |
| controller             | string            | Controller of the stream                                                                  |
| dimensions             | map(string,bytes) | Set of key values dimension pairs of the stream                                           |
| event_cid              | bytes             | Cid of the event                                                                          |
| event_type             | u8                | Type of the event, see [event type values](#event-types)                                  |
| data                   | bytes             | The event payload, content is stream type specific                                        |
| previous               | list(bytes)       | Ordered list of CID previous to this event. Meaning of the order is stream type dependent |

#### Example Queries

Select all conclusion events for a stream:

```sql
SELECT
    cid_string(event_cid),
    data::varchar
FROM
    conclusion_events
WHERE
    cid_string(stream_cid) = 'k2t6wzhjp5kk1rr864mc5h30xpi9pnthzizp67jtuqp5ve81k2mcxg7qu4ylqf'
```

Select all conclusion events for a specific model:

```sql
SELECT
    cid_string(event_cid),
    data::varchar
FROM
    conclusion_events
WHERE
    stream_id_string(arrow_cast(dimension_extract(dimensions, 'model'), 'Binary'))
        = 'k2t6wz4yhfp1oyk54l2wkypm2shd8nxgywkm1pmi3jpxatnlfz0cd9urr3t3sr'
```

Select feed of all past and future conclusion events starting where the application last left off:

```sql
SELECT
    conclusion_event_order,
    cid_string(stream_cid),
    cid_string(event_cid),
    data::varchar
FROM
    conclusion_events_feed
WHERE
    conclusion_event_order > $last_conclusion_event_order
```

See the SDK on how to use parameters in prepared queries.

### event_states

The event_states table contains a row for each event in a stream and the state of the document at that point in the stream.

| Column                 | Type              | Description                                                                                                                                      |
| ------                 | ----              | -----------                                                                                                                                      |
| conclusion_event_order | u64               | Order of this event from the conclusion_events table.                                                                                            |
| event_state_order      | u64               | Order of this event state. Value is always greater than any previous event in the same stream and any dependent streams (i.e. model streams) |
| stream_cid             | bytes             | Cid of the stream                                                                                                                                |
| stream_type            | u8                | Type of the stream, see [stream type values](#stream-types)                                                                                      |
| controller             | string            | Controller of the stream                                                                                                                         |
| dimensions             | map(string,bytes) | Set of key values dimension pairs of the stream                                                                                                  |
| event_cid              | bytes             | Cid of the event                                                                                                                                 |
| event_type             | u8                | Type of the event, see [event type values](#event-types)                                                                                         |
| event_height           | i32               | Number of events between this event and the init event of the stream.                                                                            |
| data                   | bytes             | The event payload, content is stream type specific                                                                                               |
| validation_errors      | list(string)      | List of validation errors, will always be an empty list (not null) when the stream state is valid.                                               |

#### Example Queries

Select only valid event states for a stream:

```sql
SELECT
    cid_string(event_cid),
    data::varchar
FROM
    event_states
WHERE
        cid_string(stream_cid) = 'k2t6wzhjp5kk1rr864mc5h30xpi9pnthzizp67jtuqp5ve81k2mcxg7qu4ylqf'
    AND
        empty(validation_errors)
```

Select only valid events states for a specific model:

```sql
SELECT
    cid_string(event_cid),
    data::varchar
FROM
    event_states
WHERE
        stream_id_string(arrow_cast(dimension_extract(dimensions, 'model'), 'Binary'))
            = 'k2t6wz4yhfp1oyk54l2wkypm2shd8nxgywkm1pmi3jpxatnlfz0cd9urr3t3sr'
    AND
        empty(validation_errors)
```

Select feed of valid past and future event states starting where the application last left off:

```sql
SELECT
    conclusion_event_order,
    cid_string(stream_cid),
    cid_string(event_cid),
    data::varchar
FROM
    event_states_feed
WHERE
        conclusion_event_order > $last_conclusion_event_order
    AND
        empty(validation_errors)

```
Select list of validation error across all model instance document events

```sql
SELECT
    distinct validation_errors
FROM
    event_states
WHERE
    stream_type = 3
```

Select distinct models used by all model instance document events

```sql
SELECT
    distinct stream_id_string(arrow_cast(dimension_extract(dimensions, 'model'), 'Binary'))
FROM
    event_states
WHERE
    stream_type = 3
```

Select distinct controllers used by all events

```sql
SELECT
    distinct arrow_cast(dimension_extract(dimensions, 'controller'),'Binary')::varchar
FROM
    event_states
```

Select specific values from json payload of an event state.

```sql
SELECT
    data::varchar->'content'->'color'->>'red' as red,
    data::varchar->'content'->'color'->>'green' as green,
    data::varchar->'content'->'color'->>'blue' as blue
FROM
    event_states
```

See [datafusion json functions](https://github.com/datafusion-contrib/datafusion-functions-json) for full docs on supportted JSON functions.

## Functions

Ceramic-one ships with some builtin functions beyond the base [datafusion functions](https://datafusion.apache.org/user-guide/sql/index.html)

### cid_string

Returns the string representation of a cid. Expects as input binary data.

    cid_string(binary_expression)

### array_cid_string

Returns an array of string representations of cids. Expects as input an array of binary data.

    array_cid_string(array_binary_expression)


### stream_id_string

Returns the string representation of a stream id. Expects as input binary data.

    stream_id_string(binary_expression)

### array_stream_id_string

Returns an array of string representations of stream ids. Expects as input an array of binary data.

    array_stream_id_string(array_binary_expression)


### stream_id_to_cid

Returns the binary representation of the cid portion of a stream id. Expects as input binary data.

    stream_id_to_cid(binary_expression)

### dimension_extract

Returns the value of a named dimension. Expects as input a map and a key name.

    dimension_extract(map_expression, string_expression)

>NOTE: Dimensions are dictionary encoded and dimension_extract preserves this encoding.
As such on occasion it is helpful to explicity cast to a binary column instead of a dictionary column for other functions that cannot process dictionary encoded columns.
For example: stream_id_string(arrow_cast(dimension_extract(dimensions, 'model'), 'Binary')), that expression extracts the model stream id bytes from the dimensions, casts it to a binary expression and then passes it to stream_id_string to produce the string representation of the stream id.
