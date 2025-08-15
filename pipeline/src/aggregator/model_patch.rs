use std::sync::Arc;

use arrow::{
    array::{Array as _, ArrayRef, BinaryBuilder, StructArray, UInt32Builder},
    datatypes::DataType,
};
use arrow_schema::Field;
use cid::Cid;
use datafusion::{
    common::{
        cast::{as_binary_array, as_uint32_array},
        exec_datafusion_err, Result,
    },
    logical_expr::{
        function::PartitionEvaluatorArgs, PartitionEvaluator, Signature, TypeSignature, Volatility,
        WindowUDF, WindowUDFImpl,
    },
};
use json_patch::PatchOperation;
use tracing::warn;

use super::{bytes_value_at, u32_value_at, EventDataContainer};

/// Applies a Ceramic data event to a document state returning the new document state.
#[derive(Debug)]
pub struct ModelPatch {
    signature: Signature,
}

impl ModelPatch {
    pub fn new_udwf() -> WindowUDF {
        WindowUDF::new_from_impl(Self::new())
    }
    fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![
                    // Event CID
                    DataType::Binary,
                    // Previous CID
                    DataType::Binary,
                    // Previous State
                    DataType::Binary,
                    // Previous event height
                    DataType::UInt32,
                    // State/Patch
                    DataType::Binary,
                ]),
                Volatility::Immutable,
            ),
        }
    }
}

impl WindowUDFImpl for ModelPatch {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "model_patch"
    }

    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(CeramicPatchEvaluator))
    }

    fn field(
        &self,
        field_args: datafusion::logical_expr::function::WindowUDFFieldArgs,
    ) -> Result<Arc<arrow_schema::Field>> {
        Ok(Arc::new(Field::new_struct(
            field_args.name(),
            vec![
                Field::new("previous_data", DataType::Binary, true),
                Field::new("data", DataType::Binary, true),
                Field::new("event_height", DataType::UInt32, true),
            ],
            true,
        )))
    }
}

type DataContainerPatch = EventDataContainer<Vec<PatchOperation>>;
type DataContainerState = EventDataContainer<serde_json::Value>;

#[derive(Debug)]
struct CeramicPatchEvaluator;

impl CeramicPatchEvaluator {
    fn apply_patch(patch: &[u8], previous_state: &[u8]) -> Result<Vec<u8>> {
        let patch: DataContainerPatch = serde_json::from_slice(patch)
            .map_err(|err| exec_datafusion_err!("Error parsing patch: {err}"))?;
        let mut state: DataContainerState = serde_json::from_slice(previous_state)
            .map_err(|err| exec_datafusion_err!("Error parsing previous state: {err}"))?;
        // If the state is null use an empty object in order to apply the patch to a valid object.
        if serde_json::Value::Null == state.content {
            state.content = serde_json::Value::Object(serde_json::Map::default());
        }
        state.metadata.extend(patch.metadata);
        json_patch::patch(&mut state.content, &patch.content)
            .map_err(|err| exec_datafusion_err!("Error applying JSON patch: {err}"))?;
        serde_json::to_vec(&state).map_err(|err| exec_datafusion_err!("Error JSON encoding: {err}"))
    }
}

impl PartitionEvaluator for CeramicPatchEvaluator {
    // Compute the new state of each document for a batch of events.
    // Produces num_rows new document states, i.e. one for each input event.
    //
    // Assumption made by the function:
    //    * Window partitions are by stream_cid
    //    * Rows are ordered by the index column
    //
    // With these assumptions the code assumes it has all events for a stream and only events from
    // a single stream.
    // Additionally index sort order means that any event's previous event comes earlier in the
    // data set and so a single pass algorithm can be implemented.
    //
    // Input data must have the following columns:
    //     * event_cid - unique id of the event
    //     * previous_cid - id of the previous event, nullable implies an init event.
    //     * previous_state - state of the previous event, nullable implies that the previous event
    //         exists in the current dataset.
    //     * patch - json patch to apply
    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        let event_cids = as_binary_array(&values[0])?;
        let previous_cids = as_binary_array(&values[1])?;
        let previous_states = as_binary_array(&values[2])?;
        let previous_heights = as_uint32_array(&values[3])?;
        let patches = as_binary_array(&values[4])?;
        let mut new_states = BinaryBuilder::new();
        let mut new_heights = UInt32Builder::new();
        // We need to return the matched previous state so later we can do direct comparisons.
        let mut resolved_previous_states = BinaryBuilder::new();
        for i in 0..num_rows {
            if previous_cids.is_valid(i) {
                if let Some((previous_state, previous_height)) = if !previous_states.is_null(i) {
                    // We know the previous state already
                    Some((previous_states.value(i), previous_heights.value(i)))
                } else {
                    // Iterator backwards till we find the previous state among the new states.
                    let previous_cid = previous_cids.value(i);
                    let mut j = i;
                    loop {
                        if j == 0 {
                            break None;
                        }
                        j -= 1;
                        if event_cids.value(j) == previous_cid {
                            break Some((
                                bytes_value_at(&new_states, j),
                                u32_value_at(&new_heights, j),
                            ));
                        }
                    }
                } {
                    new_heights.append_value(previous_height + 1);
                    if patches.is_null(i) {
                        // We have a time event, new state is just the previous state
                        let state = previous_state.to_owned();
                        new_states.append_value(&state);
                        resolved_previous_states.append_value(&state);
                    } else {
                        resolved_previous_states.append_value(previous_state);
                        match Self::apply_patch(patches.value(i), previous_state) {
                            Ok(data) => {
                                new_states.append_value(data);
                            }
                            Err(err) => {
                                warn!(%err, event_cid=?Cid::read_bytes(event_cids.value(i)), "failed to apply patch to model event");
                                new_states.append_null();
                            }
                        };
                    }
                } else {
                    // Unreachable when data is well formed.
                    // Appending null means well formed documents can continue to be aggregated.
                    new_states.append_null();
                    resolved_previous_states.append_null();
                    new_heights.append_null();
                }
            } else {
                //Init event, patch value is the initial state
                new_heights.append_value(0);
                if patches.is_null(i) {
                    // We have an init event without data
                    new_states.append_null();
                    resolved_previous_states.append_null();
                } else {
                    new_states.append_value(patches.value(i));
                    resolved_previous_states.append_null();
                }
            }
        }

        Ok(Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("previous_data", DataType::Binary, true)),
                Arc::new(resolved_previous_states.finish()) as ArrayRef,
            ),
            (
                Arc::new(Field::new("data", DataType::Binary, true)),
                Arc::new(new_states.finish()) as ArrayRef,
            ),
            (
                Arc::new(Field::new("event_height", DataType::UInt32, true)),
                Arc::new(new_heights.finish()) as ArrayRef,
            ),
        ])))
    }
}
