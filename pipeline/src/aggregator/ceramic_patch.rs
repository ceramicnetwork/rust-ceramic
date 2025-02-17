use std::{collections::BTreeMap, sync::Arc};

use arrow::{
    array::{Array as _, ArrayBuilder as _, ArrayRef, BinaryBuilder},
    datatypes::DataType,
};
use arrow_schema::Field;
use datafusion::{
    common::{cast::as_binary_array, exec_datafusion_err, Result},
    logical_expr::{
        function::PartitionEvaluatorArgs, PartitionEvaluator, Signature, TypeSignature, Volatility,
        WindowUDF, WindowUDFImpl,
    },
};
use json_patch::PatchOperation;

/// Applies a Ceramic data event to a document state returning the new document state.
#[derive(Debug)]
pub struct CeramicPatch {
    signature: Signature,
}

impl CeramicPatch {
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
                    // State/Patch
                    DataType::Binary,
                ]),
                Volatility::Immutable,
            ),
        }
    }
}

impl WindowUDFImpl for CeramicPatch {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "ceramic_patch"
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
    ) -> Result<arrow_schema::Field> {
        Ok(Field::new(field_args.name(), DataType::Binary, false))
    }
}
// Small wrapper container around the data/state fields to hold
// other mutable metadata for the event.
// This is specific to Model Instance Documents.
// Metadata is considered to be mutable from event to event and a overriting merge is performed
// with the previous metadata to the current metadata.
// This means if a metadata key is missing it is propogated forward until a new data event changes
// its value.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MIDDataContainer<D> {
    metadata: BTreeMap<String, serde_json::Value>,
    content: D,
}

type MIDDataContainerPatch = MIDDataContainer<Vec<PatchOperation>>;
type MIDDataContainerState = MIDDataContainer<serde_json::Value>;

#[derive(Debug)]
struct CeramicPatchEvaluator;

impl CeramicPatchEvaluator {
    fn apply_patch(patch: &[u8], previous_state: &[u8]) -> Result<Vec<u8>> {
        let patch: MIDDataContainerPatch = serde_json::from_slice(patch)
            .map_err(|err| exec_datafusion_err!("Error parsing patch: {err}"))?;
        let mut state: MIDDataContainerState = serde_json::from_slice(previous_state)
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
        let patches = as_binary_array(&values[3])?;
        let mut new_states = BinaryBuilder::new();
        for i in 0..num_rows {
            if previous_cids.is_valid(i) {
                if let Some(previous_state) = if !previous_states.is_null(i) {
                    // We know the previous state already
                    Some(previous_states.value(i))
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
                            break Some(value_at(&new_states, j));
                        }
                    }
                } {
                    if patches.is_null(i) {
                        // We have a time event, new state is just the previous state
                        //
                        // Allow clippy warning as previous_state is a reference back into new_states.
                        // So we need to copy the data to a new location before we can copy it back
                        // into the new_states.
                        #[allow(clippy::unnecessary_to_owned)]
                        new_states.append_value(previous_state.to_owned());
                    } else {
                        new_states.append_value(CeramicPatchEvaluator::apply_patch(
                            patches.value(i),
                            previous_state,
                        )?);
                    }
                } else {
                    // Unreachable when data is well formed.
                    // Appending null means well formed documents can continue to be aggregated.
                    new_states.append_null();
                }
            } else {
                //Init event, patch value is the initial state
                if patches.is_null(i) {
                    // We have an init event without data
                    new_states.append_null();
                } else {
                    new_states.append_value(patches.value(i));
                }
            }
        }
        Ok(Arc::new(new_states.finish()))
    }
}

fn value_at(builder: &BinaryBuilder, idx: usize) -> &[u8] {
    let start = builder.offsets_slice()[idx] as usize;
    let stop = if idx < builder.len() {
        builder.offsets_slice()[idx + 1] as usize
    } else {
        builder.values_slice().len()
    };
    &builder.values_slice()[start..stop]
}
