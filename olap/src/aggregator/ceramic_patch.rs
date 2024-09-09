use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{Array as _, ArrayRef, StringBuilder},
    datatypes::DataType,
};
use datafusion::{
    common::{
        cast::{as_binary_array, as_string_array},
        exec_datafusion_err, exec_err, Result,
    },
    logical_expr::{
        PartitionEvaluator, Signature, TypeSignature, Volatility, WindowUDF, WindowUDFImpl,
    },
};

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
                    DataType::Binary,
                    DataType::Binary,
                    DataType::Utf8,
                    DataType::Utf8,
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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn partition_evaluator(&self) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(CeramicPatchEvaluator))
    }
}

#[derive(Debug)]
struct CeramicPatchEvaluator;

impl CeramicPatchEvaluator {
    fn apply_patch(patch: &str, previous_state: &str) -> Result<String> {
        let patch: Vec<json_patch::PatchOperation> = serde_json::from_str(patch)
            .map_err(|err| exec_datafusion_err!("Error parsing patch: {err}"))?;
        let mut new_state: serde_json::Value = serde_json::from_str(previous_state)
            .map_err(|err| exec_datafusion_err!("Error parsing previous state: {err}"))?;
        json_patch::patch(&mut new_state, &patch)
            .map_err(|err| exec_datafusion_err!("Error applying JSON patch: {err}"))?;
        serde_json::to_string(&new_state)
            .map_err(|err| exec_datafusion_err!("Error JSON encoding: {err}"))
    }
}

impl PartitionEvaluator for CeramicPatchEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        let event_cids = as_binary_array(&values[0])?;
        let previous_cids = as_binary_array(&values[1])?;
        let previous_states = as_string_array(&values[2])?;
        let patches = as_string_array(&values[3])?;
        // TODO: avoid using String here
        let mut new_states: HashMap<&[u8], String> = HashMap::new();
        // Repeatedly iterate events until we have determined the new state of all events.
        loop {
            let mut progress = false;
            for i in 0..num_rows {
                let cid = event_cids.value(i);
                if new_states.contains_key(cid) {
                    // We already determine this event's new state
                    continue;
                }
                if previous_cids.is_null(i) {
                    //Init event, patch value is the initial state
                    if patches.is_null(i) {
                        // If we have an init event without data use an empty object as the initial
                        // state. This feels like a leaky abstraction is this expected based on the
                        // Ceramic spec?
                        new_states.insert(cid, "{}".to_string());
                    } else {
                        new_states.insert(cid, patches.value(i).to_string());
                    }
                    progress = true;
                } else if let Some(previous_state) = if !previous_states.is_null(i) {
                    Some(previous_states.value(i))
                } else {
                    new_states.get(previous_cids.value(i)).map(String::as_str)
                } {
                    let new_state = if patches.is_null(i) {
                        //We have a time event, new state is just the previous state
                        previous_state.to_string()
                    } else {
                        CeramicPatchEvaluator::apply_patch(patches.value(i), previous_state)?
                    };
                    new_states.insert(cid, new_state);
                    progress = true;
                }
            }
            if !progress {
                //TODO provide summary data about events that do not have a previous
                return exec_err!("broken chain, missing events");
            }
            if new_states.len() == num_rows {
                break;
            }
        }
        // Construct arrow array from all new states
        let size = new_states.values().map(String::len).sum();
        let mut new_states_arr = StringBuilder::with_capacity(new_states.len(), size);
        for i in 0..num_rows {
            let cid = event_cids.value(i);
            new_states_arr.append_value(
                new_states
                    .get(&cid)
                    .ok_or_else(|| exec_datafusion_err!("invalid conclusion events found"))?,
            );
        }
        Ok(Arc::new(new_states_arr.finish()))
    }
}
