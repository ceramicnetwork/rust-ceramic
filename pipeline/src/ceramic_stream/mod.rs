mod interface_util;
pub mod model;
pub mod model_instance;
mod model_versions;
pub mod schema_validator;

pub(crate) use interface_util::InterfaceUtil;

// TODO: module info while I'm OOO
// The both model and model_instance structs implemente a `validate` function.
// This is the expected entry point and if we can provide those inputs, does
// _most_ of the expected schema validation. There are a handful of open questions
// mostly, in comments in specific functions. The `interface_util` module is lacking
// the most functionality currently (i.e. JSON schema comparisons), and there are a
// handful of checks related to headers (e.g. unique) that are missing. We could either
// include that data, or move any checks like that we want to the event validation (write path)
// side via an initial pass that is a sort of `validate_stream_type(event)` function.
