use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use anyhow::Result;
use cid::Cid;
use jsonschema::Validator;
use lru::LruCache;

use crate::aggregator::result::{ResultValidation as _, ValidationResult};

use super::model::ModelDefinition;

#[derive(Clone, Debug)]
pub struct SchemaValidator {
    include_errors: bool,
    schemas: Arc<Mutex<LruCache<Cid, Arc<Validator>>>>,
}

impl SchemaValidator {
    /// `cache_size`: number of schema validators to keep in the LRU cache (slow to create)
    /// Including errors during validation is much slower but worth it when you need to understand why
    pub fn new(cache_size: usize, include_errors: bool) -> Self {
        let size = NonZeroUsize::new(cache_size)
            .unwrap_or_else(|| NonZeroUsize::new(100).expect("100 is non zero"));
        Self {
            include_errors,
            schemas: Arc::new(Mutex::new(LruCache::new(size))),
        }
    }

    /// Validates the ModelInstanceDocument content generated as a result of the patch conforms to the model schema
    pub fn validate_mid_conforms_to_model(
        &self,
        mid_data: &serde_json::Value,
        model_version: &Cid,
        model: &ModelDefinition,
    ) -> ValidationResult {
        let validator = maybe_fail!(self
            .get_schema_validator(model_version, model)
            .map_to_validation_internal_err());
        if validator.is_valid(mid_data) {
            ValidationResult::Pass(())
        } else {
            // much slower to do this
            if self.include_errors {
                let mut errs = Vec::new();
                for err in validator.iter_errors(mid_data) {
                    if err.instance_path.as_str().is_empty() {
                        errs.push(err.to_string())
                    } else {
                        errs.push(format!("'{err}' at {}", err.instance_path));
                    }
                }
                errs.into()
            } else {
                fail!("Schema validation failed")
            }
        }
    }

    fn get_schema_validator(
        &self,
        model_version: &Cid,
        model: &ModelDefinition,
    ) -> Result<Arc<Validator>> {
        let mut cached_validators = self.schemas.lock().unwrap();
        let validator = if let Some(v) = cached_validators.get(model_version) {
            v.clone()
        } else {
            let schema = model.schema();
            let validator = Arc::new(jsonschema::validator_for(schema.as_value())?);
            cached_validators.push(*model_version, validator.clone());
            validator
        };
        Ok(validator)
    }
}

#[cfg(test)]
mod test {
    use ceramic_core::METAMODEL_STREAM_ID;
    use ceramic_event::unvalidated::Event;

    use super::*;

    #[test]
    fn gitcoin_attestation_from_car() {
        let car_from_c1 = "mO6Jlcm9vdHOB2CpYJgABhQESIBlux++Tb7MhYC8wK34upI1yJ03KXTsr+UX9bpBNVvo8Z3ZlcnNpb24B/AMBcRIgfakrjBwrt3EimC6P51wcQOCHoA86AfnG74544M+AHHmiZGRhdGGpZG5hbWVyR2l0Y29pbkF0dGVzdGF0aW9uZXZpZXdzoGZzY2hlbWGlZHR5cGVmb2JqZWN0ZyRzY2hlbWF4LGh0dHBzOi8vanNvbi1zY2hlbWEub3JnL2RyYWZ0LzIwMjAtMTIvc2NoZW1haHJlcXVpcmVkgWR0eXBlanByb3BlcnRpZXOhZHR5cGWjZHR5cGVlYXJyYXllaXRlbXOjZHR5cGVmc3RyaW5naW1heExlbmd0aBkEAGltaW5MZW5ndGgBaG1heEl0ZW1zGQQAdGFkZGl0aW9uYWxQcm9wZXJ0aWVz9Gd2ZXJzaW9uYzIuMGlpbnRlcmZhY2X1aXJlbGF0aW9uc6BqaW1wbGVtZW50c4BrZGVzY3JpcHRpb254JkJhc2UgY2xhc3MgZm9yIGFueSBHaXRjb2luIGF0dGVzdGF0aW9ub2FjY291bnRSZWxhdGlvbqFkdHlwZWRub25lZmhlYWRlcqNjc2VwZW1vZGVsZW1vZGVsUs4BBAFxcQsACWhtb2RlbC12MWtjb250cm9sbGVyc4F4OGRpZDprZXk6ejZNa2dVek5ZVjhKMXl3NDN3ajlLMkNiaFRab04yNXVaNlRKM0dpNGNZVnBaeURiugIBhQESIBlux++Tb7MhYC8wK34upI1yJ03KXTsr+UX9bpBNVvo8omdwYXlsb2FkWCQBcRIgfakrjBwrt3EimC6P51wcQOCHoA86AfnG74544M+AHHlqc2lnbmF0dXJlc4GiaXByb3RlY3RlZFiBeyJhbGciOiJFZERTQSIsImtpZCI6ImRpZDprZXk6ejZNa2dVek5ZVjhKMXl3NDN3ajlLMkNiaFRab04yNXVaNlRKM0dpNGNZVnBaeURiI3o2TWtnVXpOWVY4SjF5dzQzd2o5SzJDYmhUWm9OMjV1WjZUSjNHaTRjWVZwWnlEYiJ9aXNpZ25hdHVyZVhAkQc+CgkTB6b2ZD/TG23JytECfb4rMNrdeS9rktqnR1/t/gGCUIHj2eRV5GIbxKHNx5MDXiH9lS2f5yAHx1RXCA";
        let (_base, data) = multibase::decode(car_from_c1).unwrap();
        let (_cid, event) = Event::<ModelDefinition>::decode_car(data.as_slice(), true).unwrap();
        match event {
            Event::Time(_) => unreachable!(),
            Event::Signed(event) => {
                let data = match event.payload() {
                    ceramic_event::unvalidated::Payload::Data(_) => {
                        unreachable!("models can't be updated")
                    }
                    ceramic_event::unvalidated::Payload::Init(p) => p.data().unwrap(),
                };
                data.validate(None, None, Some(METAMODEL_STREAM_ID))
                    .unwrap();
            }
            Event::Unsigned(_) => unreachable!(),
        }
    }
}
