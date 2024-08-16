use std::{any::Any, sync::Arc};

use arrow::{
    array::{Array as _, BinaryBuilder, BooleanBufferBuilder, ListBuilder, StructArray},
    datatypes::{DataType, Field, Fields},
};
use ceramic_event::unvalidated;
use cid::Cid;
use datafusion::{
    common::cast::as_binary_array,
    error::DataFusionError,
    logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility},
};
use serde_json::Value;
use tracing::debug;

/// UDF that extracts the car event data.
#[derive(Debug)]
pub struct CeramicCar {
    signature: Signature,
    return_fields: Fields,
}

impl CeramicCar {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Binary]),
                Volatility::Immutable,
            ),
            return_fields: vec![
                Field::new("event_cid", DataType::Binary, false),
                Field::new(
                    "multi_prev",
                    DataType::List(Field::new_list_field(DataType::Binary, true).into()),
                    false,
                ),
                Field::new("payload", DataType::Binary, false),
            ]
            .into(),
        }
    }
    fn extract(car: &[u8]) -> anyhow::Result<Option<Commit>> {
        let (cid, event) = unvalidated::Event::<Value>::decode_car(car, false)?;
        debug!(?event, "extract");
        match event {
            unvalidated::Event::Time(_time) => Ok(None),
            unvalidated::Event::Signed(signed) => match signed.payload() {
                unvalidated::Payload::Data(data) => Ok(Some(Commit {
                    cid,
                    prevs: (vec![*data.prev()]),
                    payload: (data.data().clone()),
                })),
                unvalidated::Payload::Init(_init) => Ok(None),
            },
            unvalidated::Event::Unsigned(_init) => Ok(None),
        }
    }
}

impl ScalarUDFImpl for CeramicCar {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "ceramic_car"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Struct(self.return_fields.clone()))
    }
    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::common::Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        let cars = as_binary_array(&args[0])?;
        let mut cids = BinaryBuilder::new();
        let mut multi_prevs = ListBuilder::new(BinaryBuilder::new());
        let mut payloads = BinaryBuilder::new();
        let mut nulls = BooleanBufferBuilder::new(cars.len());
        debug!(len = cars.len(), "extracting cars");
        for car in cars {
            if let Some(car) = car {
                if let Some(Commit {
                    cid,
                    prevs,
                    payload,
                }) = CeramicCar::extract(car)
                    .map_err(|err| DataFusionError::Internal(err.to_string()))?
                {
                    cids.append_value(cid.to_bytes());
                    for prev in prevs {
                        multi_prevs.values().append_value(prev.to_bytes())
                    }
                    multi_prevs.append(true);
                    payloads.append_value(
                        serde_json::to_vec(&payload)
                            .map_err(|err| DataFusionError::Internal(err.to_string()))?,
                    );
                } else {
                    cids.append_null();
                    multi_prevs.append(false);
                    payloads.append_null();
                }
                nulls.append(false);
            } else {
                nulls.append(true);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(StructArray::new(
            self.return_fields.clone(),
            vec![
                Arc::new(cids.finish()),
                Arc::new(multi_prevs.finish()),
                Arc::new(payloads.finish()),
            ],
            Some(nulls.finish().into()),
        ))))
    }
}

#[derive(Debug)]
struct Commit {
    cid: Cid,
    prevs: Vec<Cid>,
    payload: Value,
}
