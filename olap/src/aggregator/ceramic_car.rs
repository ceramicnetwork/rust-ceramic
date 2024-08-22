use std::{any::Any, sync::Arc};

use arrow::{
    array::{
        Array as _, BinaryBuilder, BooleanBufferBuilder, ListBuilder, StringBuilder, StructArray,
    },
    datatypes::{DataType, Field, Fields},
};
use ceramic_event::unvalidated;
use cid::Cid;
use datafusion::{
    common::{cast::as_binary_array, exec_datafusion_err},
    logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility},
};
use serde_json::Value;
use tracing::{debug, instrument, Level};

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
                Field::new("stream_cid", DataType::Binary, false),
                Field::new("event_cid", DataType::Binary, false),
                Field::new(
                    "previous",
                    DataType::List(Field::new_list_field(DataType::Binary, false).into()),
                    true,
                ),
                Field::new("data", DataType::Utf8, true),
            ]
            .into(),
        }
    }
    #[instrument(skip(car), ret(level = Level::TRACE))]
    fn extract(car: &[u8]) -> anyhow::Result<Commit> {
        let (cid, event) = unvalidated::Event::<Value>::decode_car(car, false)?;
        match event {
            unvalidated::Event::Time(time) => Ok(Commit {
                stream_cid: *time.id(),
                cid,
                previous: vec![*time.prev()],
                data: None,
            }),
            unvalidated::Event::Signed(signed) => match signed.payload() {
                unvalidated::Payload::Data(data) => Ok(Commit {
                    stream_cid: *data.id(),
                    cid,
                    previous: vec![*data.prev()],
                    data: Some(data.data().clone()),
                }),
                unvalidated::Payload::Init(init) => Ok(Commit {
                    stream_cid: cid,
                    cid,
                    previous: vec![],
                    data: init.data().cloned(),
                }),
            },
            unvalidated::Event::Unsigned(init) => Ok(Commit {
                stream_cid: cid,
                cid,
                previous: vec![],
                data: init.data().cloned(),
            }),
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
        let mut stream_cids = BinaryBuilder::new();
        let mut event_cids = BinaryBuilder::new();
        let mut prevs = ListBuilder::new(BinaryBuilder::new())
            .with_field(Field::new_list_field(DataType::Binary, false));
        let mut datas = StringBuilder::new();
        let mut nulls = BooleanBufferBuilder::new(cars.len());
        debug!(len = cars.len(), "extracting cars");
        for car in cars {
            if let Some(car) = car {
                let Commit {
                    stream_cid,
                    cid,
                    previous,
                    data,
                } = CeramicCar::extract(car)
                    .map_err(|err| exec_datafusion_err!("Error extracting event: {err}"))?;
                stream_cids.append_value(stream_cid.to_bytes());
                event_cids.append_value(cid.to_bytes());
                for prev in &previous {
                    prevs.values().append_value(prev.to_bytes())
                }
                prevs.append(!previous.is_empty());
                if let Some(data) = data {
                    datas.append_value(
                        serde_json::to_string(&data)
                            .map_err(|err| exec_datafusion_err!("Error JSON encoding: {err}"))?,
                    );
                } else {
                    datas.append_null();
                }
                nulls.append(false);
            } else {
                nulls.append(true);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(StructArray::new(
            self.return_fields.clone(),
            vec![
                Arc::new(stream_cids.finish()),
                Arc::new(event_cids.finish()),
                Arc::new(prevs.finish()),
                Arc::new(datas.finish()),
            ],
            Some(nulls.finish().into()),
        ))))
    }
}

#[derive(Debug)]
struct Commit {
    stream_cid: Cid,
    cid: Cid,
    previous: Vec<Cid>,
    data: Option<Value>,
}
