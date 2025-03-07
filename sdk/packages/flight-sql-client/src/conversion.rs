use std::io::Cursor;
use std::ops::Deref;

use arrow_array::RecordBatch;
use arrow_ipc::reader::FileReader;
use arrow_ipc::writer::FileWriter;
use arrow_schema::SchemaRef;
use snafu::prelude::*;

use crate::error::{ArrowSnafu, Result};

#[allow(unused)]
pub(crate) fn arrow_buffer_to_record_batch(slice: &[u8]) -> Result<(Vec<RecordBatch>, SchemaRef)> {
    let mut batches: Vec<RecordBatch> = Vec::new();
    let file_reader = FileReader::try_new(Cursor::new(slice), None).context(ArrowSnafu {
        message: "failed to convert to record batch",
    })?;
    let schema = file_reader.schema().clone();
    for b in file_reader {
        let record_batch = b.context(ArrowSnafu {
            message: "failed to convert to record batch",
        })?;
        batches.push(record_batch);
    }
    Ok((batches, schema))
}

pub(crate) fn record_batches_to_buffer(batches: Vec<RecordBatch>) -> Result<Vec<u8>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }

    let schema = batches.first().unwrap().schema();
    let mut fr = FileWriter::try_new(Vec::new(), schema.deref()).context(ArrowSnafu {
        message: "failed to convert to buffer",
    })?;
    for batch in batches.iter() {
        fr.write(batch).context(ArrowSnafu {
            message: "failed to convert to buffer",
        })?
    }
    fr.finish().context(ArrowSnafu {
        message: "failed to convert to buffer",
    })?;
    fr.into_inner().context(ArrowSnafu {
        message: "failed to convert to buffer",
    })
}

pub(crate) fn record_batch_to_buffer(batch: RecordBatch) -> Result<Vec<u8>> {
    let schema = batch.schema();
    let mut fr = FileWriter::try_new(Vec::new(), schema.deref()).context(ArrowSnafu {
        message: "failed to convert to buffer",
    })?;
    fr.write(&batch).context(ArrowSnafu {
        message: "failed to convert to buffer",
    })?;
    fr.finish().context(ArrowSnafu {
        message: "failed to convert to buffer",
    })?;
    fr.into_inner().context(ArrowSnafu {
        message: "failed to convert to buffer",
    })
}
