use arrow_flight::{FlightClient, FlightDescriptor, FlightInfo, Ticket};
use async_stream::try_stream;
use bytes::Bytes;
use ceramic_event::unvalidated;
use cid::Cid;
use datafusion::arrow::array::{Array as _, AsArray as _, BinaryArray, ListArray};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{Field, FieldRef, SchemaRef};
use datafusion::arrow::{array::ArrayRef, datatypes::DataType};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::SessionState;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::{error::Result, physical_plan::Accumulator};
use datafusion::{logical_expr::Volatility, prelude::*, scalar::ScalarValue};
use futures::{StreamExt as _, TryStreamExt as _};
use json_patch::PatchOperation;
use serde_json::Value;
use std::any::Any;
use std::sync::Arc;
use tonic::async_trait;
use tonic::transport::Channel;
use tracing::{debug, warn};

// create local session context with an in-memory table
async fn create_context() -> Result<SessionContext> {
    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let ctx = SessionContext::new();

    // Configure listing options
    let file_format = ParquetFormat::default().with_enable_pruning(true);
    let listing_options =
        ListingOptions::new(Arc::new(file_format)).with_file_extension(".parquet");

    ctx.register_listing_table(
        "my_table",
        "/home/nathanielc/projects/scratch/gitcoin/pdb/",
        listing_options,
        None,
        None,
    )
    .await
    .unwrap();
    Ok(ctx)
}

/// A UDAF has state across multiple rows, and thus we require a `struct` with that state.
#[derive(Debug)]
struct Ceramic {
    commits: Vec<Commit>,
}

impl Ceramic {
    // how the struct is initialized
    pub fn new() -> Self {
        Ceramic {
            commits: Default::default(),
        }
    }
    async fn extract(car: &[u8]) -> Result<Commit> {
        let (cid, event) = unvalidated::Event::<Value>::decode_car(car, false)
            .await
            .unwrap();
        debug!(?event, "extract");
        match event {
            unvalidated::Event::Time(time) => Ok(Commit {
                cid,
                prev: Some(time.prev()),
                patch: None,
            }),
            unvalidated::Event::Signed(signed) => match signed.payload() {
                unvalidated::Payload::Data(data) => Ok(Commit {
                    cid,
                    prev: Some(*data.prev()),
                    patch: Some(data.data().clone()),
                }),
                unvalidated::Payload::Init(init) => Ok(Commit {
                    cid,
                    prev: None,
                    patch: init.data().cloned(),
                }),
            },
            unvalidated::Event::Unsigned(init) => Ok(Commit {
                cid,
                prev: None,
                patch: init.data().cloned(),
            }),
        }
    }
    fn list_from_arr(field_ref: &FieldRef, arr: ArrayRef) -> Arc<ListArray> {
        let offsets = OffsetBuffer::from_lengths([arr.len()]);

        Arc::new(ListArray::new(field_ref.clone(), offsets, arr, None))
    }
}

// UDAFs are built using the trait `Accumulator`, that offers DataFusion the necessary functions
// to use them.
impl Accumulator for Ceramic {
    // This function serializes our state to `ScalarValue`, which DataFusion uses
    // to pass this state between execution stages.
    // Note that this can be arbitrary data.
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let cids = Arc::new(BinaryArray::from_iter_values(
            self.commits.iter().map(|commit| commit.cid.to_bytes()),
        )) as ArrayRef;
        let prevs = Arc::new(BinaryArray::from_iter(
            self.commits
                .iter()
                .map(|commit| commit.prev.map(|prev| prev.to_bytes())),
        )) as ArrayRef;
        let patches = Arc::new(BinaryArray::from_iter_values(
            self.commits
                .iter()
                .map(|commit| serde_json::to_vec(&commit.patch).unwrap()),
        )) as ArrayRef;
        debug!(
            cids = cids.len(),
            prevs = prevs.len(),
            patches = patches.len(),
            "state"
        );

        Ok(vec![
            ScalarValue::List(Self::list_from_arr(
                &Arc::new(Field::new("cid", DataType::Binary, false)),
                cids,
            )),
            ScalarValue::List(Self::list_from_arr(
                &Arc::new(Field::new("prev", DataType::Binary, true)),
                prevs,
            )),
            ScalarValue::List(Self::list_from_arr(
                &Arc::new(Field::new("patch", DataType::Binary, true)),
                patches,
            )),
        ])
    }

    // Rollup commits into materialized state
    fn evaluate(&mut self) -> Result<ScalarValue> {
        debug!("evaluate");
        if self.commits.is_empty() {
            return Ok(ScalarValue::Utf8(None));
        }
        // Naively get a linear log from the set of commits
        // This is inaccurate and incomplete, a real world example would ensure that we get events
        // in order to begin with and then handle the branches accordingly.
        // The algo here is naive and will find _a_ linear log.

        let mut curr = if let Some(curr) = self.commits.iter().find(|commit| commit.prev.is_none())
        {
            curr
        } else {
            warn!("stream without an init event");
            return Ok(ScalarValue::Utf8(None));
        };

        let mut log: Vec<&Commit> = Vec::with_capacity(self.commits.len());
        log.push(curr);
        loop {
            curr = if let Some(commit) = self.commits.iter().find(|commit| {
                if let Some(prev) = commit.prev {
                    prev == curr.cid
                } else {
                    false
                }
            }) {
                commit
            } else {
                // No commit builds off the current log we are done
                break;
            };
            log.push(curr);
        }

        // Apply the log
        let mut doc = if let Some(doc) = log[0].patch.clone() {
            doc
        } else {
            warn!("stream init event does not have data");
            return Ok(ScalarValue::Utf8(None));
        };
        for commit in &log[1..] {
            debug!(patch=?commit.patch,"patch");
            if let Some(patch) = &commit.patch {
                let patch: Vec<PatchOperation> = serde_json::from_value(patch.clone()).unwrap();
                json_patch::patch(&mut doc, &patch).unwrap();
            }
        }
        Ok(ScalarValue::Utf8(Some(
            serde_json::to_string(&doc).unwrap(),
        )))
    }

    // DataFusion calls this function to update the accumulator's state for a batch
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        //Hack the async bits away
        use tokio::runtime::Handle;

        // Create the runtime
        let rt = Handle::current();
        let cars = values[0].as_binary::<i32>().clone();
        let cars_len = cars.len();

        let mut new_commits = std::thread::spawn(move || -> Result<Vec<Commit>> {
            rt.block_on(async {
                let mut commits = Vec::new();
                for car in cars.into_iter().flatten() {
                    commits.push(Ceramic::extract(car).await?);
                }
                Ok(commits)
            })
        })
        .join()
        .unwrap()
        .unwrap();
        debug!(
            cars = cars_len,
            new_commits = new_commits.len(),
            "update_batch"
        );
        self.commits.append(&mut new_commits);
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        debug!("merge_batch");
        //TODO iterate over lists?
        let cids = states[0].as_list::<i32>().value(0);
        let cids = cids.as_binary::<i32>();
        let prevs = states[1].as_list::<i32>().value(0);
        let prevs = prevs.as_binary::<i32>();
        let patches = states[2].as_list::<i32>().value(0);
        let patches = patches.as_binary::<i32>();
        debug!(
            cids = cids.len(),
            prevs = prevs.len(),
            patches = patches.len(),
            "states"
        );
        for i in 0..cids.len() {
            let cid = Cid::try_from(cids.value(i)).unwrap();
            let prev = if prevs.is_null(i) {
                None
            } else {
                Some(Cid::try_from(prevs.value(i)).unwrap())
            };
            let patch = if patches.is_null(i) {
                None
            } else {
                serde_json::from_slice(patches.value(i)).unwrap()
            };
            self.commits.push(Commit { cid, prev, patch });
        }
        Ok(())
    }

    fn size(&self) -> usize {
        //TODO account for all commits
        std::mem::size_of_val(self)
    }
}

#[derive(Debug)]
struct Commit {
    cid: Cid,
    prev: Option<Cid>,
    patch: Option<Value>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let ctx = create_context().await?;

    let channel = Channel::from_static("http://localhost:5102")
        .connect()
        .await
        .expect("error connecting");

    let mut client = FlightClient::new(channel);

    // Send 'Hi' bytes as the handshake request to the server
    let response = client
        .handshake(Bytes::from("Hi"))
        .await
        .expect("error handshaking");

    // Expect the server responded with 'Ho'
    assert_eq!(response, Bytes::from("Ho"));
    let flight_info = client
        .get_flight_info(FlightDescriptor {
            r#type: 2,
            cmd: Bytes::from("dimensions.foo='y'"),
            path: vec![],
        })
        .await
        .expect("error getting flight info");

    let table = FlightTable::try_new(flight_info)?;
    ctx.register_table("conclusion_event", Arc::new(table))?;

    // here is where we define the UDAF. We also declare its signature:
    let ceramic = create_udaf(
        "ceramic",
        vec![DataType::Binary],
        Arc::new(DataType::Utf8),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::new(Ceramic::new()))),
        Arc::new(vec![
            DataType::List(Arc::new(Field::new("cid", DataType::Binary, false))),
            DataType::List(Arc::new(Field::new("prev", DataType::Binary, true))),
            DataType::List(Arc::new(Field::new("patch", DataType::Binary, true))),
        ]),
    );
    ctx.register_udaf(ceramic.clone());

    let sql_df = ctx
        .sql("SELECT stream_id, ceramic(car) FROM conclusion_event GROUP BY stream_id")
        .await?;
    sql_df.show().await?;

    Ok(())
}

struct FlightTable {
    schema: SchemaRef,
    ticket: Ticket,
}

impl FlightTable {
    fn try_new(mut flight_info: FlightInfo) -> Result<Self> {
        Ok(Self {
            ticket: flight_info.endpoint.pop().unwrap().ticket.unwrap(),
            schema: flight_info.try_decode_schema()?.into(),
        })
    }
}

#[async_trait]
impl TableProvider for FlightTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(StreamingTableExec::try_new(
            self.schema(),
            vec![Arc::new(FlightPartitionStream::new(
                self.schema(),
                self.ticket.clone(),
            ))],
            projection,
            vec![],
            false,
            limit,
        )?))
    }
}

struct FlightPartitionStream {
    schema: SchemaRef,
    ticket: Ticket,
}

impl FlightPartitionStream {
    fn new(schema: SchemaRef, ticket: Ticket) -> Self {
        Self { schema, ticket }
    }
}

impl PartitionStream for FlightPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, ctx: Arc<datafusion::execution::TaskContext>) -> SendableRecordBatchStream {
        let ticket = self.ticket.clone();
        let stream = try_stream! {
            let channel = Channel::from_static("http://localhost:5102")
                .connect()
                .await
                .expect("error connecting");

            let mut client = FlightClient::new(channel);
            let mut data = client.do_get(ticket).await?;
            while let Some(batch) = data.try_next().await? {
                yield batch
            }
        }
        .map_err(|err: anyhow::Error| {
            datafusion::error::DataFusionError::Internal(err.to_string())
        });
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream))
    }
}
