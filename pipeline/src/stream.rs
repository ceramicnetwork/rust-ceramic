use std::{any::Any, str::FromStr as _, sync::Arc, time::Duration};

use arrow_schema::SchemaRef;
use async_stream::stream;
use async_trait::async_trait;
use ceramic_core::StreamIdType;
use cid::Cid;
use datafusion::{
    catalog::{Session, TableProvider},
    common::exec_datafusion_err,
    datasource::TableType,
    error::DataFusionError,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        stream::RecordBatchStreamAdapter, ExecutionMode, ExecutionPlan, Partitioning,
        PlanProperties,
    },
    prelude::Expr,
};
use futures::TryStreamExt as _;

use crate::{
    conclusion_events_to_record_batch, schemas, ConclusionData, ConclusionEvent, ConclusionInit,
};

pub fn stream_tbl() -> StreamTable {
    StreamTable
}

#[derive(Debug)]
pub struct StreamTable;

#[async_trait]
impl TableProvider for StreamTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        //TODO can this be generic?
        crate::schemas::conclusion_events()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(StreamExec {
            properties: PlanProperties::new(
                EquivalenceProperties::new(self.schema()),
                Partitioning::UnknownPartitioning(1),
                ExecutionMode::Unbounded,
            ),
        }))
    }
}

#[derive(Debug)]
pub struct StreamExec {
    properties: PlanProperties,
}

impl ExecutionPlan for StreamExec {
    fn name(&self) -> &str {
        "StreamExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        let stream = stream! {
            for i in 0..10{
                tokio::time::sleep(Duration::from_secs(2)).await;
                let events = vec![
                    ConclusionEvent::Data(ConclusionData {
                        event_cid: Cid::from_str(
                            "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                        )
                        .unwrap(),
                        init: ConclusionInit {
                            stream_cid: Cid::from_str(
                                "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                            )
                            .unwrap(),
                            stream_type: StreamIdType::Model as u8,
                            controller: "did:key:test1".to_string(),
                            dimensions: vec![
                                ("controller".to_string(), b"did:key:test1".to_vec()),
                                ("model".to_string(), b"model".to_vec()),
                            ],
                        },
                        previous: vec![],
                        data: b"123".into(),
                        index: i+10_000,
                    })];
                yield conclusion_events_to_record_batch(&events)
            }
        };

        let stream = stream.map_err(|err: anyhow::Error| exec_datafusion_err!("{err}"));
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schemas::conclusion_events(),
            stream,
        )))
    }
}

impl datafusion::physical_plan::DisplayAs for StreamExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        // TODO add useful information about predicates etc
        write!(f, "StreamExec")
    }
}
