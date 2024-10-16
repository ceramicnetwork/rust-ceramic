//! Implementation of the FlightSQL server.
use std::any::Any;
use std::net::SocketAddr;
use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_stream::try_stream;
use ceramic_pipeline::cid_string::{CidString, CidStringList};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::exec_datafusion_err;
use datafusion::datasource::TableType;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::context::{SQLOptions, SessionContext};
use datafusion::logical_expr::{Expr, ScalarUDF, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{ExecutionMode, ExecutionPlan, PlanProperties};
use datafusion::scalar::ScalarValue;
use datafusion_flight_sql_server::service::FlightSqlService;
use futures::{Future, TryStreamExt};
use tonic::transport::server::Router;
use tonic::transport::Server;
use tracing::{debug, info, instrument, Level};

use crate::{conclusion_events_to_record_batch, ConclusionEvent};

/// Start FlightSQL server, blocks until server has shutdown
pub async fn run(
    feed: Arc<impl ConclusionFeed + std::fmt::Debug + 'static>,
    addr: SocketAddr,
    shutdown_signal: impl Future<Output = ()>,
) -> anyhow::Result<()> {
    let srv = new_server(feed)?;
    run_service(addr, srv, shutdown_signal).await
}

/// Constructs a new server and can be started.
pub fn new_server(
    feed: Arc<impl ConclusionFeed + std::fmt::Debug + 'static>,
) -> anyhow::Result<Router> {
    let ctx = SessionContext::new_with_config(
        SessionConfig::new().with_default_catalog_and_schema("ceramic", "v0"),
    );
    ctx.register_table("conclusion_feed", Arc::new(FeedTable::new(feed)))?;
    ctx.register_udf(ScalarUDF::new_from_impl(CidString::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(CidStringList::new()));
    let svc = FlightServiceServer::new(
        FlightSqlService::new(ctx.state()).with_sql_options(Some(
            // Disable all access except read only queries.
            SQLOptions::new()
                .with_allow_dml(false)
                .with_allow_ddl(false)
                .with_allow_statements(false),
        )),
    );
    Ok(Server::builder().add_service(svc))
}

async fn run_service(
    addr: SocketAddr,
    svr: Router,
    shutdown_signal: impl Future<Output = ()>,
) -> anyhow::Result<()> {
    info!(%addr, "FlightSQL server listening");
    Ok(svr.serve_with_shutdown(addr, shutdown_signal).await?)
}

/// A ConclusionFeed provides access to [`ConclusionEvent`]s.
#[async_trait::async_trait]
pub trait ConclusionFeed: Send + Sync {
    /// Produce a set of conclusion events up to the limit with an index greater than the highwater_mark
    /// Event must be returned in index order.
    async fn conclusion_events_since(
        &self,
        highwater_mark: i64,
        limit: i64,
    ) -> anyhow::Result<Vec<ConclusionEvent>>;
}
#[async_trait::async_trait]
impl<T: ConclusionFeed> ConclusionFeed for Arc<T> {
    async fn conclusion_events_since(
        &self,
        highwater_mark: i64,
        limit: i64,
    ) -> anyhow::Result<Vec<ConclusionEvent>> {
        self.as_ref()
            .conclusion_events_since(highwater_mark, limit)
            .await
    }
}

// Implements the [`TableProvider`] trait producing a [`FeedExec`] instance when the table is
// scanned, which in turn calls into the [`ConclusionFeed`] to get the actual events.
struct FeedTable<T> {
    feed: Arc<T>,
    schema: SchemaRef,
}

impl<T> FeedTable<T> {
    fn new(feed: Arc<T>) -> Self {
        Self {
            feed,
            schema: Arc::new(Schema::new(vec![
                Field::new("index", DataType::UInt64, false),
                Field::new("event_type", DataType::UInt8, false),
                Field::new("stream_cid", DataType::Binary, false),
                Field::new("stream_type", DataType::UInt8, false),
                Field::new("controller", DataType::Utf8, false),
                Field::new(
                    // NOTE: The entire dimensions map may be null or values for a given key may
                    // be null. No other aspect of dimensions may be null.
                    "dimensions",
                    DataType::Map(
                        Field::new(
                            "entries",
                            DataType::Struct(
                                vec![
                                    Field::new("key", DataType::Utf8, false),
                                    Field::new(
                                        "value",
                                        DataType::Dictionary(
                                            Box::new(DataType::Int32),
                                            Box::new(DataType::Binary),
                                        ),
                                        true,
                                    ),
                                ]
                                .into(),
                            ),
                            false,
                        )
                        .into(),
                        false,
                    ),
                    true,
                ),
                Field::new("event_cid", DataType::Binary, false),
                Field::new("data", DataType::Binary, true),
                Field::new(
                    "previous",
                    DataType::List(Arc::new(Field::new("item", DataType::Binary, false))),
                    true,
                ),
            ])),
        }
    }
    fn highwater_mark_from_expr(expr: &Expr) -> Option<u64> {
        let find_highwater_mark = |col: &Expr, lit: &Expr| {
            col.try_as_col()
                .map_or(false, |column| column.name == "index")
                .then(|| {
                    if let Expr::Literal(ScalarValue::UInt64(highwater_mark)) = lit {
                        highwater_mark.to_owned()
                    } else {
                        None
                    }
                })
                .flatten()
        };
        match expr {
            Expr::BinaryExpr(expr) => match expr.op {
                datafusion::logical_expr::Operator::Gt => {
                    find_highwater_mark(expr.left.as_ref(), expr.right.as_ref())
                }
                datafusion::logical_expr::Operator::LtEq => {
                    find_highwater_mark(expr.right.as_ref(), expr.left.as_ref())
                }
                _ => None,
            },
            _ => None,
        }
    }
}
#[async_trait::async_trait]
impl<T: ConclusionFeed + std::fmt::Debug + 'static> TableProvider for FeedTable<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }
    #[instrument(skip(self,_state), ret(level = Level::DEBUG))]
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let schema = projection
            .map(|projection| self.schema.project(projection))
            .transpose()?
            .map(Arc::new)
            .unwrap_or_else(|| self.schema.clone());
        debug!(?schema, "projected schema");
        Ok(Arc::new(FeedExec {
            feed: self.feed.clone(),
            schema: schema.clone(),
            projection: projection.cloned(),
            properties: PlanProperties::new(
                EquivalenceProperties::new(schema),
                datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
                ExecutionMode::Bounded,
            ),
            highwater_mark: filters
                .iter()
                .filter_map(Self::highwater_mark_from_expr)
                .next()
                .map(|hm| hm as i64)
                .unwrap_or(0),
            limit: limit.map(|l| l as i64),
        }))
    }
    #[instrument(skip(self), ret(level = Level::DEBUG))]
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|expr| Self::highwater_mark_from_expr(expr))
            .map(|highwater_mark| {
                if highwater_mark.is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
}

// Implements the [`ExecutionPlan`] trait in terms of a [`ConclusionFeed`].
// This allows calls to scan the `conclusion_feed` table to be mapped to calls into the
// [`ConclusionFeed`].
#[derive(Debug)]
struct FeedExec<T> {
    feed: Arc<T>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    properties: PlanProperties,
    highwater_mark: i64,
    limit: Option<i64>,
}

impl<T> datafusion::physical_plan::DisplayAs for FeedExec<T> {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        // TODO add useful information about predicates etc
        write!(f, "FeedExec")
    }
}

impl<T: ConclusionFeed + std::fmt::Debug + 'static> ExecutionPlan for FeedExec<T> {
    fn name(&self) -> &str {
        "FeedExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
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
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        // Set a reasonable default limit
        const DEFAULT_LIMIT: i64 = 10_000;
        let feed = self.feed.clone();
        let projection = self.projection.clone();
        let highwater_mark = self.highwater_mark;
        let limit = self.limit.unwrap_or(DEFAULT_LIMIT);
        let stream = try_stream! {
            let events = feed.conclusion_events_since(highwater_mark,limit).await?;
            let batch = conclusion_events_to_record_batch(&events)?;
            let batch = projection
                .map(|projection| batch.project(&projection))
                .transpose()?
                .unwrap_or_else(|| batch);
            yield batch;
        };
        let stream = stream.map_err(|err: anyhow::Error| exec_datafusion_err!("{err}"));
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}
