//! Implementation of the FlightSQL server.
use std::net::SocketAddr;

use arrow_flight::flight_service_server::FlightServiceServer;
use ceramic_pipeline::SessionContextRef;
use datafusion::execution::context::SQLOptions;
use datafusion_flight_sql_server::service::FlightSqlService;
use futures::Future;
use tonic::transport::server::Router;
use tonic::transport::Server;
use tracing::info;

/// Start FlightSQL server, blocks until server has shutdown
pub async fn run(
    ctx: SessionContextRef,
    addr: SocketAddr,
    shutdown_signal: impl Future<Output = ()>,
) -> anyhow::Result<()> {
    let srv = new_server(ctx)?;
    run_service(addr, srv, shutdown_signal).await
}

/// Constructs a new server and can be started.
pub fn new_server(ctx: SessionContextRef) -> anyhow::Result<Router> {
    let svc = FlightServiceServer::new(
        FlightSqlService::new(ctx.state()).with_sql_options(
            // Disable all access except read only queries.
            SQLOptions::new()
                .with_allow_dml(false)
                .with_allow_ddl(false)
                .with_allow_statements(false),
        ),
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
