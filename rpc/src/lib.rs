use std::future::ready;
use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, RecordBatch, UInt32Array};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::FlightEndpoint;
use arrow_schema::{DataType, Field, SchemaBuilder};
use async_stream::try_stream;
use bytes::Bytes;
use ceramic_core::EventId;
use futures::stream::{once, BoxStream};
use futures::{StreamExt as _, TryStreamExt};
use recon::{AssociativeHash as _, Sha256a};
use tonic::transport::Server;
use tonic::{async_trait, Request, Response, Status, Streaming};
use tracing::debug;

use arrow_flight::{
    flight_service_server::FlightService, flight_service_server::FlightServiceServer, Action,
    ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};

/// Trait for accessing persistent storage of Events
#[async_trait]
pub trait EventStore: Send + Sync {
    async fn events_since_highwater_mark(
        &self,
        highwater: i64,
        limit: i64,
    ) -> anyhow::Result<(i64, Vec<(EventId, Vec<u8>)>)>;
}
#[async_trait::async_trait]
impl<S: EventStore> EventStore for Arc<S> {
    async fn events_since_highwater_mark(
        &self,
        highwater: i64,
        limit: i64,
    ) -> anyhow::Result<(i64, Vec<(EventId, Vec<u8>)>)> {
        self.as_ref()
            .events_since_highwater_mark(highwater, limit)
            .await
    }
}

#[derive(Clone)]
pub struct FlightServiceImpl<S> {
    event_store: S,
}

impl<S: EventStore> FlightServiceImpl<S> {
    pub fn new(event_store: S) -> Self {
        Self { event_store }
    }
}

#[tonic::async_trait]
impl<S: EventStore + 'static> FlightService for FlightServiceImpl<S> {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Ok(Response::new(
            once(ready(Ok(HandshakeResponse {
                protocol_version: 1,
                payload: Bytes::from("Ho"),
            })))
            .boxed(),
        ))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Implement list_flights"))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let mut builder = SchemaBuilder::new();
        builder.push(Field::new("order_key", DataType::Binary, false));
        builder.push(Field::new("ahash_0", DataType::UInt32, false));
        builder.push(Field::new("ahash_1", DataType::UInt32, false));
        builder.push(Field::new("ahash_2", DataType::UInt32, false));
        builder.push(Field::new("ahash_3", DataType::UInt32, false));
        builder.push(Field::new("ahash_4", DataType::UInt32, false));
        builder.push(Field::new("ahash_5", DataType::UInt32, false));
        builder.push(Field::new("ahash_6", DataType::UInt32, false));
        builder.push(Field::new("ahash_7", DataType::UInt32, false));
        builder.push(Field::new("stream_id", DataType::Binary, false));
        builder.push(Field::new("cid", DataType::Binary, false));
        builder.push(Field::new("car", DataType::Binary, false));
        Ok(Response::new(
            FlightInfo::new()
                .try_with_schema(&builder.finish())
                .map_err(|err| Status::internal(err.to_string()))?
                .with_endpoint(
                    FlightEndpoint::new().with_ticket(Ticket::new(request.get_ref().cmd.clone())),
                )
                .with_descriptor(request.into_inner()),
        ))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Implement poll_flight_info"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Implement get_schema"))
    }

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        // TODO Loop over with some kind of pagination
        let (_new_highwater_mark, events) = self
            .event_store
            .events_since_highwater_mark(0, 1000)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        debug!(?events);
        let batches = try_stream! {
            let order_keys = BinaryArray::from_iter_values(
                events.iter().map(|(event_id, _car)| event_id.as_slice()),
            );
            let hashes: Vec<_> = events
                .iter()
                .map(|(event_id, _car)| Sha256a::digest(event_id))
                .collect();

            let ahash_0 = UInt32Array::from_iter_values(hashes.iter().map(|hash| hash.as_u32s()[0]));
            let ahash_1 = UInt32Array::from_iter_values(hashes.iter().map(|hash| hash.as_u32s()[1]));
            let ahash_2 = UInt32Array::from_iter_values(hashes.iter().map(|hash| hash.as_u32s()[2]));
            let ahash_3 = UInt32Array::from_iter_values(hashes.iter().map(|hash| hash.as_u32s()[3]));
            let ahash_4 = UInt32Array::from_iter_values(hashes.iter().map(|hash| hash.as_u32s()[4]));
            let ahash_5 = UInt32Array::from_iter_values(hashes.iter().map(|hash| hash.as_u32s()[5]));
            let ahash_6 = UInt32Array::from_iter_values(hashes.iter().map(|hash| hash.as_u32s()[6]));
            let ahash_7 = UInt32Array::from_iter_values(hashes.iter().map(|hash| hash.as_u32s()[7]));
            let stream_ids = BinaryArray::from_iter_values(
                events
                    .iter()
                    .map(|(event_id, _car)| event_id.stream_id().unwrap()),
            );
            let cids = BinaryArray::from_iter_values(
                events
                    .iter()
                    .map(|(event_id, _car)| event_id.cid().unwrap().to_bytes()),
            );
            let cars =
                BinaryArray::from_iter_values(events.iter().map(|(_event_id, car)| car.as_slice()));

            debug!("yielding a batch");
            yield dbg!(RecordBatch::try_from_iter(vec![
                ("order_key", Arc::new(order_keys) as ArrayRef),
                ("ahash_0", Arc::new(ahash_0) as ArrayRef),
                ("ahash_1", Arc::new(ahash_1) as ArrayRef),
                ("ahash_2", Arc::new(ahash_2) as ArrayRef),
                ("ahash_3", Arc::new(ahash_3) as ArrayRef),
                ("ahash_4", Arc::new(ahash_4) as ArrayRef),
                ("ahash_5", Arc::new(ahash_5) as ArrayRef),
                ("ahash_6", Arc::new(ahash_6) as ArrayRef),
                ("ahash_7", Arc::new(ahash_7) as ArrayRef),
                ("stream_id", Arc::new(stream_ids) as ArrayRef),
                ("cid", Arc::new(cids) as ArrayRef),
                ("car", Arc::new(cars) as ArrayRef),
            ]))?;
            debug!("batches done");
        };

        let flight_data_stream = FlightDataEncoderBuilder::new().build(batches);

        Ok(Response::new(
            flight_data_stream
                .map_err(|err| Status::internal(err.to_string()))
                .boxed(),
        ))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Implement do_put"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Implement do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Implement list_actions"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Implement do_exchange"))
    }
}

pub async fn run<S: EventStore + 'static>(
    event_store: S,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:5102".parse()?;
    let service = FlightServiceImpl::new(event_store);

    let svc = FlightServiceServer::new(service);

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
