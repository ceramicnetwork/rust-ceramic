//! Publish Subscribe API

use futures_util::stream::BoxStream;

use crate::{error::Error, Bytes, GossipsubEvent, IpfsDep};

/// Publish a message to a topic
#[tracing::instrument(skip(client, data))]
pub async fn publish<T>(client: T, topic: String, data: Bytes) -> Result<(), Error>
where
    T: IpfsDep,
{
    client.publish(topic, data).await?;
    Ok(())
}
/// Subscribe to a topic returning a stream of messages from that topic
#[tracing::instrument(skip(client))]
pub async fn subscribe<T>(
    client: T,
    topic: String,
) -> Result<BoxStream<'static, anyhow::Result<GossipsubEvent>>, Error>
where
    T: IpfsDep,
{
    client.subscribe(topic).await
}
/// Returns a list of topics, to which we are currently subscribed.
#[tracing::instrument(skip(client))]
pub async fn topics<T>(client: T) -> Result<Vec<String>, Error>
where
    T: IpfsDep,
{
    client.topics().await
}
