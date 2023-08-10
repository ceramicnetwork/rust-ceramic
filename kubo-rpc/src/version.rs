use crate::{error::Error, IpfsDep};

/// Lookup version.
#[tracing::instrument(skip(client))]
pub async fn version<T>(client: T) -> Result<ceramic_metadata::Version, Error>
where
    T: IpfsDep,
{
    client.version().await
}
