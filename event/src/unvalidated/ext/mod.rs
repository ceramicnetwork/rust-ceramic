use crate::EventBytes;

/// Extension trait to pull fields from payloads
pub trait CeramicExt {
    /// Obtain the model of the payload
    fn model(&self) -> anyhow::Result<&EventBytes>;

    /// Obtain the seperator of the payload
    fn sep(&self) -> anyhow::Result<&str>;
}

impl<D> CeramicExt for crate::unvalidated::payload::InitPayload<D> {
    fn model(&self) -> anyhow::Result<&EventBytes> {
        let value = self
            .header()
            .additional()
            .get("model")
            .ok_or_else(|| anyhow::anyhow!("model not found"))?;
        let bytes = value
            .as_bytes()
            .ok_or_else(|| anyhow::anyhow!("model is not bytes"))?;
        Ok(bytes)
    }
    fn sep(&self) -> anyhow::Result<&str> {
        let value = self
            .header()
            .additional()
            .get("sep")
            .ok_or_else(|| anyhow::anyhow!("sep not found"))?;
        let sep = value
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("sep is not string"))?;
        Ok(sep)
    }
}
