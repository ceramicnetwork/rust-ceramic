use std::vec;

use libp2p::core::UpgradeInfo;
use libp2p::{futures::future, InboundUpgrade, OutboundUpgrade};
use void::Void;

/// Implementation of [`UpgradeInfo`], [`InboundUpgrade`] and [`OutboundUpgrade`] for a set of protocols that directly yields the substream.
#[derive(Debug, Clone)]
pub struct MultiReadyUpgrade<P> {
    protocol_names: Vec<P>,
}

impl<P> MultiReadyUpgrade<P> {
    pub fn new(protocol_names: Vec<P>) -> Self {
        Self { protocol_names }
    }
}

impl<P> UpgradeInfo for MultiReadyUpgrade<P>
where
    P: AsRef<str> + Clone,
{
    type Info = P;
    type InfoIter = vec::IntoIter<P>;

    fn protocol_info(&self) -> Self::InfoIter {
        self.protocol_names.clone().into_iter()
    }
}

impl<C, P> InboundUpgrade<C> for MultiReadyUpgrade<P>
where
    P: AsRef<str> + Clone,
{
    /// Report both the negotiated protocol and the stream.
    type Output = (P, C);
    type Error = Void;
    type Future = future::Ready<Result<Self::Output, Self::Error>>;

    fn upgrade_inbound(self, stream: C, info: Self::Info) -> Self::Future {
        future::ready(Ok((info, stream)))
    }
}

impl<C, P> OutboundUpgrade<C> for MultiReadyUpgrade<P>
where
    P: AsRef<str> + Clone,
{
    type Output = C;
    type Error = Void;
    type Future = future::Ready<Result<Self::Output, Self::Error>>;

    fn upgrade_outbound(self, stream: C, _: Self::Info) -> Self::Future {
        future::ready(Ok(stream))
    }
}
