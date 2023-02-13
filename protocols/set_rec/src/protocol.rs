use std::{collections::BTreeSet, io};

use libp2p::futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::{info, instrument};

pub const PROTOCOL_NAME: &[u8] = b"/ceramic/set_rec/0.1.0";
const MAX_SET_SIZE: usize = 256;

#[instrument(skip_all)]
pub async fn send_set<S>(mut stream: S, mut set: BTreeSet<u8>) -> io::Result<(S, BTreeSet<u8>)>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    info!("send_set");
    debug_assert!(set.len() <= MAX_SET_SIZE);
    let data: Vec<u8> = set.iter().map(|i| *i).collect();
    stream.write_all(&data).await?;
    stream.flush().await?;
    info!("send_set sent");

    let mut recv_set = [0u8; MAX_SET_SIZE];
    stream.read(&mut recv_set).await?;

    set.extend(recv_set.iter());

    info!(
        "send_set recvd {:?}",
        recv_set.clone().into_iter().collect::<BTreeSet<u8>>()
    );
    Ok((stream, set))
}

#[instrument(skip_all)]
pub async fn recv_set<S>(mut stream: S, mut set: BTreeSet<u8>) -> io::Result<(S, BTreeSet<u8>)>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    info!("recv_set");
    let mut recv_set = [0u8; MAX_SET_SIZE];
    stream.read(&mut recv_set).await?;
    info!(
        "recv_set recvd {:?}",
        recv_set.clone().into_iter().collect::<BTreeSet<u8>>()
    );

    debug_assert!(set.len() <= MAX_SET_SIZE);
    let data: Vec<u8> = set.iter().map(|i| *i).collect();
    stream.write_all(&data).await?;
    stream.flush().await?;
    info!("recv_set sent");

    set.extend(recv_set.iter());

    Ok((stream, set))
}
