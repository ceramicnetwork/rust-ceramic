use anyhow::anyhow;
use cid::Cid;

use crate::{
    sql::entities::{
        EventHeader, EventHeaderRow, EventType, StreamCid, StreamCommitRow, StreamRow,
    },
    Error, Result, SqlitePool, SqliteTransaction,
};

/// Access to the stream and related tables. Generally querying events as a stream.
pub struct CeramicOneStream {}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Represents a stream event in a way that allows ordering it in the stream.
pub struct StreamCommit {
    /// The event CID
    pub cid: Cid,
    /// The previous event CID
    pub prev: Option<Cid>,
    /// Whether the event has been delivered
    pub delivered: bool,
}

impl TryFrom<StreamCommitRow> for StreamCommit {
    type Error = crate::Error;

    fn try_from(row: StreamCommitRow) -> std::result::Result<Self, Self::Error> {
        let cid = Cid::try_from(row.cid)
            .map_err(|e| Error::new_app(anyhow!("Invalid event cid: {}", e)))?;
        let prev = row
            .prev
            .map(Cid::try_from)
            .transpose()
            .map_err(|e| Error::new_app(anyhow!("Invalid event prev: {}", e)))?;
        Ok(Self {
            cid,
            prev,
            delivered: row.delivered,
        })
    }
}

impl CeramicOneStream {
    /// Load the events (commits) for a given stream
    pub async fn load_stream_commits(
        pool: &SqlitePool,
        stream_cid: StreamCid,
    ) -> Result<Vec<StreamCommit>> {
        let rows: Vec<(Vec<u8>, Option<Vec<u8>>, bool)> =
            sqlx::query_as(StreamCommitRow::fetch_by_stream_cid())
                .bind(stream_cid.to_bytes())
                .fetch_all(pool.reader())
                .await?;

        let res = rows
            .into_iter()
            .map(|(cid, prev, delivered)| {
                let cid = Cid::try_from(cid).expect("cid");
                let prev = prev.map(Cid::try_from).transpose().expect("prev");

                StreamCommit {
                    cid,
                    prev,
                    delivered,
                }
            })
            .collect();

        Ok(res)
    }

    pub(crate) async fn insert_tx(
        tx: &mut SqliteTransaction<'_>,
        stream_cid: StreamCid,
        header: &ceramic_event::unvalidated::init::Header,
    ) -> Result<()> {
        let _resp = sqlx::query(StreamRow::insert())
            .bind(stream_cid.to_bytes())
            .bind(header.sep())
            .bind(header.model())
            .fetch_one(&mut **tx.inner())
            .await?;

        Ok(())
    }

    pub(crate) async fn insert_event_header_tx(
        tx: &mut SqliteTransaction<'_>,
        header: &EventHeader,
    ) -> Result<()> {
        let (cid, event_type, stream_cid, prev) = match header {
            EventHeader::Init { cid, .. } => (
                cid.to_bytes(),
                EventType::Init,
                header.stream_cid().to_bytes(),
                None,
            ),
            EventHeader::Data {
                cid,
                stream_cid,
                prev,
            } => (
                cid.to_bytes(),
                EventType::Data,
                stream_cid.to_bytes(),
                Some(prev.to_bytes()),
            ),
            EventHeader::Time {
                cid,
                stream_cid,
                prev,
            } => (
                cid.to_bytes(),
                EventType::Time,
                stream_cid.to_bytes(),
                Some(prev.to_bytes()),
            ),
        };

        let _res = sqlx::query(EventHeaderRow::insert())
            .bind(cid)
            .bind(stream_cid)
            .bind(event_type)
            .bind(prev)
            .execute(&mut **tx.inner())
            .await?;

        Ok(())
    }
}
