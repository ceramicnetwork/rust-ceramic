use anyhow::anyhow;
use cid::Cid;

use crate::{
    sql::entities::{
        EventHeader, EventMetadataRow, EventType, IncompleteStream, StreamCid, StreamEventRow,
        StreamRow,
    },
    Error, Result, SqlitePool, SqliteTransaction,
};

/// Access to the stream and related tables. Generally querying events as a stream.
pub struct CeramicOneStream {}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Represents a stream event in a way that allows ordering it in the stream. It is metadata and not the event payload itself.
pub struct StreamEventMetadata {
    /// The event CID
    pub cid: Cid,
    /// The previous event CID
    pub prev: Option<Cid>,
    /// Whether the event is deliverable
    pub deliverable: bool,
}

impl TryFrom<StreamEventRow> for StreamEventMetadata {
    type Error = crate::Error;

    fn try_from(row: StreamEventRow) -> std::result::Result<Self, Self::Error> {
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
            deliverable: row.deliverable,
        })
    }
}

impl CeramicOneStream {
    /// Load the events for a given stream. Will return nothing if the stream does not exist (i.e. the init event is undiscovered).
    pub async fn load_stream_events(
        pool: &SqlitePool,
        stream_cid: StreamCid,
    ) -> Result<Vec<StreamEventMetadata>> {
        let rows: Vec<(Vec<u8>, Option<Vec<u8>>, bool)> =
            sqlx::query_as(StreamEventRow::fetch_by_stream_cid())
                .bind(stream_cid.to_bytes())
                .fetch_all(pool.reader())
                .await?;

        let res = rows
            .into_iter()
            .map(|(cid, prev, delivered)| {
                let cid = Cid::try_from(cid).expect("cid");
                let prev = prev.map(Cid::try_from).transpose().expect("prev");

                StreamEventMetadata {
                    cid,
                    prev,
                    deliverable: delivered,
                }
            })
            .collect();

        Ok(res)
    }

    /// Load streams with undelivered events to see if they need to be delivered now.
    /// cid_cursor is the last CID processed that you want to start after.
    /// Start with `Cid::default()` to start at the beginning.
    pub async fn load_stream_cids_with_undelivered_events(
        pool: &SqlitePool,
        cid_cursor: StreamCid,
    ) -> Result<Vec<StreamCid>> {
        let streams: Vec<IncompleteStream> =
            sqlx::query_as(IncompleteStream::fetch_all_with_undelivered())
                .bind(cid_cursor.to_bytes())
                .bind(1000)
                .fetch_all(pool.reader())
                .await?;

        Ok(streams.into_iter().map(|s| s.stream_cid).collect())
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

        let _res = sqlx::query(EventMetadataRow::insert())
            .bind(cid)
            .bind(stream_cid)
            .bind(event_type)
            .bind(prev)
            .execute(&mut **tx.inner())
            .await?;

        Ok(())
    }
}
