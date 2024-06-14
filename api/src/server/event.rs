use std::collections::HashMap;

use anyhow::{anyhow, bail, Context, Result};
use ceramic_core::{Cid, EventId, Network};
use ceramic_event::unvalidated;
use ipld_core::ipld::Ipld;
use iroh_car::CarReader;
use tokio::io::AsyncRead;
use tracing::debug;

use crate::EventStore;

// Helper function to construct an event ID from CAR data of an event coming in via the HTTP api.
pub async fn event_id_from_car<R, S>(network: Network, reader: R, store: &S) -> Result<EventId>
where
    R: AsyncRead + Send + Unpin,
    S: EventStore,
{
    let (event_cid, event) = event_from_car(reader, true).await?;
    event_id_for_event(event_cid, event, network, store).await
}

async fn event_from_car<R>(
    reader: R,
    deny_unexpected_fields: bool,
) -> Result<(Cid, unvalidated::Event<Ipld>)>
where
    R: AsyncRead + Send + Unpin,
{
    let mut car = CarReader::new(reader).await?;
    let event_cid = *car
        .header()
        .roots()
        .first()
        .ok_or_else(|| anyhow!("CAR data should have at least one root"))?;

    debug!(%event_cid, "first root cid");

    let mut car_blocks = HashMap::new();
    while let Some((cid, bytes)) = car.next_block().await? {
        car_blocks.insert(cid, bytes);
    }
    let event_bytes = car_blocks
        .get(&event_cid)
        .ok_or_else(|| anyhow!("Event CAR data missing block for root CID"))?;
    let raw_event: unvalidated::RawEvent<Ipld> =
        serde_ipld_dagcbor::from_slice(event_bytes).context("decoding event")?;

    if deny_unexpected_fields {
        // Re-serialize the event and compare the bytes. This indirectly checks that there were no
        // unexpected fields in the event sent by the client.
        let event_bytes_reserialized = serde_ipld_dagcbor::to_vec(&raw_event)?;
        if !event_bytes.eq(&event_bytes_reserialized) {
            bail!(
                "Event bytes do not round-trip. This most likely means the event contains unexpected fields."
            );
        }
    }

    match raw_event {
        unvalidated::RawEvent::Time(event) => {
            let proof_bytes = car_blocks
                .get(&event.proof())
                .ok_or_else(|| anyhow!("Time Event CAR data missing block for proof"))?;
            let proof: unvalidated::Proof =
                serde_ipld_dagcbor::from_slice(proof_bytes).context("decoding proof")?;
            let mut blocks_in_path = Vec::new();
            let block_bytes = car_blocks
                .get(&proof.root())
                .ok_or_else(|| anyhow!("Time Event CAR data missing block for root",))?;
            let mut block: Ipld = serde_ipld_dagcbor::from_slice(block_bytes)?;
            let parts: Vec<_> = event.path().split('/').collect();
            // Add blocks for all parts but the last as it is the prev.
            for index in parts.iter().take(parts.len() - 1) {
                let cid = block
                    .get(*index)?
                    .ok_or_else(|| anyhow!("Time Event path indexes missing data"))?;
                let cid = match cid {
                    Ipld::Link(cid) => cid,
                    _ => bail!("Time Event path does not index to a CID"),
                };
                let block_bytes = car_blocks
                    .get(cid)
                    .ok_or_else(|| anyhow!("Time Event CAR data missing block for path index"))?;
                blocks_in_path.push(block);
                block = serde_ipld_dagcbor::from_slice(block_bytes)?;
            }

            Ok((
                event_cid,
                unvalidated::Event::Time(Box::new(unvalidated::TimeEvent::new(
                    *event,
                    proof,
                    blocks_in_path,
                ))),
            ))
        }
        unvalidated::RawEvent::Signed(envelope) => {
            let payload_cid = envelope
                .link()
                .ok_or_else(|| anyhow!("event should have a link"))?;

            let payload_bytes = car_blocks
                .get(&payload_cid)
                .ok_or_else(|| anyhow!("Signed Event CAR data missing block for payload"))?;
            let payload: unvalidated::Payload<Ipld> =
                serde_ipld_dagcbor::from_slice(payload_bytes).context("decoding payload")?;

            if deny_unexpected_fields {
                // Re-serialize the payload and compare the bytes. This indirectly checks that there
                // were no unexpected fields in the event sent by the client.
                let payload_bytes_reserialized = serde_ipld_dagcbor::to_vec(&payload)?;
                if !payload_bytes.eq(&payload_bytes_reserialized) {
                    bail!("Signed event payload bytes do not round-trip. This most likely means the event contains unexpected fields.");
                }
            }

            Ok((
                event_cid,
                unvalidated::Event::Signed(unvalidated::signed::Event::new(
                    event_cid,
                    envelope,
                    payload_cid,
                    payload,
                )),
            ))
        }
        unvalidated::RawEvent::Unsigned(event) => {
            Ok((event_cid, unvalidated::Event::Unsigned(event)))
        }
    }
}

async fn event_id_for_event<S>(
    event_cid: Cid,
    event: unvalidated::Event<Ipld>,
    network: Network,
    store: &S,
) -> Result<EventId>
where
    S: EventStore,
{
    match event {
        unvalidated::Event::Time(time_event) => {
            let init_payload = get_init_event_payload_from_store(&time_event.id(), store).await?;
            event_id_from_init_payload(event_cid, network, time_event.id(), &init_payload)
        }
        unvalidated::Event::Signed(signed_event) => {
            let payload = signed_event.payload();

            match payload {
                unvalidated::Payload::Init(init_payload) => event_id_from_init_payload(
                    event_cid,
                    network,
                    signed_event.envelope_cid(),
                    init_payload,
                ),
                unvalidated::Payload::Data(payload) => {
                    let init_cid = *payload.id();
                    let init_payload = get_init_event_payload_from_store(&init_cid, store).await?;
                    event_id_from_init_payload(event_cid, network, init_cid, &init_payload)
                }
            }
        }
        unvalidated::Event::Unsigned(payload) => {
            event_id_from_init_payload(event_cid, network, event_cid, &payload)
        }
    }
}

fn event_id_from_init_payload(
    event_cid: Cid,
    network: Network,
    init_cid: Cid,
    init_payload: &unvalidated::init::Payload<Ipld>,
) -> Result<EventId> {
    let controller = init_payload
        .header()
        .controllers()
        .first()
        .ok_or_else(|| anyhow!("init header should contain at least one controller"))?;

    Ok(EventId::new(
        &network,
        init_payload.header().sep(),
        init_payload.header().model(),
        controller,
        &init_cid,
        &event_cid,
    ))
}

async fn get_init_event_payload_from_store(
    init_cid: &Cid,
    store: &impl EventStore,
) -> Result<unvalidated::init::Payload<Ipld>> {
    let init_bytes = store
        .get_block(init_cid)
        .await?
        .ok_or_else(|| anyhow!("cannot find init event block {init_cid}"))?;

    let init_event: unvalidated::RawEvent<Ipld> =
        serde_ipld_dagcbor::from_slice(&init_bytes).context("decoding init event")?;
    match init_event {
        unvalidated::RawEvent::Signed(event) => {
            let link = event
                .link()
                .ok_or_else(|| anyhow!("init event should have a link"))?;

            let payload_bytes = store
                .get_block(&link)
                .await?
                .ok_or_else(|| anyhow!("cannot find init event payload block {link}"))?;
            let payload: unvalidated::Payload<Ipld> =
                serde_ipld_dagcbor::from_slice(&payload_bytes).context("decoding init payload")?;
            if let unvalidated::Payload::Init(payload) = payload {
                Ok(payload)
            } else {
                bail!("init event payload is not well formed")
            }
        }
        unvalidated::RawEvent::Unsigned(event) => Ok(event),
        unvalidated::RawEvent::Time(_) => {
            bail!("init event payload can't be a time event")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    use crate::tests::{
        decode_multibase_str, mock_get_init_event, mock_get_unsigned_init_event,
        MockEventStoreTest, DATA_EVENT_CAR, DATA_EVENT_CAR_UNSIGNED_INIT, SIGNED_INIT_EVENT_CAR,
        TIME_EVENT_CAR, UNSIGNED_INIT_EVENT_CAR,
    };
    use async_trait::async_trait;
    use expect_test::{expect, Expect};
    use mockall::{mock, predicate};
    use tracing_test::traced_test;

    async fn test_event_id_from_car(
        event_data: &str,
        expected_event_id: Expect,
        mock_store: MockEventStoreTest,
    ) {
        let event_id = event_id_from_car(
            Network::Mainnet,
            decode_multibase_str(event_data).as_slice(),
            &mock_store,
        )
        .await;

        expected_event_id.assert_debug_eq(&event_id);
    }

    #[tokio::test]
    #[traced_test]
    async fn event_id_from_car_signed_init_event() {
        let expected = expect![[r#"
            Ok(
                EventId {
                    bytes: "ce010500aa5773c7d75777e1deb6cb4af0e69eebd504d38e01850112208e80607ffcf8d9e2ba60da10f71b3c877a65000c15f5621032f11959d504d38e",
                    network_id: Some(
                        0,
                    ),
                    separator: Some(
                        "aa5773c7d75777e1",
                    ),
                    controller: Some(
                        "deb6cb4af0e69eeb",
                    ),
                    stream_id: Some(
                        "d504d38e",
                    ),
                    cid: Some(
                        "bagcqcerar2aga7747dm6fota3iipogz4q55gkaamcx2weebs6emvtvie2oha",
                    ),
                },
            )
        "#]];
        // Init events do not need to access the store
        test_event_id_from_car(SIGNED_INIT_EVENT_CAR, expected, MockEventStoreTest::new()).await
    }

    #[tokio::test]
    #[traced_test]
    async fn event_id_from_car_unsigned_init_event() {
        let expected = expect![[r#"
            Ok(
                EventId {
                    bytes: "ce010500c703887c2b8374ed63a8eb5b47190f4706aabe66017112200a43060a07ecf21b7d3569c3c67a9e9dabb293e170a2905e1d379fbb06aabe66",
                    network_id: Some(
                        0,
                    ),
                    separator: Some(
                        "c703887c2b8374ed",
                    ),
                    controller: Some(
                        "63a8eb5b47190f47",
                    ),
                    stream_id: Some(
                        "06aabe66",
                    ),
                    cid: Some(
                        "bafyreiakimdaub7m6inx2nljypdhvhu5vozjhylqukif4hjxt65qnkv6my",
                    ),
                },
            )
        "#]];
        test_event_id_from_car(
            // Unsigned init payload event
            UNSIGNED_INIT_EVENT_CAR,
            expected,
            // Init events do not need to access the store
            MockEventStoreTest::new(),
        )
        .await
    }

    #[tokio::test]
    #[traced_test]
    async fn event_id_from_car_data_event() {
        let expected = expect![[r#"
            Ok(
                EventId {
                    bytes: "ce010500aa5773c7d75777e1deb6cb4af0e69eebd504d38e0185011220275d0719794a4d9eec8db4a735fd9032dfd238fa5af210d4aa9b337590882943",
                    network_id: Some(
                        0,
                    ),
                    separator: Some(
                        "aa5773c7d75777e1",
                    ),
                    controller: Some(
                        "deb6cb4af0e69eeb",
                    ),
                    stream_id: Some(
                        "d504d38e",
                    ),
                    cid: Some(
                        "bagcqcerae5oqoglzjjgz53enwsttl7mqglp5eoh2llzbbvfktmzxleeiffbq",
                    ),
                },
            )
        "#]];
        let mut mock_event_store = MockEventStoreTest::new();
        mock_get_init_event(&mut mock_event_store);
        test_event_id_from_car(DATA_EVENT_CAR, expected, mock_event_store).await
    }

    #[tokio::test]
    #[traced_test]
    async fn event_id_from_car_data_event_unsigned_init() {
        let expected = expect![[r#"
            Ok(
                EventId {
                    bytes: "ce010500c703887c2b8374ed63a8eb5b47190f4706aabe6601850112200953f8c9dd5669e2f638b04ba24fb57b6f6006b5fb8b63aeea8b7ed33a071bd3",
                    network_id: Some(
                        0,
                    ),
                    separator: Some(
                        "c703887c2b8374ed",
                    ),
                    controller: Some(
                        "63a8eb5b47190f47",
                    ),
                    stream_id: Some(
                        "06aabe66",
                    ),
                    cid: Some(
                        "bagcqcerabfj7rso5kzu6f5rywbf2et5vpnxwabvv7ofwhlxkrn7ngoqhdpjq",
                    ),
                },
            )
        "#]];
        let mut mock_event_store = MockEventStoreTest::new();
        mock_get_unsigned_init_event(&mut mock_event_store);
        test_event_id_from_car(DATA_EVENT_CAR_UNSIGNED_INIT, expected, mock_event_store).await
    }

    #[tokio::test]
    #[traced_test]
    async fn event_id_from_car_time_event() {
        let expected = expect![[r#"
            Ok(
                EventId {
                    bytes: "ce010500aa5773c7d75777e1deb6cb4af0e69eebd504d38e01711220726aa06fb7874a0437da14b534654a66bb8b24670a0c8d5fe25a8ec8d627dde5",
                    network_id: Some(
                        0,
                    ),
                    separator: Some(
                        "aa5773c7d75777e1",
                    ),
                    controller: Some(
                        "deb6cb4af0e69eeb",
                    ),
                    stream_id: Some(
                        "d504d38e",
                    ),
                    cid: Some(
                        "bafyreidsnkqg7n4hjicdpwquwu2gkstgxofsizykbsgv7ys2r3enmj654u",
                    ),
                },
            )
        "#]];
        let mut mock_event_store = MockEventStoreTest::new();
        mock_get_init_event(&mut mock_event_store);
        test_event_id_from_car(TIME_EVENT_CAR, expected, mock_event_store).await
    }

    #[tokio::test]
    #[traced_test]
    async fn event_id_from_car_data_event_missing() {
        let mut mock_store = MockEventStoreTest::new();
        mock_store
            .expect_get_block()
            // Report that the block does not exist
            .returning(move |_| Ok(None));
        let expected = expect![[r#"
            Err(
                "cannot find init event block bagcqcerar2aga7747dm6fota3iipogz4q55gkaamcx2weebs6emvtvie2oha",
            )
        "#]];
        test_event_id_from_car(DATA_EVENT_CAR, expected, mock_store).await
    }
}
