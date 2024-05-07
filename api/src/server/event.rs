use std::collections::HashMap;

use anyhow::{anyhow, bail, Context, Result};
use ceramic_core::{EventId, Network};
use ceramic_event::unvalidated;
use cid::Cid;
use ipld_core::ipld::Ipld;
use iroh_car::CarReader;
use tokio::io::AsyncRead;
use tracing::debug;

use crate::AccessModelStore;

// Helper function to construct an event Id from CAR data of an event
// We should likely move this closer to where its needed but this is good enough for now.
// TODO remove this allow once its used as part of the API
#[allow(dead_code)]
pub async fn event_id_from_car<R, S>(network: Network, reader: R, store: &S) -> Result<EventId>
where
    R: AsyncRead + Send + Unpin,
    S: AccessModelStore,
{
    let mut car = CarReader::new(reader).await?;
    let event_cid = *car
        .header()
        .roots()
        .first()
        .ok_or_else(|| anyhow!("car data should have at least one root"))?;

    debug!(%event_cid, "first root cid");

    let mut car_blocks = HashMap::new();
    while let Some((cid, bytes)) = car.next_block().await? {
        car_blocks.insert(cid, bytes);
    }
    let event_bytes = get_block(&event_cid, &car_blocks, store).await?;
    let event: unvalidated::Event<Ipld> =
        serde_ipld_dagcbor::from_slice(&event_bytes).context("decoding event")?;
    let (init_id, payload) = match event {
        unvalidated::Event::Time(event) => (
            event.id(),
            get_init_event_payload(&event.id(), &car_blocks, store).await?,
        ),
        unvalidated::Event::Signed(event) => {
            let link = event
                .link()
                .ok_or_else(|| anyhow!("event should have a link"))?;

            let payload_bytes = get_block(&link, &car_blocks, store).await?;
            let payload: unvalidated::Payload<Ipld> =
                serde_ipld_dagcbor::from_slice(&payload_bytes).context("decoding payload")?;
            let init_id = match payload {
                unvalidated::Payload::Init(_) => event_cid,
                unvalidated::Payload::Data(payload) => payload.id(),
            };
            (
                init_id,
                get_init_event_payload(&init_id, &car_blocks, store).await?,
            )
        }
        unvalidated::Event::Unsigned(event) => (event_cid, event),
    };

    Ok(EventId::new(
        &network,
        payload.header().sep(),
        payload.header().model().as_slice(),
        payload
            .header()
            .controllers()
            .first()
            .ok_or_else(|| anyhow!("init header should contain at least one controller"))?,
        &init_id,
        &event_cid,
    ))
}

async fn get_init_event_payload(
    init_id: &Cid,
    car_blocks: &HashMap<Cid, Vec<u8>>,
    store: &impl AccessModelStore,
) -> Result<unvalidated::InitPayload<Ipld>> {
    let init_bytes = get_block(init_id, car_blocks, store).await?;
    let init_event: unvalidated::Event<Ipld> =
        serde_ipld_dagcbor::from_slice(&init_bytes).context("decoding init event")?;
    if let unvalidated::Event::Signed(event) = init_event {
        let link = event
            .link()
            .ok_or_else(|| anyhow!("init event should have a link"))?;

        let payload_bytes = get_block(&link, car_blocks, store).await?;
        let payload: unvalidated::Payload<Ipld> =
            serde_ipld_dagcbor::from_slice(&payload_bytes).context("decoding init payload")?;
        if let unvalidated::Payload::Init(payload) = payload {
            Ok(payload)
        } else {
            bail!("init event payload is not well formed")
        }
    } else {
        bail!("init event should be a signed event")
    }
}

// Helper function to get the block first from the CAR file data or otherwise from the store.
async fn get_block<S: AccessModelStore>(
    cid: &Cid,
    car_blocks: &HashMap<Cid, Vec<u8>>,
    store: &S,
) -> Result<Vec<u8>> {
    if let Some(bytes) = car_blocks.get(cid) {
        Ok(bytes.clone())
    } else {
        Ok(store
            .get_block(cid)
            .await?
            .ok_or_else(|| anyhow!("cannot find block {cid}"))?)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    use async_trait::async_trait;
    use expect_test::{expect, Expect};
    use mockall::{mock, predicate};
    use tracing_test::traced_test;

    const INIT_EVENT_CID: &str = "bagcqcerar2aga7747dm6fota3iipogz4q55gkaamcx2weebs6emvtvie2oha";
    const INIT_EVENT_PAYLOAD_CID: &str =
        "bafyreiaroclcgqih242byss6pneufencrulmeex2ttfdzefst67agwq3im";
    const INIT_EVENT_CAR: &str = "
        uO6Jlcm9vdHOB2CpYJgABhQESII6AYH_8-NniumDaEPcbPId6ZQAMFfViEDLxGVnVBNOOZ3ZlcnNpb24
        B0QEBcRIgEXCWI0EH1zQcSl57SUKRoo0WwhL6nMo8kLKfvgNaG0OiZGRhdGGhZXN0ZXBoGQFNZmhlYWR
        lcqRjc2VwZW1vZGVsZW1vZGVsWCjOAQIBhQESIKDoMqM144vTQLQ6DwKZvzxRWg_DPeTNeRCkPouTHo1
        YZnVuaXF1ZUxEpvE6skELu2qFaN5rY29udHJvbGxlcnOBeDhkaWQ6a2V5Ono2TWt0QnluQVBMckV5ZVM
        3cFZ0aGJpeVNjbWZ1OG41Vjdib1hneHlvNXEzU1pSUroCAYUBEiCOgGB__PjZ4rpg2hD3GzyHemUADBX
        1YhAy8RlZ1QTTjqJncGF5bG9hZFgkAXESIBFwliNBB9c0HEpee0lCkaKNFsIS-pzKPJCyn74DWhtDanN
        pZ25hdHVyZXOBomlwcm90ZWN0ZWRYgXsiYWxnIjoiRWREU0EiLCJraWQiOiJkaWQ6a2V5Ono2TWt0Qnl
        uQVBMckV5ZVM3cFZ0aGJpeVNjbWZ1OG41Vjdib1hneHlvNXEzU1pSUiN6Nk1rdEJ5bkFQTHJFeWVTN3B
        WdGhiaXlTY21mdThuNVY3Ym9YZ3h5bzVxM1NaUlIifWlzaWduYXR1cmVYQCQDjlx8fT8rbTR4088HtOE
        27LJMc38DSuf1_XtK14hDp1Q6vhHqnuiobqp5EqNOp0vNFCCzwgG-Dsjmes9jJww";
    const INIT_EVENT: &str = "
        uomdwYXlsb2FkWCQBcRIgEXCWI0EH1zQcSl57SUKRoo0WwhL6nMo8kLKfvgNaG0Nqc2lnbmF0dXJlc4G
        iaXByb3RlY3RlZFiBeyJhbGciOiJFZERTQSIsImtpZCI6ImRpZDprZXk6ejZNa3RCeW5BUExyRXllUzd
        wVnRoYml5U2NtZnU4bjVWN2JvWGd4eW81cTNTWlJSI3o2TWt0QnluQVBMckV5ZVM3cFZ0aGJpeVNjbWZ
        1OG41Vjdib1hneHlvNXEzU1pSUiJ9aXNpZ25hdHVyZVhAJAOOXHx9PyttNHjTzwe04TbsskxzfwNK5_X
        9e0rXiEOnVDq-Eeqe6KhuqnkSo06nS80UILPCAb4OyOZ6z2MnDA";
    const INIT_EVENT_PAYLOAD: &str = "
        uomRkYXRhoWVzdGVwaBkBTWZoZWFkZXKkY3NlcGVtb2RlbGVtb2RlbFgozgECAYUBEiCg6DKjNeOL00C
        0Og8Cmb88UVoPwz3kzXkQpD6Lkx6NWGZ1bmlxdWVMRKbxOrJBC7tqhWjea2NvbnRyb2xsZXJzgXg4ZGl
        kOmtleTp6Nk1rdEJ5bkFQTHJFeWVTN3BWdGhiaXlTY21mdThuNVY3Ym9YZ3h5bzVxM1NaUlI";

    const DATA_EVENT_CAR: &str = "
        uO6Jlcm9vdHOB2CpYJgABhQESICddBxl5Sk2e7I20pzX9kDLf0jj6WvIQ1KqbM3WQiClDZ3ZlcnNpb24
        BqAEBcRIgdtssXEgR7sXQQQA1doBpxUpTn4pcAaVFZfQjyo-03SGjYmlk2CpYJgABhQESII6AYH_8-Nn
        iumDaEPcbPId6ZQAMFfViEDLxGVnVBNOOZGRhdGGBo2JvcGdyZXBsYWNlZHBhdGhmL3N0ZXBoZXZhbHV
        lGQFOZHByZXbYKlgmAAGFARIgjoBgf_z42eK6YNoQ9xs8h3plAAwV9WIQMvEZWdUE0466AgGFARIgJ10
        HGXlKTZ7sjbSnNf2QMt_SOPpa8hDUqpszdZCIKUOiZ3BheWxvYWRYJAFxEiB22yxcSBHuxdBBADV2gGn
        FSlOfilwBpUVl9CPKj7TdIWpzaWduYXR1cmVzgaJpcHJvdGVjdGVkWIF7ImFsZyI6IkVkRFNBIiwia2l
        kIjoiZGlkOmtleTp6Nk1rdEJ5bkFQTHJFeWVTN3BWdGhiaXlTY21mdThuNVY3Ym9YZ3h5bzVxM1NaUlI
        jejZNa3RCeW5BUExyRXllUzdwVnRoYml5U2NtZnU4bjVWN2JvWGd4eW81cTNTWlJSIn1pc2lnbmF0dXJ
        lWECym-Kwb5ti-T5dCygt4zf8Lr6MescAbkk_DILoy3fFjYG8fZVUCGKDQiTTHbNbzOk1yze7-2hA3AK
        dBfzJY1kA";

    const TIME_EVENT_CAR: &str = "
        uOqJlcm9vdHOB2CpYJQABcRIgcmqgb7eHSgQ32hS1NGVKZruLJGcKDI1f4lqOyNYn3eVndmVyc2lvbgG
        3AQFxEiByaqBvt4dKBDfaFLU0ZUpmu4skZwoMjV_iWo7I1ifd5aRiaWTYKlgmAAGFARIgjoBgf_z42eK
        6YNoQ9xs8h3plAAwV9WIQMvEZWdUE045kcGF0aGEwZHByZXbYKlgmAAGFARIgJ10HGXlKTZ7sjbSnNf2
        QMt_SOPpa8hDUqpszdZCIKUNlcHJvb2bYKlglAAFxEiAFKLx3fi7-yD1aPNyqnblI_r_5XllReVz55jB
        MvMxs9q4BAXESIAUovHd-Lv7IPVo83KqduUj-v_leWVF5XPnmMEy8zGz2pGRyb2902CpYJQABcRIgfWt
        bF-FQN6GN6ZL8OtHvp2YrGlmLbZwkOl6UY-3AUNFmdHhIYXNo2CpYJgABkwEbIBv-WU6fLnsyo5_lDST
        C_T-xUlW95brOAUDByGHJzbCRZnR4VHlwZWpmKGJ5dGVzMzIpZ2NoYWluSWRvZWlwMTU1OjExMTU1MTE
        xeQFxEiB9a1sX4VA3oY3pkvw60e-nZisaWYttnCQ6XpRj7cBQ0YPYKlgmAAGFARIgJ10HGXlKTZ7sjbS
        nNf2QMt_SOPpa8hDUqpszdZCIKUP22CpYJQABcRIgqVOMo-IVjo08Mk0cim3Z8flNyHY7c9g7uGMqeS0
        PFHA";

    mock! {
        pub AccessModelStoreTest {}
        #[async_trait]
        impl AccessModelStore for AccessModelStoreTest {
            async fn insert_many(&self, items: &[(EventId, Option<Vec<u8>>)]) -> Result<(Vec<bool>, usize)>;
            async fn range_with_values(
                &self,
                start: &EventId,
                end: &EventId,
                offset: usize,
                limit: usize,
            ) -> Result<Vec<(Cid, Vec<u8>)>>;
            async fn value_for_order_key(&self, key: &EventId) -> Result<Option<Vec<u8>>>;
            async fn value_for_cid(&self, key: &Cid) -> Result<Option<Vec<u8>>>;
            async fn events_since_highwater_mark(
                &self,
                highwater: i64,
                limit: i64,
            ) -> Result<(i64, Vec<Cid>)>;
            async fn get_block(& self, cid: &Cid) -> Result<Option<Vec<u8>>>;
        }
    }

    fn get_init_event_mock() -> MockAccessModelStoreTest {
        // Expect two get_block calls
        let mut mock_store = MockAccessModelStoreTest::new();

        // Call to get the init event envelope
        mock_store
            .expect_get_block()
            .once()
            .with(predicate::eq(Cid::from_str(INIT_EVENT_CID).unwrap()))
            .return_once(move |_| Ok(Some(decode_multibase_str(INIT_EVENT))));

        // Call to get the init event payload
        mock_store
            .expect_get_block()
            .once()
            .with(predicate::eq(
                Cid::from_str(INIT_EVENT_PAYLOAD_CID).unwrap(),
            ))
            .return_once(move |_| Ok(Some(decode_multibase_str(INIT_EVENT_PAYLOAD))));
        mock_store
    }

    async fn test_event_id_from_car(
        event_data: &str,
        expected_event_id: Expect,
        mock_store: MockAccessModelStoreTest,
    ) {
        let event_id = event_id_from_car(
            Network::Mainnet,
            decode_multibase_str(event_data).as_slice(),
            &mock_store,
        )
        .await;
        expected_event_id.assert_debug_eq(&event_id);
    }

    fn decode_multibase_str(encoded: &str) -> Vec<u8> {
        let (_, bytes) = multibase::decode(
            encoded
                .chars()
                .filter(|c| !c.is_whitespace())
                .collect::<String>(),
        )
        .unwrap();
        bytes
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
        test_event_id_from_car(INIT_EVENT_CAR, expected, MockAccessModelStoreTest::new()).await
    }

    #[tokio::test]
    #[traced_test]
    async fn event_id_from_car_unsigned_init_event() {
        let expected = expect![[r#"
            Ok(
                EventId {
                    bytes: "ce0105004bf23f697fcdb44763a8eb5b47190f472416a164017112203130e432a07501b157851b7cc2e30ca2baed822009e02323daf04e4f2416a164",
                    network_id: Some(
                        0,
                    ),
                    separator: Some(
                        "4bf23f697fcdb447",
                    ),
                    controller: Some(
                        "63a8eb5b47190f47",
                    ),
                    stream_id: Some(
                        "2416a164",
                    ),
                    cid: Some(
                        "bafyreibrgdsdfidvagyvpbi3ptbogdfcxlwyeiaj4arshwxqjzhsifvbmq",
                    ),
                },
            )
        "#]];
        test_event_id_from_car(
            // Unsigned init payload event
            "
            uOqJlcm9vdHOB2CpYJQABcRIgMTDkMqB1AbFXhRt8wuMMorrtgiAJ4CMj2vBOTyQWoWRndmVyc2lvbgG
            0AQFxEiAxMOQyoHUBsVeFG3zC4wyiuu2CIAngIyPa8E5PJBahZKJkZGF0YfZmaGVhZGVyo2NzZXBlbW9
            kZWxlbW9kZWxYKM4BAgGFARIgV1HMaNjwORnyUJjowofErLoZ5HkFm-5dsy3v2onQm6NrY29udHJvbGx
            lcnOBeDhkaWQ6a2V5Ono2TWt0Q0ZSY3dMUkZRQTlXYmVEUk03VzdrYkJkWlRIUTJ4blBneXhaTHExZ0N
            wSw",
            expected,
            // Init events do not need to access the store
            MockAccessModelStoreTest::new(),
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
        test_event_id_from_car(DATA_EVENT_CAR, expected, get_init_event_mock()).await
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
        test_event_id_from_car(TIME_EVENT_CAR, expected, get_init_event_mock()).await
    }

    #[tokio::test]
    #[traced_test]
    async fn event_id_from_car_data_event_missing() {
        let mut mock_store = MockAccessModelStoreTest::new();
        mock_store
            .expect_get_block()
            // Report that the block does not exist
            .returning(move |_| Ok(None));
        let expected = expect![[r#"
            Err(
                "cannot find block bagcqcerar2aga7747dm6fota3iipogz4q55gkaamcx2weebs6emvtvie2oha",
            )
        "#]];
        test_event_id_from_car(DATA_EVENT_CAR, expected, mock_store).await
    }
}
