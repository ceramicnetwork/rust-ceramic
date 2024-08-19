use crate::unvalidated;
use anyhow::{anyhow, bail};
use cid::Cid;
use ipld_core::ipld::Ipld;

use super::{cid_from_dag_cbor, ProofEdge};

/// Builder for constructing events.
pub struct Builder;

impl Builder {
    /// Create builder for init events
    pub fn init() -> InitBuilder<InitBuilderEmpty> {
        InitBuilder {
            state: InitBuilderEmpty,
        }
    }

    /// Create builder for data events
    pub fn data() -> DataBuilder<DataBuilderEmpty> {
        DataBuilder {
            state: DataBuilderEmpty,
        }
    }

    /// Create builder for data events
    pub fn time() -> TimeBuilder<TimeBuilderEmpty> {
        TimeBuilder {
            state: TimeBuilderEmpty,
        }
    }
}

struct Separator {
    key: String,
    value: Vec<u8>,
}

/// Builder for constructing an [`unvalidated::init::Payload`].
#[derive(Default)]
pub struct InitBuilder<S: InitBuilderState> {
    state: S,
}

/// State of the builder
pub trait InitBuilderState {}

/// Initial state
pub struct InitBuilderEmpty;
impl InitBuilderState for InitBuilderEmpty {}

/// State with controller added
pub struct InitBuilderWithController {
    controller: String,
}
impl InitBuilderState for InitBuilderWithController {}
impl InitBuilder<InitBuilderEmpty> {
    /// Specify the controller.
    pub fn with_controller(self, controller: String) -> InitBuilder<InitBuilderWithController> {
        InitBuilder {
            state: InitBuilderWithController { controller },
        }
    }
}

/// State with separator added, also supports all optional init event fields.
pub struct InitBuilderWithSep<D> {
    controller: String,
    sep: Separator,
    unique: Option<Vec<u8>>,
    data: Option<D>,
    should_index: Option<bool>,
    context: Option<Vec<u8>>,
}
impl<D> InitBuilderState for InitBuilderWithSep<D> {}
impl InitBuilder<InitBuilderWithController> {
    /// Specify the separator key and value.
    pub fn with_sep<D>(self, key: String, value: Vec<u8>) -> InitBuilder<InitBuilderWithSep<D>> {
        InitBuilder {
            state: InitBuilderWithSep {
                controller: self.state.controller,
                sep: Separator { key, value },
                unique: None,
                data: None,
                should_index: None,
                context: None,
            },
        }
    }
}

impl<D> InitBuilder<InitBuilderWithSep<D>> {
    /// Specify the unique bytes.
    pub fn with_unique(mut self, unique: Vec<u8>) -> Self {
        self.state.unique = Some(unique);
        self
    }

    /// Specify the context.
    pub fn with_context(mut self, context: Vec<u8>) -> Self {
        self.state.context = Some(context);
        self
    }

    /// Specify the should index value.
    pub fn with_should_index(mut self, should_index: bool) -> Self {
        self.state.should_index = Some(should_index);
        self
    }

    /// Specify the data.
    pub fn with_data(mut self, data: D) -> Self {
        self.state.data = Some(data);
        self
    }
    /// Build the event.
    pub fn build(self) -> unvalidated::init::Payload<D> {
        let header = unvalidated::init::Header::new(
            vec![self.state.controller],
            self.state.sep.key,
            self.state.sep.value,
            self.state.should_index,
            self.state.unique,
            self.state.context,
        );
        unvalidated::init::Payload::new(header, self.state.data)
    }
}

/// Builder for constructing an [`unvalidated::data::Payload`].
#[derive(Default)]
#[allow(private_bounds)]
pub struct DataBuilder<S: unvalidated::builder::DataBuilderState> {
    state: S,
}

/// State of the builder
pub trait DataBuilderState {}

/// Initial state
pub struct DataBuilderEmpty;
impl DataBuilderState for DataBuilderEmpty {}

/// State with id added
pub struct DataBuilderWithId {
    id: Cid,
}
impl DataBuilderState for DataBuilderWithId {}
impl DataBuilder<DataBuilderEmpty> {
    /// Specify the id.
    pub fn with_id(self, id: Cid) -> DataBuilder<DataBuilderWithId> {
        DataBuilder {
            state: DataBuilderWithId { id },
        }
    }
}

/// State with prev added
pub struct DataBuilderWithPrev {
    id: Cid,
    prev: Cid,
}
impl DataBuilderState for DataBuilderWithPrev {}
impl DataBuilder<DataBuilderWithId> {
    /// Specify the prev.
    pub fn with_prev(self, prev: Cid) -> DataBuilder<DataBuilderWithPrev> {
        DataBuilder {
            state: DataBuilderWithPrev {
                id: self.state.id,
                prev,
            },
        }
    }
}

/// State with data added, also supports the optional should_index field.
pub struct DataBuilderWithData<D> {
    id: Cid,
    prev: Cid,
    data: D,
    should_index: Option<bool>,
}
impl<D> DataBuilderState for DataBuilderWithData<D> {}
impl DataBuilder<DataBuilderWithPrev> {
    /// Specify the data.
    pub fn with_data<D>(self, data: D) -> DataBuilder<DataBuilderWithData<D>> {
        DataBuilder {
            state: DataBuilderWithData {
                id: self.state.id,
                prev: self.state.prev,
                data,
                should_index: None,
            },
        }
    }
}

impl<D> DataBuilder<DataBuilderWithData<D>> {
    /// Specify should_index.
    pub fn with_should_index(mut self, should_index: bool) -> Self {
        self.state.should_index = Some(should_index);
        self
    }

    /// Build the event.
    pub fn build(self) -> unvalidated::data::Payload<D> {
        let header = self
            .state
            .should_index
            .map(|si| unvalidated::data::Header::new(Some(si)));
        unvalidated::data::Payload::new(self.state.id, self.state.prev, header, self.state.data)
    }
}

/// Builder for constructing an [`unvalidated::TimeEvent`].
#[derive(Default)]
#[allow(private_bounds)]
pub struct TimeBuilder<S: unvalidated::builder::TimeBuilderState> {
    state: S,
}

/// State of the builder
pub trait TimeBuilderState {}

/// Initial state
pub struct TimeBuilderEmpty;
impl TimeBuilderState for TimeBuilderEmpty {}

impl TimeBuilder<TimeBuilderEmpty> {
    /// Specify the Cid of the init event for the stream.
    pub fn with_id(self, id: Cid) -> TimeBuilder<TimeBuilderWithId> {
        TimeBuilder {
            state: TimeBuilderWithId { id },
        }
    }
}

/// State with id added
pub struct TimeBuilderWithId {
    id: Cid,
}
impl TimeBuilderState for TimeBuilderWithId {}

impl TimeBuilder<TimeBuilderWithId> {
    /// Specify details about the time transaction.
    pub fn with_tx(
        self,
        chain_id: String,
        tx_hash: Cid,
        tx_type: String,
    ) -> TimeBuilder<TimeBuilderWithTx> {
        TimeBuilder {
            state: TimeBuilderWithTx {
                id: self.state.id,
                chain_id,
                tx_hash,
                tx_type,
            },
        }
    }
}

/// State with the transaction details.
pub struct TimeBuilderWithTx {
    id: Cid,
    chain_id: String,
    tx_hash: Cid,
    tx_type: String,
}
impl TimeBuilderState for TimeBuilderWithTx {}

impl TimeBuilder<TimeBuilderWithTx> {
    /// Specify the root node of the witness proof.
    /// The edge_index is an index into the node that should be followed.
    pub fn with_root(self, edge_index: usize, node: Ipld) -> TimeBuilder<TimeBuilderWithRoot> {
        TimeBuilder {
            state: TimeBuilderWithRoot {
                id: self.state.id,
                chain_id: self.state.chain_id,
                tx_hash: self.state.tx_hash,
                tx_type: self.state.tx_type,
                witness_nodes: vec![(edge_index, node)],
            },
        }
    }

    /// Specify the Cid of the event being anchored. Only used in the special case where the event
    /// was anchored in a single-write batch and no anchor merkle tree was created.
    pub fn with_prev(self, prev: Cid) -> TimeBuilder<TimeBuilderWithPrev> {
        TimeBuilder {
            state: TimeBuilderWithPrev {
                id: self.state.id,
                chain_id: self.state.chain_id,
                tx_hash: self.state.tx_hash,
                tx_type: self.state.tx_type,
                prev,
            },
        }
    }
}

/// State with the Cid that is anchored by this TimeEvent added.
/// Only used for TimeEvents from single-write anchor batches that don't have an associated
/// anchor merkle tree.
pub struct TimeBuilderWithPrev {
    id: Cid,
    chain_id: String,
    tx_hash: Cid,
    tx_type: String,
    prev: Cid,
}
impl TimeBuilderState for TimeBuilderWithPrev {}
impl TimeBuilder<TimeBuilderWithPrev> {
    /// Build the [`unvalidated::TimeEvent`].
    pub fn build(self) -> anyhow::Result<unvalidated::TimeEvent> {
        let proof = unvalidated::Proof::new(
            self.state.chain_id,
            self.state.prev,
            self.state.tx_hash,
            self.state.tx_type,
        );
        let proof_bytes = serde_ipld_dagcbor::to_vec(&proof)?;
        let proof_cid = cid_from_dag_cbor(&proof_bytes);

        let event = unvalidated::RawTimeEvent::new(
            self.state.id,
            self.state.prev,
            proof_cid,
            "".to_string(),
        );
        Ok(unvalidated::TimeEvent::new(event, proof, vec![]))
    }
}

/// State with the proof root node added. Used in the (more common) case where the event was
/// anchored in a batch along with other writes and so there is an anchor merkle tree and a witness
/// proof path through that tree.
/// More nodes may be added.
pub struct TimeBuilderWithRoot {
    id: Cid,
    chain_id: String,
    tx_hash: Cid,
    tx_type: String,
    witness_nodes: Vec<(usize, Ipld)>,
}
impl TimeBuilderState for TimeBuilderWithRoot {}
impl TimeBuilder<TimeBuilderWithRoot> {
    /// Specify an additional node in the witness proof.
    /// The edge_index is an index into the node that should be followed.
    /// The last edge_index must index to a Cid of the previous event.
    pub fn with_witness_node(mut self, edge_index: usize, node: Ipld) -> Self {
        self.state.witness_nodes.push((edge_index, node));
        self
    }
    /// Build the [`unvalidated::TimeEvent`].
    /// Errors if the proof edges and indexes are not valid.
    pub fn build(self) -> anyhow::Result<unvalidated::TimeEvent> {
        let path = self
            .state
            .witness_nodes
            .iter()
            .map(|(index, _edge)| index.to_string())
            .collect::<Vec<_>>()
            .join("/");
        let (_index, root) = self
            .state
            .witness_nodes
            .first()
            .expect("should always be at least one edge");
        let (leaf_index, leaf_edge) = self
            .state
            .witness_nodes
            .iter()
            .last()
            .expect("should always be at least one edge");
        let prev = leaf_edge
            .get(*leaf_index)?
            .ok_or_else(|| anyhow!("leaf index should always exist"))?;
        let prev = match prev {
            Ipld::Link(prev) => *prev,
            _ => bail!("leaf indexed value should always be a Cid"),
        };
        let root_bytes = serde_ipld_dagcbor::to_vec(root)?;
        let root_cid = cid_from_dag_cbor(&root_bytes);
        let proof = unvalidated::Proof::new(
            self.state.chain_id,
            root_cid,
            self.state.tx_hash,
            self.state.tx_type,
        );
        let proof_bytes = serde_ipld_dagcbor::to_vec(&proof)?;
        let proof_cid = cid_from_dag_cbor(&proof_bytes);
        let blocks_in_path = self
            .state
            .witness_nodes
            .into_iter()
            .map(|(_index, edge)| match edge {
                Ipld::List(v) => Ok(v),
                ipld => {
                    tracing::info!(?ipld, "Time event witness node is not a list");
                    Err(anyhow!("Time event witness node is not a list"))
                }
            })
            .collect::<anyhow::Result<Vec<ProofEdge>>>()?;

        let event = unvalidated::RawTimeEvent::new(self.state.id, prev, proof_cid, path);
        Ok(unvalidated::TimeEvent::new(event, proof, blocks_in_path))
    }
}
#[cfg(test)]
mod tests {
    use std::str::FromStr as _;

    use ceramic_core::StreamId;
    use ipld_core::ipld;
    use ipld_core::ipld::Ipld;
    use multibase;
    use test_log::test;

    use super::*;
    use crate::unvalidated::signed;
    use crate::unvalidated::signed::JwkSigner;
    use crate::unvalidated::tests::{
        DATA_EVENT_PAYLOAD, SIGNED_INIT_EVENT, SIGNED_INIT_EVENT_CAR, SIGNED_INIT_EVENT_CID,
        SIGNED_INIT_EVENT_PAYLOAD, TIME_EVENT_CAR_SINGLE_EVENT_BATCH,
    };
    use crate::DidDocument;

    #[test]
    fn build_init_payload() {
        let model =
            StreamId::from_str("kjzl6hvfrbw6c90uwoyz8j519gxma787qbsfjtrarkr1huq1g1s224k7hopvsyg")
                .unwrap();
        let model = model.to_vec();
        let unique = vec![68, 166, 241, 58, 178, 65, 11, 187, 106, 133, 104, 222];
        let data = ipld_core::ipld!({"steph": 333});

        let event = Builder::init()
            .with_controller("did:key:z6MktBynAPLrEyeS7pVthbiyScmfu8n5V7boXgxyo5q3SZRR".to_string())
            .with_sep("model".to_string(), model)
            .with_unique(unique)
            .with_data(data)
            .build();

        let dagcbor_str = multibase::encode(
            multibase::Base::Base64Url,
            serde_ipld_dagcbor::to_vec(&event).unwrap(),
        );
        assert_eq!(SIGNED_INIT_EVENT_PAYLOAD, dagcbor_str);
    }

    #[test]
    fn build_data_payload() {
        let data = ipld_core::ipld!([{"op":"replace","path":"/steph","value":334}]);
        let id = Cid::from_str(SIGNED_INIT_EVENT_CID).unwrap();
        let prev = Cid::from_str(SIGNED_INIT_EVENT_CID).unwrap();

        let event = Builder::data()
            .with_id(id)
            .with_prev(prev)
            .with_data(data)
            .build();

        let dagcbor_str = multibase::encode(
            multibase::Base::Base64Url,
            serde_ipld_dagcbor::to_vec(&event).unwrap(),
        );
        assert_eq!(DATA_EVENT_PAYLOAD, dagcbor_str);
    }

    #[test(tokio::test)]
    async fn sign_init_payload() {
        let model =
            StreamId::from_str("kjzl6hvfrbw6c90uwoyz8j519gxma787qbsfjtrarkr1huq1g1s224k7hopvsyg")
                .unwrap();
        let model = model.to_vec();
        let unique = vec![68, 166, 241, 58, 178, 65, 11, 187, 106, 133, 104, 222];
        let data = ipld_core::ipld!({"steph": 333});

        let payload = Builder::init()
            .with_controller("did:key:z6MktBynAPLrEyeS7pVthbiyScmfu8n5V7boXgxyo5q3SZRR".to_string())
            .with_sep("model".to_string(), model)
            .with_unique(unique)
            .with_data(data)
            .build();

        let signer = JwkSigner::new(
            DidDocument::new("did:key:z6MktBynAPLrEyeS7pVthbiyScmfu8n5V7boXgxyo5q3SZRR#z6MktBynAPLrEyeS7pVthbiyScmfu8n5V7boXgxyo5q3SZRR"),
            "df9ecf4c79e5ad77701cfc88c196632b353149d85810a381f469f8fc05dc1b92",
        )
        .await
        .unwrap();

        let signed_event =
            signed::Event::from_payload(unvalidated::Payload::Init(payload), signer).unwrap();

        let envelope_cbor_str = multibase::encode(
            multibase::Base::Base64Url,
            signed_event.encode_envelope().unwrap(),
        );

        assert_eq!(SIGNED_INIT_EVENT, envelope_cbor_str);

        let event_car_str = multibase::encode(
            multibase::Base::Base64Url,
            signed_event.encode_car().unwrap(),
        );
        assert_eq!(SIGNED_INIT_EVENT_CAR, event_car_str);
    }
    #[test(tokio::test)]
    async fn build_time_event_single_event_batch() {
        let id = Cid::from_str(SIGNED_INIT_EVENT_CID).unwrap();
        let prev =
            Cid::from_str("bagcqcerae5oqoglzjjgz53enwsttl7mqglp5eoh2llzbbvfktmzxleeiffbq").unwrap();
        let tx_hash =
            Cid::from_str("bagjqcgzadp7fstu7fz5tfi474ugsjqx5h6yvevn54w5m4akayhegdsonwciq").unwrap();

        let metadata_cid =
            Cid::from_str("bafyreifjkogkhyqvr2gtymsndsfg3wpr7fg4q5r3opmdxoddfj4s2dyuoa").unwrap();
        let event = Builder::time()
            .with_id(id)
            .with_tx(
                "eip155:11155111".to_string(),
                tx_hash,
                "f(bytes32)".to_string(),
            )
            .with_root(0, ipld! {[prev, Ipld::Null, metadata_cid]})
            .build()
            .unwrap();

        let event_car_str =
            multibase::encode(multibase::Base::Base64Url, event.encode_car().unwrap());
        assert_eq!(TIME_EVENT_CAR_SINGLE_EVENT_BATCH, event_car_str);
    }
}
