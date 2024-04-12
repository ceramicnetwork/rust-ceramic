//! Tests in the file rely on a few patterns.
//!
//! ## Debug + Display + Pretty
//!
//! We leverage Debug, Display, and Pretty various purposes.
//!
//! * Display - User facing representation of the data
//! * Debug - Compact developer facing representation of the data (i.e. first few chars of a hash)
//! * Debug Alternate ({:#?}) - Full debug representation of the data
//! * Pretty - Psuedo sequence diagram representation (used for sequence tests)

lalrpop_util::lalrpop_mod!(
    #[allow(clippy::all, missing_debug_implementations)]
    pub parser, "/recon/parser.rs"
); // synthesized by LALRPOP

use anyhow::Result;
use async_trait::async_trait;
use ceramic_core::RangeOpen;
use futures::{ready, Future, Sink, Stream};
use pin_project::pin_project;
use prometheus_client::registry::Registry;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt::Display};
use std::{collections::BTreeSet, sync::Arc};
use test_log::test;
use tokio::sync::mpsc::channel;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::PollSender;
use tracing::debug;

use codespan_reporting::{
    diagnostic::{Diagnostic, Label},
    files::SimpleFiles,
    term::{self, termcolor::Buffer},
};
use expect_test::{expect, Expect};
use lalrpop_util::ParseError;
use pretty::{Arena, DocAllocator, DocBuilder, Pretty};

use crate::protocol::ReconMessage;
use crate::{
    protocol::{self, InitiatorMessage, ResponderMessage, ValueResponse},
    recon::{FullInterests, HashCount, InterestProvider, Range, ReconItem},
    tests::AlphaNumBytes,
    AssociativeHash, BTreeStore, Client, Key, Metrics, Recon, Server, Sha256a, Store,
};

#[derive(Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MemoryAHash {
    ahash: Sha256a,
    set: BTreeSet<AlphaNumBytes>,
}

impl std::fmt::Debug for MemoryAHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            f.debug_struct("MemoryAHash")
                .field("ahash", &self.ahash)
                .field("set", &self.set)
                .finish()
        } else if self.is_zero() {
            write!(f, "0")
        } else {
            write!(f, "h(")?;
            for (i, key) in self.set.iter().enumerate() {
                if i != 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{key}")?;
            }
            write!(f, ")")
        }
    }
}

impl std::fmt::Display for MemoryAHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl std::ops::Add for MemoryAHash {
    type Output = Self;
    fn add(self, rhs: Self) -> Self::Output {
        Self {
            ahash: self.ahash + rhs.ahash,
            set: self.set.union(&rhs.set).map(|s| s.to_owned()).collect(),
        }
    }
}

impl AssociativeHash for MemoryAHash {
    fn digest<K: Key>(key: &K) -> Self {
        Self {
            ahash: Sha256a::digest(key),
            // Note we do not preserve the original key type and always use a generic Bytes key
            set: BTreeSet::from_iter([key.as_bytes().into()]),
        }
    }

    fn as_bytes(&self) -> [u8; 32] {
        self.ahash.as_bytes()
    }

    /// unpack the raw ints from a MemoryAHash
    fn as_u32s(&self) -> &[u32; 8] {
        self.ahash.as_u32s()
    }
}

impl From<[u32; 8]> for MemoryAHash {
    fn from(state: [u32; 8]) -> Self {
        Self {
            set: BTreeSet::default(),
            ahash: Sha256a::from(state),
        }
    }
}

/// Recon type that uses Bytes for a Key and MemoryAHash for the Hash
pub type ReconMemoryBytes = Recon<
    AlphaNumBytes,
    MemoryAHash,
    BTreeStore<AlphaNumBytes, MemoryAHash>,
    FixedInterests<AlphaNumBytes>,
>;

/// Recon type that uses Bytes for a Key and Sha256a for the Hash
pub type ReconBytes =
    Recon<AlphaNumBytes, Sha256a, BTreeStore<AlphaNumBytes, Sha256a>, FullInterests<AlphaNumBytes>>;

/// Implement InterestProvider for a fixed set of interests.
#[derive(Clone, Debug, PartialEq)]
pub struct FixedInterests<K>(Vec<RangeOpen<K>>);

impl<K: Key> FixedInterests<K> {
    pub fn full() -> Self {
        Self(vec![(K::min_value(), K::max_value()).into()])
    }
    pub fn is_full(&self) -> bool {
        self == &Self::full()
    }
}
#[async_trait]
impl<K: Key> InterestProvider for FixedInterests<K> {
    type Key = K;

    async fn interests(&self) -> anyhow::Result<Vec<RangeOpen<Self::Key>>> {
        Ok(self.0.clone())
    }
}

#[derive(Debug)]
pub struct Sequence<K: Key, H: AssociativeHash> {
    setup: SequenceSetup<K>,
    steps: Vec<SequenceStep<K, H>>,
    r#final: SequenceFinal<K>,
}

impl<K, H> Display for Sequence<K, H>
where
    K: Key,
    H: AssociativeHash,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let arena: Arena<()> = Arena::new();
        let mut w = Vec::new();
        self.pretty(&arena).render(200, &mut w).unwrap();
        write!(f, "{}", String::from_utf8(w).unwrap())
    }
}

impl<'a, D, A, K, H> Pretty<'a, D, A> for &'a Sequence<K, H>
where
    A: 'a + Clone,
    D: DocAllocator<'a, A>,
    D::Doc: Clone,
    K: Key,
    H: AssociativeHash,
{
    fn pretty(self, allocator: &'a D) -> DocBuilder<'a, D, A> {
        self.setup
            .pretty(allocator)
            .append(allocator.hardline())
            .append(allocator.intersperse(&self.steps, allocator.hardline()))
            .append(allocator.hardline())
            .append(self.r#final.pretty(allocator))
            .append(allocator.hardline())
    }
}

#[derive(Clone, Debug)]
pub struct SequenceSetup<K: Key> {
    cat: SetupState<K>,
    dog: SetupState<K>,
}

impl<'a, D, A, K> Pretty<'a, D, A> for &'a SequenceSetup<K>
where
    A: 'a + Clone,
    D: DocAllocator<'a, A>,
    D::Doc: Clone,
    K: Key,
{
    fn pretty(self, allocator: &'a D) -> DocBuilder<'a, D, A> {
        allocator
            .text("cat: ")
            .append(self.cat.pretty(allocator))
            .append(allocator.hardline())
            .append(allocator.text("dog: ").append(self.dog.pretty(allocator)))
    }
}

#[derive(Clone, Debug)]
pub struct SetupState<K: Key> {
    interests: FixedInterests<K>,
    state: BTreeMap<K, Option<K>>,
}

async fn from_setup_state(setup: SetupState<AlphaNumBytes>) -> ReconMemoryBytes {
    Recon {
        interests: setup.interests,
        store: BTreeStore::from_set(
            setup
                .state
                .into_iter()
                .map(|(k, v)| (k, v.map(|v| v.into_inner())))
                .collect(),
        )
        .await,
        metrics: Metrics::register(&mut Registry::default()),
    }
}

impl<'a, D, A, K> Pretty<'a, D, A> for &'a SetupState<K>
where
    A: 'a + Clone,
    D: DocAllocator<'a, A>,
    D::Doc: Clone,
    K: Key,
{
    fn pretty(self, allocator: &'a D) -> DocBuilder<'a, D, A> {
        // Special case full interests as nil
        let interests = if self.interests.is_full() {
            allocator.nil()
        } else {
            allocator
                .intersperse(
                    self.interests.0.iter().map(PrettyRangeOpen),
                    allocator.text(", "),
                )
                .angles()
                .append(allocator.space())
        };
        interests.append(PrettySet(&self.state).pretty(allocator))
    }
}

#[derive(Debug)]
pub struct SequenceStep<K, H>
where
    K: Key,
    H: AssociativeHash,
{
    message: Message<K, H>,
    state: BTreeMap<K, Option<K>>,
}

impl<'a, D, A, K, H> Pretty<'a, D, A> for &'a SequenceStep<K, H>
where
    A: 'a + Clone,
    D: DocAllocator<'a, A>,
    D::Doc: Clone,
    K: Key,
    H: AssociativeHash,
{
    fn pretty(self, allocator: &'a D) -> DocBuilder<'a, D, A> {
        self.message
            .pretty(allocator)
            .append(allocator.hardline())
            .append(
                match self.message {
                    Message::CatToDog(_) => allocator.text("cat: "),
                    Message::DogToCat(_) => allocator.text("dog: "),
                }
                .append(PrettySet(&self.state).pretty(allocator))
                .indent(4),
            )
    }
}

#[derive(Debug)]
pub enum Message<K, H>
where
    K: Key,
    H: AssociativeHash,
{
    CatToDog(InitiatorMessage<K, H>),
    DogToCat(ResponderMessage<K, H>),
}

impl<'a, D, A, K, H> Pretty<'a, D, A> for &'a Message<K, H>
where
    A: 'a + Clone,
    D: DocAllocator<'a, A>,
    D::Doc: Clone,
    K: Key,
    H: AssociativeHash,
{
    fn pretty(self, allocator: &'a D) -> DocBuilder<'a, D, A> {
        match self {
            Message::CatToDog(msg) => {
                let dir = allocator.text("-> ");
                match msg {
                    InitiatorMessage::InterestRequest(ir) => dir.append(
                        allocator.text("interest_req").append(
                            allocator
                                .intersperse(ir.iter().map(PrettyRangeOpen), allocator.text(", "))
                                .parens(),
                        ),
                    ),
                    InitiatorMessage::RangeRequest(rr) => dir.append(
                        allocator
                            .text("range_req")
                            .append(PrettyRange(rr).pretty(allocator).parens()),
                    ),
                    InitiatorMessage::ValueRequest(key) => dir.append(
                        allocator
                            .text("value_req")
                            .append(PrettyKey(key).pretty(allocator).parens()),
                    ),
                    InitiatorMessage::ValueResponse(vr) => dir.append(
                        allocator
                            .text("value_resp")
                            .append(PrettyValueResponse(vr).pretty(allocator).parens()),
                    ),
                    InitiatorMessage::ListenOnly => dir.append(allocator.text("listen_only")),
                    InitiatorMessage::Finished => dir.append(allocator.text("finished")),
                }
            }
            Message::DogToCat(msg) => {
                let dir = allocator.text("<- ");
                match msg {
                    ResponderMessage::InterestResponse(ir) => dir.append(
                        allocator.text("interest_resp").append(
                            allocator
                                .intersperse(ir.iter().map(PrettyRangeOpen), allocator.text(", "))
                                .parens(),
                        ),
                    ),
                    ResponderMessage::RangeResponse(rr) => dir.append(
                        allocator.text("range_resp").append(
                            allocator
                                .intersperse(rr.iter().map(PrettyRange), allocator.text(", "))
                                .parens(),
                        ),
                    ),
                    ResponderMessage::ValueRequest(key) => dir.append(
                        allocator
                            .text("value_req")
                            .append(PrettyKey(key).pretty(allocator).parens()),
                    ),
                    ResponderMessage::ValueResponse(vr) => dir.append(
                        allocator
                            .text("value_resp")
                            .append(PrettyValueResponse(vr).pretty(allocator).parens()),
                    ),
                    ResponderMessage::ListenOnly => dir.append(allocator.text("listen_only")),
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct SequenceFinal<K: Key> {
    cat: BTreeMap<K, Option<K>>,
    dog: BTreeMap<K, Option<K>>,
}

impl<'a, D, A, K> Pretty<'a, D, A> for &'a SequenceFinal<K>
where
    A: 'a + Clone,
    D: DocAllocator<'a, A>,
    D::Doc: Clone,
    K: Key,
{
    fn pretty(self, allocator: &'a D) -> DocBuilder<'a, D, A> {
        allocator
            .text("cat: ")
            .append(PrettySet(&self.cat).pretty(allocator))
            .append(allocator.hardline())
            .append(allocator.text("dog: "))
            .append(PrettySet(&self.dog).pretty(allocator))
    }
}

struct PrettyKey<'a, K>(pub &'a K);

impl<'a, D, A, K> Pretty<'a, D, A> for PrettyKey<'a, K>
where
    A: 'a + Clone,
    D: DocAllocator<'a, A>,
    D::Doc: Clone,
    K: Key,
{
    fn pretty(self, allocator: &'a D) -> DocBuilder<'a, D, A> {
        // Use Alpha and Omega as the min and max values respectively
        if self.0 == &K::min_value() {
            allocator.text("𝚨".to_string())
        } else if self.0 == &K::max_value() {
            allocator.text("𝛀 ".to_string())
        } else {
            allocator.text(format!("{:?}", self.0))
        }
    }
}

struct PrettyHash<'a, H>(pub &'a HashCount<H>);

impl<'a, D, A, H> Pretty<'a, D, A> for PrettyHash<'a, H>
where
    A: 'a + Clone,
    D: DocAllocator<'a, A>,
    D::Doc: Clone,
    H: AssociativeHash,
{
    fn pretty(self, allocator: &'a D) -> DocBuilder<'a, D, A> {
        if self.0.hash.is_zero() {
            allocator.text("0")
        } else {
            allocator.text(format!("{:?}", self.0))
        }
    }
}

struct PrettyRange<'a, K, H>(pub &'a Range<K, H>);

impl<'a, D, A, K, H> Pretty<'a, D, A> for PrettyRange<'a, K, H>
where
    A: 'a + Clone,
    D: DocAllocator<'a, A>,
    D::Doc: Clone,
    K: Key,
    H: AssociativeHash,
{
    fn pretty(self, allocator: &'a D) -> DocBuilder<'a, D, A> {
        PrettyKey(&self.0.first)
            .pretty(allocator)
            .append(allocator.space())
            .append(PrettyHash(&self.0.hash).pretty(allocator))
            .append(allocator.space())
            .append(PrettyKey(&self.0.last).pretty(allocator))
            .braces()
    }
}
struct PrettyRangeOpen<'a, T>(pub &'a RangeOpen<T>);

impl<'a, D, A, T> Pretty<'a, D, A> for PrettyRangeOpen<'a, T>
where
    A: 'a + Clone,
    D: DocAllocator<'a, A>,
    D::Doc: Clone,
    T: Key,
{
    fn pretty(self, allocator: &'a D) -> DocBuilder<'a, D, A> {
        PrettyKey(&self.0.start)
            .pretty(allocator)
            .append(allocator.text(", "))
            .append(PrettyKey(&self.0.end).pretty(allocator))
            .parens()
    }
}

struct PrettyValueResponse<'a, K>(pub &'a ValueResponse<K>);

impl<'a, D, A, K> Pretty<'a, D, A> for PrettyValueResponse<'a, K>
where
    A: 'a + Clone,
    D: DocAllocator<'a, A>,
    D::Doc: Clone,
    K: Key,
{
    fn pretty(self, allocator: &'a D) -> DocBuilder<'a, D, A> {
        PrettyKey(&self.0.key)
            .pretty(allocator)
            .append(allocator.text(": "))
            .append(format!("{}", AlphaNumBytes::from(self.0.value.clone())))
    }
}

struct PrettySet<'a, K, V>(pub &'a BTreeMap<K, Option<V>>);

impl<'a, D, A, K, V> Pretty<'a, D, A> for PrettySet<'a, K, V>
where
    A: 'a + Clone,
    D: DocAllocator<'a, A>,
    D::Doc: Clone,
    K: std::fmt::Display,
    V: std::fmt::Display,
{
    fn pretty(self, allocator: &'a D) -> DocBuilder<'a, D, A> {
        allocator
            .intersperse(
                self.0.iter().map(|(k, v)| {
                    allocator
                        .text(k.to_string())
                        .append(allocator.text(": "))
                        .append(if let Some(v) = v {
                            allocator.text(v.to_string())
                        } else {
                            allocator.text("∅")
                        })
                }),
                allocator.text(", "),
            )
            .brackets()
    }
}

fn start_recon<K, H, S, I>(recon: Recon<K, H, S, I>) -> Client<K, H>
where
    K: Key,
    H: AssociativeHash,
    S: Store<Key = K, Hash = H> + Send + Sync + 'static,
    I: InterestProvider<Key = K> + Send + Sync + 'static,
{
    let mut server = Server::new(recon);
    let client = server.client();
    tokio::spawn(server.run());
    client
}

#[test(tokio::test)]
async fn word_lists() {
    async fn recon_from_string(s: &str) -> Client<AlphaNumBytes, Sha256a> {
        let r = ReconBytes::new(
            BTreeStore::default(),
            FullInterests::default(),
            Metrics::register(&mut Registry::default()),
        );
        for key in s.split([' ', '\n']).map(|s| s.to_string()) {
            if !s.is_empty() {
                r.insert(&ReconItem::new(
                    &key.as_bytes().into(),
                    key.to_uppercase().as_bytes().into(),
                ))
                .await
                .unwrap();
            }
        }
        start_recon(r)
    }
    let mut peers = vec![
        recon_from_string(include_str!("./testdata/bip_39.txt")).await,
        recon_from_string(include_str!("./testdata/eff_large_wordlist.txt")).await,
        recon_from_string(include_str!("./testdata/eff_short_wordlist_1.txt")).await,
        recon_from_string(include_str!("./testdata/eff_short_wordlist_2.txt")).await,
        recon_from_string(include_str!("./testdata/wordle_words5_big.txt")).await,
        recon_from_string(include_str!("./testdata/wordle_words5.txt")).await,
    ];
    let expected_ahash =
        expect![["495BF24CE0DB5C33CE846ADCD6D9A87592E05324585D85059C3DC2113B500F79#21139"]];

    for peer in &mut peers {
        debug!(count = peer.len().await.unwrap(), "initial peer state");
    }

    let local = ReconBytes::new(
        BTreeStore::default(),
        FullInterests::default(),
        Metrics::register(&mut Registry::default()),
    );
    let local = start_recon(local);
    async fn sync_pair(
        local: Client<AlphaNumBytes, Sha256a>,
        remote: Client<AlphaNumBytes, Sha256a>,
    ) {
        type InitiatorEnv = ReconMessage<InitiatorMessage<AlphaNumBytes, Sha256a>>;
        type ResponderEnv = ReconMessage<ResponderMessage<AlphaNumBytes, Sha256a>>;

        let (local_channel, remote_channel): (
            DuplexChannel<InitiatorEnv, ResponderEnv>,
            DuplexChannel<ResponderEnv, InitiatorEnv>,
        ) = duplex(10000);

        // Spawn a task for each half to make things go quick, we do not care about determinism
        // here.
        let local_handle = tokio::spawn(protocol::initiate_synchronize(local, local_channel));
        let remote_handle = tokio::spawn(protocol::respond_synchronize(remote, remote_channel));
        // Error if either synchronize method errors
        let (local, remote) = tokio::join!(local_handle, remote_handle);
        local.unwrap().unwrap();
        remote.unwrap().unwrap();
    }
    async fn sync_all(
        local: Client<AlphaNumBytes, Sha256a>,
        peers: &[Client<AlphaNumBytes, Sha256a>],
    ) {
        for j in 0..3 {
            for (i, peer) in peers.iter().enumerate() {
                debug!(
                    round = j,
                    local.count = local.len().await.unwrap(),
                    remote.count = peer.len().await.unwrap(),
                    remote.peer = i,
                    "state before sync",
                );
                sync_pair(local.clone(), peer.clone()).await;
                debug!(
                    round = j,
                    local.count = local.len().await.unwrap(),
                    remote.count = peer.len().await.unwrap(),
                    remote.peer = i,
                    "state after sync",
                );
            }
        }
    }
    sync_all(local.clone(), &peers).await;

    let mut all_peers = Vec::with_capacity(peers.len() + 1);
    all_peers.push(local);
    all_peers.append(&mut peers);

    for peer in all_peers.iter_mut() {
        let full_range = peer
            .initial_range((AlphaNumBytes::min_value(), AlphaNumBytes::max_value()).into())
            .await
            .unwrap();
        expected_ahash.assert_eq(&full_range.hash.to_string())
    }
}

fn parse_sequence(sequence: &str) -> SequenceSetup<AlphaNumBytes> {
    // We only parse the setup which is the first two lines.
    let setup = sequence
        .split('\n')
        .filter(|line| !line.trim().is_empty())
        .take(2)
        .collect::<Vec<&str>>()
        .join("\n");
    // Setup codespan-reporting
    let mut files = SimpleFiles::new();
    let file_id = files.add("sequence.recon", &setup);
    match parser::SequenceSetupParser::new().parse(&setup) {
        Ok(r) => r,
        Err(e) => {
            let mut diagnostic = Diagnostic::error();
            match e {
                ParseError::InvalidToken { location } => {
                    diagnostic = diagnostic
                        .with_message("invalid token")
                        .with_labels(vec![Label::primary(file_id, location..location)]);
                }
                ParseError::UnrecognizedEof { location, expected } => {
                    diagnostic = diagnostic
                        .with_message("unrecognized EOF")
                        .with_labels(vec![Label::primary(file_id, location..location)])
                        .with_notes(vec![format!("expected one of {}", expected.join(","))]);
                }
                ParseError::UnrecognizedToken { token, expected } => {
                    diagnostic = diagnostic
                        .with_message("unrecognized token")
                        .with_labels(vec![Label::primary(file_id, token.0..token.2)])
                        .with_notes(vec![format!("expected one of {}", expected.join(" "))]);
                }
                ParseError::ExtraToken { token } => {
                    diagnostic = diagnostic
                        .with_message("extra token")
                        .with_labels(vec![Label::primary(file_id, token.0..token.2)]);
                }
                ParseError::User { error } => {
                    diagnostic = diagnostic.with_message(error);
                }
            };
            // Write diagnostic to buffer and panic to fail the test
            let mut writer = Buffer::ansi();
            let config = codespan_reporting::term::Config::default();
            term::emit(&mut writer, &config, &files, &diagnostic).unwrap();
            panic!("{}", String::from_utf8(writer.into_inner()).unwrap())
        }
    }
}

fn test_parse_sequence(sequence: &str, expect: Expect) {
    let setup = parse_sequence(sequence);
    expect.assert_debug_eq(&setup);
}

#[test]
fn parse_sequence_small() {
    test_parse_sequence(
        r#"
        cat: [a:A,b:B,c:C]
        dog: [e:E,f:F,g:G]
        "#,
        expect![[r#"
            SequenceSetup {
                cat: SetupState {
                    interests: FixedInterests(
                        [
                            RangeOpen {
                                start: Bytes(
                                    "",
                                ),
                                end: Bytes(
                                    "0xFF",
                                ),
                            },
                        ],
                    ),
                    state: {
                        Bytes(
                            "a",
                        ): Some(
                            Bytes(
                                "A",
                            ),
                        ),
                        Bytes(
                            "b",
                        ): Some(
                            Bytes(
                                "B",
                            ),
                        ),
                        Bytes(
                            "c",
                        ): Some(
                            Bytes(
                                "C",
                            ),
                        ),
                    },
                },
                dog: SetupState {
                    interests: FixedInterests(
                        [
                            RangeOpen {
                                start: Bytes(
                                    "",
                                ),
                                end: Bytes(
                                    "0xFF",
                                ),
                            },
                        ],
                    ),
                    state: {
                        Bytes(
                            "e",
                        ): Some(
                            Bytes(
                                "E",
                            ),
                        ),
                        Bytes(
                            "f",
                        ): Some(
                            Bytes(
                                "F",
                            ),
                        ),
                        Bytes(
                            "g",
                        ): Some(
                            Bytes(
                                "G",
                            ),
                        ),
                    },
                },
            }
        "#]],
    )
}

#[test]
fn parse_sequence_empty_set() {
    test_parse_sequence(
        r#"
cat: [a: X]
dog: []
        "#,
        expect![[r#"
            SequenceSetup {
                cat: SetupState {
                    interests: FixedInterests(
                        [
                            RangeOpen {
                                start: Bytes(
                                    "",
                                ),
                                end: Bytes(
                                    "0xFF",
                                ),
                            },
                        ],
                    ),
                    state: {
                        Bytes(
                            "a",
                        ): Some(
                            Bytes(
                                "X",
                            ),
                        ),
                    },
                },
                dog: SetupState {
                    interests: FixedInterests(
                        [
                            RangeOpen {
                                start: Bytes(
                                    "",
                                ),
                                end: Bytes(
                                    "0xFF",
                                ),
                            },
                        ],
                    ),
                    state: {},
                },
            }
        "#]],
    )
}
#[test]
fn parse_sequence_interests_alpha_omega() {
    test_parse_sequence(
        r#"
        cat: <(𝚨,c)> [a:A,b:B,c:C]
        dog: <(b,f),(g,𝛀)> [e:E,f:F,g:G]
        "#,
        expect![[r#"
            SequenceSetup {
                cat: SetupState {
                    interests: FixedInterests(
                        [
                            RangeOpen {
                                start: Bytes(
                                    "",
                                ),
                                end: Bytes(
                                    "c",
                                ),
                            },
                        ],
                    ),
                    state: {
                        Bytes(
                            "a",
                        ): Some(
                            Bytes(
                                "A",
                            ),
                        ),
                        Bytes(
                            "b",
                        ): Some(
                            Bytes(
                                "B",
                            ),
                        ),
                        Bytes(
                            "c",
                        ): Some(
                            Bytes(
                                "C",
                            ),
                        ),
                    },
                },
                dog: SetupState {
                    interests: FixedInterests(
                        [
                            RangeOpen {
                                start: Bytes(
                                    "b",
                                ),
                                end: Bytes(
                                    "f",
                                ),
                            },
                            RangeOpen {
                                start: Bytes(
                                    "g",
                                ),
                                end: Bytes(
                                    "0xFF",
                                ),
                            },
                        ],
                    ),
                    state: {
                        Bytes(
                            "e",
                        ): Some(
                            Bytes(
                                "E",
                            ),
                        ),
                        Bytes(
                            "f",
                        ): Some(
                            Bytes(
                                "F",
                            ),
                        ),
                        Bytes(
                            "g",
                        ): Some(
                            Bytes(
                                "G",
                            ),
                        ),
                    },
                },
            }
        "#]],
    )
}
#[test]
fn parse_sequence_missing_value() {
    test_parse_sequence(
        r#"
cat: [a: ∅]
dog: [b: ∅]
        "#,
        expect![[r#"
            SequenceSetup {
                cat: SetupState {
                    interests: FixedInterests(
                        [
                            RangeOpen {
                                start: Bytes(
                                    "",
                                ),
                                end: Bytes(
                                    "0xFF",
                                ),
                            },
                        ],
                    ),
                    state: {
                        Bytes(
                            "a",
                        ): None,
                    },
                },
                dog: SetupState {
                    interests: FixedInterests(
                        [
                            RangeOpen {
                                start: Bytes(
                                    "",
                                ),
                                end: Bytes(
                                    "0xFF",
                                ),
                            },
                        ],
                    ),
                    state: {
                        Bytes(
                            "b",
                        ): None,
                    },
                },
            }
        "#]],
    )
}

#[pin_project]
struct DuplexChannel<In, Out> {
    #[pin]
    sender: PollSender<In>,
    #[pin]
    receiver: ReceiverStream<Out>,
}

impl<In, Out> Sink<In> for DuplexChannel<In, Out>
where
    In: Send + 'static,
{
    type Error = <PollSender<In> as Sink<In>>::Error;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        let this = self.project();
        this.sender.poll_ready(cx)
    }

    fn start_send(
        self: std::pin::Pin<&mut Self>,
        item: In,
    ) -> std::result::Result<(), Self::Error> {
        let this = self.project();
        this.sender.start_send(item)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        let this = self.project();
        this.sender.poll_flush(cx)
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        let this = self.project();
        this.sender.poll_close(cx)
    }
}

impl<In, Out> Stream for DuplexChannel<In, Out>
where
    In: Send + 'static,
    Out: Send + 'static,
{
    type Item = std::result::Result<Out, <PollSender<In> as Sink<In>>::Error>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        this.receiver.poll_next(cx).map(|o| o.map(|out| Ok(out)))
    }
}

impl<T> StreamInspectExt for T
where
    T: Stream,
    T: ?Sized,
{
}

trait StreamInspectExt: Stream {
    fn inspect_async<F, Fut>(self, f: F) -> InspectAsync<Self, Fut, F>
    where
        F: FnMut(Self::Item) -> Fut,
        Fut: Future<Output = Self::Item>,
        Self: Sized,
    {
        InspectAsync::new(self, f)
    }
}

#[pin_project]
#[must_use = "futures do nothing unless you `.await` or poll them"]
struct InspectAsync<St, Fut, F>
where
    St: Stream,
{
    #[pin]
    stream: St,
    f: F,
    #[pin]
    next: Option<Fut>,
}

impl<St, Fut, F> InspectAsync<St, Fut, F>
where
    St: Stream,
    F: FnMut(St::Item) -> Fut,
    Fut: Future<Output = St::Item>,
{
    pub(super) fn new(stream: St, f: F) -> Self {
        Self {
            stream,
            f,
            next: None,
        }
    }
}

impl<St, Fut, F> Stream for InspectAsync<St, Fut, F>
where
    St: Stream,
    F: FnMut(St::Item) -> Fut,
    Fut: Future<Output = St::Item>,
{
    type Item = St::Item;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            if let Some(fut) = this.next.as_mut().as_pin_mut() {
                let item = ready!(fut.poll(cx));
                this.next.set(None);
                return std::task::Poll::Ready(Some(item));
            } else if let Some(item) = ready!(this.stream.as_mut().poll_next(cx)) {
                this.next.set(Some((this.f)(item)));
            } else {
                return std::task::Poll::Ready(None);
            }
        }
    }
}

impl<St, Fut, F, T> Sink<T> for InspectAsync<St, Fut, F>
where
    St: Stream + Sink<T>,
    F: FnMut(St::Item) -> Fut,
    Fut: Future<Output = St::Item>,
{
    type Error = <St as Sink<T>>::Error;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        let this = self.project();
        this.stream.poll_ready(cx)
    }

    fn start_send(self: std::pin::Pin<&mut Self>, item: T) -> std::result::Result<(), Self::Error> {
        let this = self.project();
        this.stream.start_send(item)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        let this = self.project();
        this.stream.poll_flush(cx)
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        let this = self.project();
        this.stream.poll_close(cx)
    }
}

fn duplex<T, U>(max_buf_size: usize) -> (DuplexChannel<T, U>, DuplexChannel<U, T>)
where
    T: Send + 'static,
    U: Send + 'static,
{
    let (tx_t, rx_t) = channel(max_buf_size);
    let (tx_u, rx_u) = channel(max_buf_size);
    (
        DuplexChannel {
            sender: PollSender::new(tx_t),
            receiver: ReceiverStream::new(rx_u),
        },
        DuplexChannel {
            sender: PollSender::new(tx_u),
            receiver: ReceiverStream::new(rx_t),
        },
    )
}

// Run the recon simulation
async fn recon_do(recon: &str) -> Sequence<AlphaNumBytes, MemoryAHash> {
    async fn snapshot_state(
        client: Client<AlphaNumBytes, MemoryAHash>,
    ) -> Result<BTreeMap<AlphaNumBytes, Option<AlphaNumBytes>>> {
        let mut state = BTreeMap::new();
        let keys: Vec<AlphaNumBytes> = client.full_range().await?.collect();
        for key in keys {
            let value = client.value_for_key(key.clone()).await?;
            state.insert(key, value.map(AlphaNumBytes::from));
        }
        Ok(state)
    }

    let setup = parse_sequence(recon);

    let cat = start_recon(from_setup_state(setup.cat.clone()).await);
    let dog = start_recon(from_setup_state(setup.dog.clone()).await);

    let steps = Arc::new(std::sync::Mutex::new(Vec::<
        SequenceStep<AlphaNumBytes, MemoryAHash>,
    >::new()));

    type InitiatorEnv = ReconMessage<InitiatorMessage<AlphaNumBytes, MemoryAHash>>;
    type ResponderEnv = ReconMessage<ResponderMessage<AlphaNumBytes, MemoryAHash>>;

    let (cat_channel, dog_channel): (
        DuplexChannel<InitiatorEnv, ResponderEnv>,
        DuplexChannel<ResponderEnv, InitiatorEnv>,
    ) = duplex(100);

    // Setup logic to capture the sequence of messages exchanged
    let cat_channel = {
        let steps = steps.clone();
        let dog = dog.clone();
        cat_channel.inspect_async(move |message| {
            let steps = steps.clone();
            let dog = dog.clone();
            async move {
                let state = snapshot_state(dog).await.unwrap();
                steps.lock().unwrap().push(SequenceStep {
                    message: Message::DogToCat(message.as_ref().unwrap().body.clone()),
                    state,
                });
                message
            }
        })
    };
    let dog_channel = {
        let steps = steps.clone();
        let cat = cat.clone();
        dog_channel.inspect_async(move |message| {
            let cat = cat.clone();
            let steps = steps.clone();
            async move {
                let state = snapshot_state(cat).await.unwrap();
                steps.lock().unwrap().push(SequenceStep {
                    message: Message::CatToDog(message.as_ref().unwrap().body.clone()),
                    state,
                });
                message
            }
        })
    };

    let cat_fut = protocol::initiate_synchronize(cat.clone(), cat_channel);
    let dog_fut = protocol::respond_synchronize(dog.clone(), dog_channel);
    // Drive both synchronize futures on the same thread
    // This is to ensure a deterministic behavior.
    let (cat_ret, dog_ret) = tokio::join!(cat_fut, dog_fut);

    // Error if either synchronize method errors
    cat_ret.unwrap();
    dog_ret.unwrap();

    let steps = Arc::try_unwrap(steps).unwrap().into_inner().unwrap();

    Sequence {
        setup,
        steps,
        r#final: SequenceFinal {
            cat: snapshot_state(cat).await.unwrap(),
            dog: snapshot_state(dog).await.unwrap(),
        },
    }
}

// A recon test is composed of a single expect value.
// The first two non empty lines of the expect value are parsed into the intial state of the nodes.
// Then synchronization is performed and the interaction captured and formated using Pretty.
// The rest of the expect value is that formatted sequence.
//
// This means that we only need to be able to parse the initial state data. The sequence steps can
// be as verbose or terse as needed without worrying about parsability.
async fn recon_test(recon: Expect) {
    let actual = format!("{}", recon_do(recon.data()).await);
    recon.assert_eq(&actual)
}

#[test(tokio::test)]
async fn abcde() {
    recon_test(expect![[r#"
        cat: [b: B, c: C, d: D, e: E]
        dog: [a: A, e: E]
        -> interest_req((𝚨, 𝛀 ))
            cat: [b: B, c: C, d: D, e: E]
        <- interest_resp((𝚨, 𝛀 ))
            dog: [a: A, e: E]
        -> range_req({𝚨 h(b, c, d, e)#4 𝛀 })
            cat: [b: B, c: C, d: D, e: E]
        <- range_resp({𝚨 0 a}, {a 0 e}, {e 0 𝛀 })
            dog: [a: A, e: E]
        -> value_resp(b: B)
            cat: [a: ∅, b: B, c: C, d: D, e: E]
        -> value_resp(c: C)
            cat: [a: ∅, b: B, c: C, d: D, e: E]
        -> value_resp(d: D)
            cat: [a: ∅, b: B, c: C, d: D, e: E]
        -> value_req(a)
            cat: [a: ∅, b: B, c: C, d: D, e: E]
        -> listen_only
            cat: [a: ∅, b: B, c: C, d: D, e: E]
        <- value_resp(a: A)
            dog: [a: A, b: B, c: C, d: D, e: E]
        <- listen_only
            dog: [a: A, b: B, c: C, d: D, e: E]
        -> finished
            cat: [a: A, b: B, c: C, d: D, e: E]
        cat: [a: A, b: B, c: C, d: D, e: E]
        dog: [a: A, b: B, c: C, d: D, e: E]
    "#]])
    .await
}

#[test(tokio::test)]
async fn two_in_a_row() {
    recon_test(expect![[r#"
        cat: [a: A, b: B, c: C, d: D, e: E]
        dog: [a: A, d: D, e: E]
        -> interest_req((𝚨, 𝛀 ))
            cat: [a: A, b: B, c: C, d: D, e: E]
        <- interest_resp((𝚨, 𝛀 ))
            dog: [a: A, d: D, e: E]
        -> range_req({𝚨 h(a, b, c, d, e)#5 𝛀 })
            cat: [a: A, b: B, c: C, d: D, e: E]
        <- range_resp({𝚨 0 a}, {a 0 d}, {d 0 e}, {e 0 𝛀 })
            dog: [a: A, d: D, e: E]
        -> value_resp(b: B)
            cat: [a: A, b: B, c: C, d: D, e: E]
        -> value_resp(c: C)
            cat: [a: A, b: B, c: C, d: D, e: E]
        -> listen_only
            cat: [a: A, b: B, c: C, d: D, e: E]
        <- listen_only
            dog: [a: A, b: B, c: C, d: D, e: E]
        -> finished
            cat: [a: A, b: B, c: C, d: D, e: E]
        cat: [a: A, b: B, c: C, d: D, e: E]
        dog: [a: A, b: B, c: C, d: D, e: E]
    "#]])
    .await
}

#[test(tokio::test)]
async fn disjoint() {
    recon_test(expect![[r#"
        cat: [a: A, b: B, c: C]
        dog: [e: E, f: F, g: G]
        -> interest_req((𝚨, 𝛀 ))
            cat: [a: A, b: B, c: C]
        <- interest_resp((𝚨, 𝛀 ))
            dog: [e: E, f: F, g: G]
        -> range_req({𝚨 h(a, b, c)#3 𝛀 })
            cat: [a: A, b: B, c: C]
        <- range_resp({𝚨 0 e}, {e 0 f}, {f 0 g}, {g 0 𝛀 })
            dog: [e: E, f: F, g: G]
        -> value_resp(a: A)
            cat: [a: A, b: B, c: C, e: ∅]
        -> value_resp(b: B)
            cat: [a: A, b: B, c: C, e: ∅, f: ∅, g: ∅]
        -> value_resp(c: C)
            cat: [a: A, b: B, c: C, e: ∅, f: ∅, g: ∅]
        -> value_req(e)
            cat: [a: A, b: B, c: C, e: ∅, f: ∅, g: ∅]
        -> value_req(f)
            cat: [a: A, b: B, c: C, e: ∅, f: ∅, g: ∅]
        <- value_resp(e: E)
            dog: [a: A, b: B, c: C, e: E, f: F, g: G]
        -> value_req(g)
            cat: [a: A, b: B, c: C, e: E, f: ∅, g: ∅]
        <- value_resp(f: F)
            dog: [a: A, b: B, c: C, e: E, f: F, g: G]
        -> listen_only
            cat: [a: A, b: B, c: C, e: E, f: F, g: ∅]
        <- value_resp(g: G)
            dog: [a: A, b: B, c: C, e: E, f: F, g: G]
        <- listen_only
            dog: [a: A, b: B, c: C, e: E, f: F, g: G]
        -> finished
            cat: [a: A, b: B, c: C, e: E, f: F, g: G]
        cat: [a: A, b: B, c: C, e: E, f: F, g: G]
        dog: [a: A, b: B, c: C, e: E, f: F, g: G]
    "#]])
    .await
}

#[test(tokio::test)]
async fn one_cat() {
    // if there is only one key it is its own message
    recon_test(expect![[r#"
        cat: [a: A]
        dog: []
        -> interest_req((𝚨, 𝛀 ))
            cat: [a: A]
        <- interest_resp((𝚨, 𝛀 ))
            dog: []
        -> range_req({𝚨 h(a)#1 𝛀 })
            cat: [a: A]
        <- range_resp({𝚨 0 𝛀 })
            dog: []
        -> value_resp(a: A)
            cat: [a: A]
        -> listen_only
            cat: [a: A]
        <- listen_only
            dog: [a: A]
        -> finished
            cat: [a: A]
        cat: [a: A]
        dog: [a: A]
    "#]])
    .await
}

#[test(tokio::test)]
async fn one_dog() {
    recon_test(expect![[r#"
        cat: []
        dog: [a: A]
        -> interest_req((𝚨, 𝛀 ))
            cat: []
        <- interest_resp((𝚨, 𝛀 ))
            dog: [a: A]
        -> range_req({𝚨 0 𝛀 })
            cat: []
        <- value_resp(a: A)
            dog: [a: A]
        <- range_resp({𝚨 h(a)#1 𝛀 })
            dog: [a: A]
        -> listen_only
            cat: [a: A]
        <- listen_only
            dog: [a: A]
        -> finished
            cat: [a: A]
        cat: [a: A]
        dog: [a: A]
    "#]])
    .await
}

#[test(tokio::test)]
async fn none() {
    recon_test(expect![[r#"
        cat: []
        dog: []
        -> interest_req((𝚨, 𝛀 ))
            cat: []
        <- interest_resp((𝚨, 𝛀 ))
            dog: []
        -> range_req({𝚨 0 𝛀 })
            cat: []
        <- range_resp({𝚨 0 𝛀 })
            dog: []
        -> listen_only
            cat: []
        <- listen_only
            dog: []
        -> finished
            cat: []
        cat: []
        dog: []
    "#]])
    .await
}

#[test(tokio::test)]
async fn two_in_sync() {
    recon_test(expect![[r#"
        cat: [a: A, z: Z]
        dog: [a: A, z: Z]
        -> interest_req((𝚨, 𝛀 ))
            cat: [a: A, z: Z]
        <- interest_resp((𝚨, 𝛀 ))
            dog: [a: A, z: Z]
        -> range_req({𝚨 h(a, z)#2 𝛀 })
            cat: [a: A, z: Z]
        <- range_resp({𝚨 h(a, z)#2 𝛀 })
            dog: [a: A, z: Z]
        -> listen_only
            cat: [a: A, z: Z]
        <- listen_only
            dog: [a: A, z: Z]
        -> finished
            cat: [a: A, z: Z]
        cat: [a: A, z: Z]
        dog: [a: A, z: Z]
    "#]])
    .await
}

#[test(tokio::test)]
async fn dog_missing_sync() {
    recon_test(expect![[r#"
        cat: [a: A, b: B, c: C]
        dog: [a: ∅, b: ∅, c: ∅]
        -> interest_req((𝚨, 𝛀 ))
            cat: [a: A, b: B, c: C]
        <- interest_resp((𝚨, 𝛀 ))
            dog: [a: ∅, b: ∅, c: ∅]
        -> range_req({𝚨 h(a, b, c)#3 𝛀 })
            cat: [a: A, b: B, c: C]
        <- range_resp({𝚨 h(a, b, c)#3 𝛀 })
            dog: [a: ∅, b: ∅, c: ∅]
        <- value_req(a)
            dog: [a: ∅, b: ∅, c: ∅]
        -> listen_only
            cat: [a: A, b: B, c: C]
        <- value_req(b)
            dog: [a: ∅, b: ∅, c: ∅]
        -> value_resp(a: A)
            cat: [a: A, b: B, c: C]
        <- value_req(c)
            dog: [a: A, b: ∅, c: ∅]
        -> value_resp(b: B)
            cat: [a: A, b: B, c: C]
        <- listen_only
            dog: [a: A, b: B, c: ∅]
        -> value_resp(c: C)
            cat: [a: A, b: B, c: C]
        -> finished
            cat: [a: A, b: B, c: C]
        cat: [a: A, b: B, c: C]
        dog: [a: A, b: B, c: C]
    "#]])
    .await;
}

#[test(tokio::test)]
async fn cat_missing_sync() {
    recon_test(expect![[r#"
        cat: [a: ∅, b: ∅, c: ∅]
        dog: [a: A, b: B, c: C]
        -> interest_req((𝚨, 𝛀 ))
            cat: [a: ∅, b: ∅, c: ∅]
        <- interest_resp((𝚨, 𝛀 ))
            dog: [a: A, b: B, c: C]
        -> range_req({𝚨 h(a, b, c)#3 𝛀 })
            cat: [a: ∅, b: ∅, c: ∅]
        <- range_resp({𝚨 h(a, b, c)#3 𝛀 })
            dog: [a: A, b: B, c: C]
        -> value_req(a)
            cat: [a: ∅, b: ∅, c: ∅]
        -> value_req(b)
            cat: [a: ∅, b: ∅, c: ∅]
        <- value_resp(a: A)
            dog: [a: A, b: B, c: C]
        -> value_req(c)
            cat: [a: A, b: ∅, c: ∅]
        <- value_resp(b: B)
            dog: [a: A, b: B, c: C]
        -> listen_only
            cat: [a: A, b: B, c: ∅]
        <- value_resp(c: C)
            dog: [a: A, b: B, c: C]
        <- listen_only
            dog: [a: A, b: B, c: C]
        -> finished
            cat: [a: A, b: B, c: C]
        cat: [a: A, b: B, c: C]
        dog: [a: A, b: B, c: C]
    "#]])
    .await;
}

#[test(tokio::test)]
async fn paper() {
    recon_test(expect![[r#"
        cat: [ape: APE, eel: EEL, fox: FOX, gnu: GNU]
        dog: [bee: BEE, cot: COT, doe: DOE, eel: EEL, fox: FOX, hog: HOG]
        -> interest_req((𝚨, 𝛀 ))
            cat: [ape: APE, eel: EEL, fox: FOX, gnu: GNU]
        <- interest_resp((𝚨, 𝛀 ))
            dog: [bee: BEE, cot: COT, doe: DOE, eel: EEL, fox: FOX, hog: HOG]
        -> range_req({𝚨 h(ape, eel, fox, gnu)#4 𝛀 })
            cat: [ape: APE, eel: EEL, fox: FOX, gnu: GNU]
        <- range_resp({𝚨 h(bee, cot)#2 doe}, {doe h(eel, fox, hog)#3 𝛀 })
            dog: [bee: BEE, cot: COT, doe: DOE, eel: EEL, fox: FOX, hog: HOG]
        -> range_req({𝚨 0 ape})
            cat: [ape: APE, doe: ∅, eel: EEL, fox: FOX, gnu: GNU]
        -> range_req({ape 0 doe})
            cat: [ape: APE, doe: ∅, eel: EEL, fox: FOX, gnu: GNU]
        <- range_resp({𝚨 0 ape})
            dog: [ape: ∅, bee: BEE, cot: COT, doe: DOE, eel: EEL, fox: FOX, hog: HOG]
        -> range_req({doe 0 eel})
            cat: [ape: APE, doe: ∅, eel: EEL, fox: FOX, gnu: GNU]
        <- value_req(ape)
            dog: [ape: ∅, bee: BEE, cot: COT, doe: DOE, eel: EEL, fox: FOX, hog: HOG]
        -> range_req({eel 0 fox})
            cat: [ape: APE, doe: ∅, eel: EEL, fox: FOX, gnu: GNU]
        <- value_resp(bee: BEE)
            dog: [ape: ∅, bee: BEE, cot: COT, doe: DOE, eel: EEL, fox: FOX, hog: HOG]
        -> range_req({fox 0 gnu})
            cat: [ape: APE, doe: ∅, eel: EEL, fox: FOX, gnu: GNU]
        <- value_resp(cot: COT)
            dog: [ape: ∅, bee: BEE, cot: COT, doe: DOE, eel: EEL, fox: FOX, hog: HOG]
        -> range_req({gnu 0 𝛀 })
            cat: [ape: APE, bee: BEE, doe: ∅, eel: EEL, fox: FOX, gnu: GNU]
        <- range_resp({ape h(bee, cot)#2 doe})
            dog: [ape: ∅, bee: BEE, cot: COT, doe: DOE, eel: EEL, fox: FOX, gnu: ∅, hog: HOG]
        -> value_req(doe)
            cat: [ape: APE, bee: BEE, cot: COT, doe: ∅, eel: EEL, fox: FOX, gnu: GNU]
        <- range_resp({doe 0 eel})
            dog: [ape: ∅, bee: BEE, cot: COT, doe: DOE, eel: EEL, fox: FOX, gnu: ∅, hog: HOG]
        -> value_resp(ape: APE)
            cat: [ape: APE, bee: BEE, cot: COT, doe: ∅, eel: EEL, fox: FOX, gnu: GNU]
        <- range_resp({eel 0 fox})
            dog: [ape: ∅, bee: BEE, cot: COT, doe: DOE, eel: EEL, fox: FOX, gnu: ∅, hog: HOG]
        <- range_resp({fox 0 gnu})
            dog: [ape: APE, bee: BEE, cot: COT, doe: DOE, eel: EEL, fox: FOX, gnu: ∅, hog: HOG]
        <- value_req(gnu)
            dog: [ape: APE, bee: BEE, cot: COT, doe: DOE, eel: EEL, fox: FOX, gnu: ∅, hog: HOG]
        <- value_resp(hog: HOG)
            dog: [ape: APE, bee: BEE, cot: COT, doe: DOE, eel: EEL, fox: FOX, gnu: ∅, hog: HOG]
        -> value_resp(gnu: GNU)
            cat: [ape: APE, bee: BEE, cot: COT, doe: ∅, eel: EEL, fox: FOX, gnu: GNU]
        <- range_resp({gnu h(hog)#1 𝛀 })
            dog: [ape: APE, bee: BEE, cot: COT, doe: DOE, eel: EEL, fox: FOX, gnu: GNU, hog: HOG]
        <- value_resp(doe: DOE)
            dog: [ape: APE, bee: BEE, cot: COT, doe: DOE, eel: EEL, fox: FOX, gnu: GNU, hog: HOG]
        -> listen_only
            cat: [ape: APE, bee: BEE, cot: COT, doe: ∅, eel: EEL, fox: FOX, gnu: GNU, hog: HOG]
        <- listen_only
            dog: [ape: APE, bee: BEE, cot: COT, doe: DOE, eel: EEL, fox: FOX, gnu: GNU, hog: HOG]
        -> finished
            cat: [ape: APE, bee: BEE, cot: COT, doe: DOE, eel: EEL, fox: FOX, gnu: GNU, hog: HOG]
        cat: [ape: APE, bee: BEE, cot: COT, doe: DOE, eel: EEL, fox: FOX, gnu: GNU, hog: HOG]
        dog: [ape: APE, bee: BEE, cot: COT, doe: DOE, eel: EEL, fox: FOX, gnu: GNU, hog: HOG]
    "#]])
    .await;
}

#[test(tokio::test)]
async fn small_diff() {
    recon_test(expect![[r#"
        cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> interest_req((𝚨, 𝛀 ))
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        <- interest_resp((𝚨, 𝛀 ))
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> range_req({𝚨 h(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z)#26 𝛀 })
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        <- range_resp({𝚨 h(a, b, c, d, e, f, g, h, i, j, k)#11 l}, {l h(m, o, p, q, r, s, t, u, w, x, y, z)#12 𝛀 })
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> range_req({l h(m, n, o, p, q, r)#6 s})
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        <- range_resp({l h(m, o)#2 p}, {p h(q, r)#2 s})
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> range_req({s h(t, u, v, w, x, y, z)#7 𝛀 })
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        <- range_resp({s h(t, u)#2 w}, {w h(x, y, z)#3 𝛀 })
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> range_req({l 0 m})
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        <- range_resp({l 0 m})
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> range_req({m 0 n})
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        -> range_req({n 0 o})
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        <- range_resp({m 0 n})
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: ∅, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        <- value_req(n)
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: ∅, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> range_req({o 0 p})
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        <- range_resp({n 0 o})
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: ∅, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> range_req({s 0 t})
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        <- range_resp({o 0 p})
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: ∅, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> range_req({t 0 u})
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        <- range_resp({s 0 t})
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: ∅, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> range_req({u 0 v})
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        <- range_resp({t 0 u})
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: ∅, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> range_req({v 0 w})
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        <- range_resp({u 0 v})
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: ∅, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: ∅, w: W, x: X, y: Y, z: Z]
        -> value_resp(n: N)
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        <- value_req(v)
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: ∅, w: W, x: X, y: Y, z: Z]
        <- range_resp({v 0 w})
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: ∅, w: W, x: X, y: Y, z: Z]
        -> value_resp(v: V)
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        -> listen_only
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        <- listen_only
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        -> finished
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
    "#]]).await;
}

#[test(tokio::test)]
async fn small_diff_off_by_one() {
    recon_test(expect![[r#"
        cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> interest_req((𝚨, 𝛀 ))
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        <- interest_resp((𝚨, 𝛀 ))
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> range_req({𝚨 h(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z)#26 𝛀 })
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        <- range_resp({𝚨 h(a, b, c, d, e, f, g, h, i, j, k)#11 l}, {l h(m, n, p, q, r, s, t, u, w, x, y, z)#12 𝛀 })
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> range_req({l h(m, n, o, p, q, r)#6 s})
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        <- range_resp({l h(m, n)#2 p}, {p h(q, r)#2 s})
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> range_req({s h(t, u, v, w, x, y, z)#7 𝛀 })
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        <- range_resp({s h(t, u)#2 w}, {w h(x, y, z)#3 𝛀 })
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> range_req({l 0 m})
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        <- range_resp({l 0 m})
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> range_req({m 0 n})
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        <- range_resp({m 0 n})
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> range_req({n 0 o})
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        -> range_req({o 0 p})
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        <- range_resp({n 0 o})
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: ∅, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        <- value_req(o)
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: ∅, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> range_req({s 0 t})
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        <- range_resp({o 0 p})
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: ∅, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> range_req({t 0 u})
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        <- range_resp({s 0 t})
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: ∅, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> range_req({u 0 v})
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        <- range_resp({t 0 u})
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: ∅, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> range_req({v 0 w})
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        <- range_resp({u 0 v})
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: ∅, p: P, q: Q, r: R, s: S, t: T, u: U, v: ∅, w: W, x: X, y: Y, z: Z]
        -> value_resp(o: O)
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        <- value_req(v)
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: ∅, w: W, x: X, y: Y, z: Z]
        -> value_resp(v: V)
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        <- range_resp({v 0 w})
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: ∅, w: W, x: X, y: Y, z: Z]
        -> listen_only
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        <- listen_only
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        -> finished
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
    "#]]).await;
}

#[test(tokio::test)]
async fn alternating() {
    recon_test(expect![[r#"
        cat: [a: A, b: B, c: C, e: E, g: G, i: I, k: K, m: M, o: O, p: P, r: R, t: T, v: V, x: X, z: Z]
        dog: [a: A, c: C, d: D, f: F, h: H, j: J, l: L, n: N, p: P, q: Q, s: S, u: U, w: W, y: Y, z: Z]
        -> interest_req((𝚨, 𝛀 ))
            cat: [a: A, b: B, c: C, e: E, g: G, i: I, k: K, m: M, o: O, p: P, r: R, t: T, v: V, x: X, z: Z]
        <- interest_resp((𝚨, 𝛀 ))
            dog: [a: A, c: C, d: D, f: F, h: H, j: J, l: L, n: N, p: P, q: Q, s: S, u: U, w: W, y: Y, z: Z]
        -> range_req({𝚨 h(a, b, c, e, g, i, k, m, o, p, r, t, v, x, z)#15 𝛀 })
            cat: [a: A, b: B, c: C, e: E, g: G, i: I, k: K, m: M, o: O, p: P, r: R, t: T, v: V, x: X, z: Z]
        <- range_resp({𝚨 h(a, c, d, f, h, j, l)#7 n}, {n h(p, q, s, u, w, y, z)#7 𝛀 })
            dog: [a: A, c: C, d: D, f: F, h: H, j: J, l: L, n: N, p: P, q: Q, s: S, u: U, w: W, y: Y, z: Z]
        -> range_req({𝚨 h(a, b, c)#3 e})
            cat: [a: A, b: B, c: C, e: E, g: G, i: I, k: K, m: M, n: ∅, o: O, p: P, r: R, t: T, v: V, x: X, z: Z]
        -> range_req({e h(g, i, k, m)#4 n})
            cat: [a: A, b: B, c: C, e: E, g: G, i: I, k: K, m: M, n: ∅, o: O, p: P, r: R, t: T, v: V, x: X, z: Z]
        <- range_resp({𝚨 0 a}, {a 0 c}, {c 0 d}, {d 0 e})
            dog: [a: A, c: C, d: D, e: ∅, f: F, h: H, j: J, l: L, n: N, p: P, q: Q, s: S, u: U, w: W, y: Y, z: Z]
        -> range_req({n h(o, p, r)#3 t})
            cat: [a: A, b: B, c: C, e: E, g: G, i: I, k: K, m: M, n: ∅, o: O, p: P, r: R, t: T, v: V, x: X, z: Z]
        <- value_req(e)
            dog: [a: A, c: C, d: D, e: ∅, f: F, h: H, j: J, l: L, n: N, p: P, q: Q, s: S, u: U, w: W, y: Y, z: Z]
        -> range_req({t h(v, x, z)#3 𝛀 })
            cat: [a: A, b: B, c: C, d: ∅, e: E, g: G, i: I, k: K, m: M, n: ∅, o: O, p: P, r: R, t: T, v: V, x: X, z: Z]
        <- range_resp({e 0 f}, {f 0 h}, {h 0 j}, {j 0 l}, {l 0 n})
            dog: [a: A, c: C, d: D, e: ∅, f: F, h: H, j: J, l: L, n: N, p: P, q: Q, s: S, t: ∅, u: U, w: W, y: Y, z: Z]
        -> value_req(n)
            cat: [a: A, b: B, c: C, d: ∅, e: E, g: G, i: I, k: K, m: M, n: ∅, o: O, p: P, r: R, t: T, v: V, x: X, z: Z]
        <- range_resp({n 0 p}, {p 0 q}, {q 0 s}, {s 0 t})
            dog: [a: A, c: C, d: D, e: ∅, f: F, h: H, j: J, l: L, n: N, p: P, q: Q, s: S, t: ∅, u: U, w: W, y: Y, z: Z]
        -> value_resp(b: B)
            cat: [a: A, b: B, c: C, d: ∅, e: E, f: ∅, g: G, h: ∅, i: I, j: ∅, k: K, l: ∅, m: M, n: ∅, o: O, p: P, r: R, t: T, v: V, x: X, z: Z]
        <- value_req(t)
            dog: [a: A, b: B, c: C, d: D, e: ∅, f: F, h: H, j: J, l: L, n: N, p: P, q: Q, s: S, t: ∅, u: U, w: W, y: Y, z: Z]
        -> value_req(d)
            cat: [a: A, b: B, c: C, d: ∅, e: E, f: ∅, g: G, h: ∅, i: I, j: ∅, k: K, l: ∅, m: M, n: ∅, o: O, p: P, q: ∅, r: R, s: ∅, t: T, v: V, x: X, z: Z]
        <- range_resp({t 0 u}, {u 0 w}, {w 0 y}, {y 0 z}, {z 0 𝛀 })
            dog: [a: A, b: B, c: C, d: D, e: ∅, f: F, h: H, j: J, l: L, n: N, p: P, q: Q, s: S, t: ∅, u: U, w: W, y: Y, z: Z]
        -> value_resp(e: E)
            cat: [a: A, b: B, c: C, d: ∅, e: E, f: ∅, g: G, h: ∅, i: I, j: ∅, k: K, l: ∅, m: M, n: ∅, o: O, p: P, q: ∅, r: R, s: ∅, t: T, v: V, x: X, z: Z]
        <- value_resp(n: N)
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, h: H, j: J, l: L, n: N, p: P, q: Q, s: S, t: ∅, u: U, w: W, y: Y, z: Z]
        -> value_resp(g: G)
            cat: [a: A, b: B, c: C, d: ∅, e: E, f: ∅, g: G, h: ∅, i: I, j: ∅, k: K, l: ∅, m: M, n: ∅, o: O, p: P, q: ∅, r: R, s: ∅, t: T, u: ∅, v: V, w: ∅, x: X, y: ∅, z: Z]
        <- value_resp(d: D)
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, h: H, j: J, l: L, n: N, p: P, q: Q, s: S, t: ∅, u: U, w: W, y: Y, z: Z]
        -> value_resp(i: I)
            cat: [a: A, b: B, c: C, d: ∅, e: E, f: ∅, g: G, h: ∅, i: I, j: ∅, k: K, l: ∅, m: M, n: N, o: O, p: P, q: ∅, r: R, s: ∅, t: T, u: ∅, v: V, w: ∅, x: X, y: ∅, z: Z]
        -> value_resp(k: K)
            cat: [a: A, b: B, c: C, d: D, e: E, f: ∅, g: G, h: ∅, i: I, j: ∅, k: K, l: ∅, m: M, n: N, o: O, p: P, q: ∅, r: R, s: ∅, t: T, u: ∅, v: V, w: ∅, x: X, y: ∅, z: Z]
        -> value_resp(m: M)
            cat: [a: A, b: B, c: C, d: D, e: E, f: ∅, g: G, h: ∅, i: I, j: ∅, k: K, l: ∅, m: M, n: N, o: O, p: P, q: ∅, r: R, s: ∅, t: T, u: ∅, v: V, w: ∅, x: X, y: ∅, z: Z]
        -> value_req(f)
            cat: [a: A, b: B, c: C, d: D, e: E, f: ∅, g: G, h: ∅, i: I, j: ∅, k: K, l: ∅, m: M, n: N, o: O, p: P, q: ∅, r: R, s: ∅, t: T, u: ∅, v: V, w: ∅, x: X, y: ∅, z: Z]
        <- value_resp(f: F)
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, p: P, q: Q, s: S, t: ∅, u: U, w: W, y: Y, z: Z]
        -> value_req(h)
            cat: [a: A, b: B, c: C, d: D, e: E, f: ∅, g: G, h: ∅, i: I, j: ∅, k: K, l: ∅, m: M, n: N, o: O, p: P, q: ∅, r: R, s: ∅, t: T, u: ∅, v: V, w: ∅, x: X, y: ∅, z: Z]
        <- value_resp(h: H)
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, p: P, q: Q, s: S, t: ∅, u: U, w: W, y: Y, z: Z]
        -> value_req(j)
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: ∅, i: I, j: ∅, k: K, l: ∅, m: M, n: N, o: O, p: P, q: ∅, r: R, s: ∅, t: T, u: ∅, v: V, w: ∅, x: X, y: ∅, z: Z]
        <- value_resp(j: J)
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, p: P, q: Q, s: S, t: ∅, u: U, w: W, y: Y, z: Z]
        -> value_req(l)
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: ∅, k: K, l: ∅, m: M, n: N, o: O, p: P, q: ∅, r: R, s: ∅, t: T, u: ∅, v: V, w: ∅, x: X, y: ∅, z: Z]
        <- value_resp(l: L)
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, p: P, q: Q, s: S, t: ∅, u: U, w: W, y: Y, z: Z]
        -> value_resp(o: O)
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: ∅, m: M, n: N, o: O, p: P, q: ∅, r: R, s: ∅, t: T, u: ∅, v: V, w: ∅, x: X, y: ∅, z: Z]
        -> value_resp(r: R)
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: ∅, r: R, s: ∅, t: T, u: ∅, v: V, w: ∅, x: X, y: ∅, z: Z]
        -> value_req(q)
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: ∅, r: R, s: ∅, t: T, u: ∅, v: V, w: ∅, x: X, y: ∅, z: Z]
        <- value_resp(q: Q)
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: ∅, u: U, w: W, y: Y, z: Z]
        -> value_req(s)
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: ∅, r: R, s: ∅, t: T, u: ∅, v: V, w: ∅, x: X, y: ∅, z: Z]
        <- value_resp(s: S)
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: ∅, u: U, w: W, y: Y, z: Z]
        -> value_resp(t: T)
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: ∅, t: T, u: ∅, v: V, w: ∅, x: X, y: ∅, z: Z]
        -> value_resp(v: V)
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: ∅, v: V, w: ∅, x: X, y: ∅, z: Z]
        -> value_resp(x: X)
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: ∅, v: V, w: ∅, x: X, y: ∅, z: Z]
        -> value_req(u)
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: ∅, v: V, w: ∅, x: X, y: ∅, z: Z]
        <- value_resp(u: U)
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        -> value_req(w)
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: ∅, v: V, w: ∅, x: X, y: ∅, z: Z]
        <- value_resp(w: W)
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        -> value_req(y)
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: ∅, x: X, y: ∅, z: Z]
        <- value_resp(y: Y)
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        -> listen_only
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: ∅, z: Z]
        <- listen_only
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        -> finished
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
        dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z]
    "#]]).await;
}

#[test(tokio::test)]
async fn small_diff_zz() {
    recon_test(expect![[r#"
        cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z, zz: ZZ]
        dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> interest_req((𝚨, 𝛀 ))
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z, zz: ZZ]
        <- interest_resp((𝚨, 𝛀 ))
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> range_req({𝚨 h(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, zz)#27 𝛀 })
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z, zz: ZZ]
        <- range_resp({𝚨 h(a, b, c, d, e, f, g, h, i, j, k)#11 l}, {l h(m, n, p, q, r, s, t, u, w, x, y, z)#12 𝛀 })
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> range_req({l h(m, n, o, p, q, r, s)#7 t})
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z, zz: ZZ]
        <- range_resp({l h(m, n)#2 p}, {p h(q, r, s)#3 t})
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> range_req({t h(u, v, w, x, y, z, zz)#7 𝛀 })
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z, zz: ZZ]
        <- range_resp({t h(u, w)#2 x}, {x h(y, z)#2 𝛀 })
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> range_req({l 0 m})
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z, zz: ZZ]
        <- range_resp({l 0 m})
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> range_req({m 0 n})
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z, zz: ZZ]
        <- range_resp({m 0 n})
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> range_req({n 0 o})
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z, zz: ZZ]
        <- range_resp({n 0 o})
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: ∅, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> range_req({o 0 p})
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z, zz: ZZ]
        <- value_req(o)
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: ∅, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> range_req({t 0 u})
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z, zz: ZZ]
        <- range_resp({o 0 p})
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: ∅, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> range_req({u 0 v})
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z, zz: ZZ]
        <- range_resp({t 0 u})
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: ∅, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> range_req({v 0 w})
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z, zz: ZZ]
        <- range_resp({u 0 v})
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: ∅, p: P, q: Q, r: R, s: S, t: T, u: U, v: ∅, w: W, x: X, y: Y, z: Z]
        -> range_req({w 0 x})
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z, zz: ZZ]
        <- value_req(v)
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: ∅, p: P, q: Q, r: R, s: S, t: T, u: U, v: ∅, w: W, x: X, y: Y, z: Z]
        -> range_req({x 0 y})
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z, zz: ZZ]
        <- range_resp({v 0 w})
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: ∅, p: P, q: Q, r: R, s: S, t: T, u: U, v: ∅, w: W, x: X, y: Y, z: Z]
        -> range_req({y 0 z})
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z, zz: ZZ]
        <- range_resp({w 0 x})
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: ∅, p: P, q: Q, r: R, s: S, t: T, u: U, v: ∅, w: W, x: X, y: Y, z: Z]
        -> range_req({z 0 zz})
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z, zz: ZZ]
        <- range_resp({x 0 y})
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: ∅, p: P, q: Q, r: R, s: S, t: T, u: U, v: ∅, w: W, x: X, y: Y, z: Z]
        -> range_req({zz 0 𝛀 })
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z, zz: ZZ]
        <- range_resp({y 0 z})
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: ∅, p: P, q: Q, r: R, s: S, t: T, u: U, v: ∅, w: W, x: X, y: Y, z: Z, zz: ∅]
        -> value_resp(o: O)
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z, zz: ZZ]
        <- range_resp({z 0 zz})
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: ∅, w: W, x: X, y: Y, z: Z, zz: ∅]
        -> value_resp(v: V)
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z, zz: ZZ]
        <- value_req(zz)
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z, zz: ∅]
        <- range_resp({zz 0 𝛀 })
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z, zz: ∅]
        -> value_resp(zz: ZZ)
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z, zz: ZZ]
        -> listen_only
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z, zz: ZZ]
        <- listen_only
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z, zz: ZZ]
        -> finished
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z, zz: ZZ]
        cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z, zz: ZZ]
        dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z, zz: ZZ]
    "#]]).await;
}

#[test(tokio::test)]
async fn dog_linear_download() {
    recon_test(expect![[r#"
        cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G]
        dog: []
        -> interest_req((𝚨, 𝛀 ))
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G]
        <- interest_resp((𝚨, 𝛀 ))
            dog: []
        -> range_req({𝚨 h(a, b, c, d, e, f, g)#7 𝛀 })
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G]
        <- range_resp({𝚨 0 𝛀 })
            dog: []
        -> value_resp(a: A)
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G]
        -> value_resp(b: B)
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G]
        -> value_resp(c: C)
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G]
        -> value_resp(d: D)
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G]
        -> value_resp(e: E)
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G]
        -> value_resp(f: F)
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G]
        -> value_resp(g: G)
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G]
        -> listen_only
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G]
        <- listen_only
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G]
        -> finished
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G]
        cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G]
        dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G]
    "#]])
    .await
}
#[test(tokio::test)]
async fn cat_linear_download() {
    recon_test(expect![[r#"
        cat: []
        dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G]
        -> interest_req((𝚨, 𝛀 ))
            cat: []
        <- interest_resp((𝚨, 𝛀 ))
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G]
        -> range_req({𝚨 0 𝛀 })
            cat: []
        <- value_resp(a: A)
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G]
        <- value_resp(b: B)
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G]
        <- value_resp(c: C)
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G]
        <- value_resp(d: D)
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G]
        <- value_resp(e: E)
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G]
        <- value_resp(f: F)
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G]
        <- value_resp(g: G)
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G]
        <- range_resp({𝚨 h(a, b, c, d, e, f, g)#7 𝛀 })
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G]
        -> listen_only
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G]
        <- listen_only
            dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G]
        -> finished
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G]
        cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G]
        dog: [a: A, b: B, c: C, d: D, e: E, f: F, g: G]
    "#]])
    .await
}
#[test(tokio::test)]
async fn subset_interest() {
    recon_test(expect![[r#"
        cat: <(b, i), (m, r)> [c: C, f: F, g: G, r: R]
        dog: <(a, z)> [b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N]
        -> interest_req((b, i), (m, r))
            cat: [c: C, f: F, g: G, r: R]
        <- interest_resp((b, i), (m, r))
            dog: [b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N]
        -> range_req({b h(c, f, g)#3 i})
            cat: [c: C, f: F, g: G, r: R]
        -> range_req({m 0 r})
            cat: [c: C, f: F, g: G, r: R]
        <- range_resp({b h(c, d)#2 e}, {e h(f, g, h)#3 i})
            dog: [b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N]
        -> range_req({b 0 c})
            cat: [b: ∅, c: C, e: ∅, f: F, g: G, i: ∅, r: R]
        <- value_resp(n: N)
            dog: [b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, r: ∅]
        -> range_req({c 0 e})
            cat: [b: ∅, c: C, e: ∅, f: F, g: G, i: ∅, r: R]
        -> range_req({e 0 f})
            cat: [b: ∅, c: C, e: ∅, f: F, g: G, i: ∅, n: N, r: R]
        <- range_resp({m h(n)#1 r})
            dog: [b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, r: ∅]
        -> range_req({f 0 g})
            cat: [b: ∅, c: C, e: ∅, f: F, g: G, i: ∅, n: N, r: R]
        <- value_req(r)
            dog: [b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, r: ∅]
        -> range_req({g 0 i})
            cat: [b: ∅, c: C, e: ∅, f: F, g: G, i: ∅, m: ∅, n: N, r: R]
        -> value_req(b)
            cat: [b: ∅, c: C, e: ∅, f: F, g: G, i: ∅, m: ∅, n: N, r: R]
        <- range_resp({b 0 c})
            dog: [b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, r: ∅]
        -> value_req(e)
            cat: [b: ∅, c: C, e: ∅, f: F, g: G, i: ∅, m: ∅, n: N, r: R]
        <- value_resp(d: D)
            dog: [b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, r: ∅]
        -> value_req(i)
            cat: [b: ∅, c: C, e: ∅, f: F, g: G, i: ∅, m: ∅, n: N, r: R]
        <- range_resp({c h(d)#1 e})
            dog: [b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, r: ∅]
        -> value_req(m)
            cat: [b: ∅, c: C, d: D, e: ∅, f: F, g: G, i: ∅, m: ∅, n: N, r: R]
        -> value_resp(r: R)
            cat: [b: ∅, c: C, d: D, e: ∅, f: F, g: G, i: ∅, m: ∅, n: N, r: R]
        <- range_resp({e 0 f})
            dog: [b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, r: R]
        <- range_resp({f 0 g})
            dog: [b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, r: R]
        <- value_resp(h: H)
            dog: [b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, r: R]
        <- range_resp({g h(h)#1 i})
            dog: [b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, r: R]
        -> listen_only
            cat: [b: ∅, c: C, d: D, e: ∅, f: F, g: G, h: H, i: ∅, m: ∅, n: N, r: R]
        <- value_resp(b: B)
            dog: [b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, r: R]
        <- value_resp(e: E)
            dog: [b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, r: R]
        <- value_resp(i: I)
            dog: [b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, r: R]
        <- value_resp(m: M)
            dog: [b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, r: R]
        <- listen_only
            dog: [b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, r: R]
        -> finished
            cat: [b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, m: M, n: N, r: R]
        cat: [b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, m: M, n: N, r: R]
        dog: [b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, r: R]
    "#]])
    .await;
}

#[test(tokio::test)]
async fn partial_interest() {
    recon_test(expect![[r#"
        cat: <(b, g), (i, q)> [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, o: O, p: P, q: Q]
        dog: <(k, t), (u, z)> [j: J, k: K, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> interest_req((b, g), (i, q))
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, o: O, p: P, q: Q]
        <- interest_resp((k, q))
            dog: [j: J, k: K, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> range_req({k h(l, m, o, p)#4 q})
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, o: O, p: P, q: Q]
        <- range_resp({k 0 n}, {n 0 o}, {o 0 p}, {p 0 q})
            dog: [j: J, k: K, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> value_resp(l: L)
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: ∅, o: O, p: P, q: Q]
        -> value_resp(m: M)
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: ∅, o: O, p: P, q: Q]
        -> value_req(n)
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: ∅, o: O, p: P, q: Q]
        <- value_resp(n: N)
            dog: [j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> listen_only
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: ∅, o: O, p: P, q: Q]
        <- listen_only
            dog: [j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
        -> finished
            cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q]
        cat: [a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q]
        dog: [j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S, t: T, u: U, w: W, x: X, y: Y, z: Z]
    "#]])
    .await;
}
