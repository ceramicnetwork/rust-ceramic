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

use std::fmt::Display;
use std::{collections::BTreeSet, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use ceramic_core::{NodeId, RangeOpen};
use futures::{ready, Future, Sink, Stream};
use pin_project::pin_project;
use prometheus_client::registry::Registry;
use serde::{Deserialize, Serialize};
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
use expect_test::{expect, expect_file, Expect};
use lalrpop_util::ParseError;
use pretty::{Arena, DocAllocator, DocBuilder, Pretty};

use crate::libp2p::Recon as _;
use crate::protocol::ProtocolConfig;
use crate::{
    protocol::{self, InitiatorMessage, ReconMessage, ResponderMessage, Value},
    recon::{FullInterests, HashCount, InterestProvider, RangeHash, ReconItem},
    tests::AlphaNumBytes,
    AssociativeHash, BTreeStore, Key, Metrics, Recon, Result as ReconResult, Sha256a,
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

    async fn interests(&self) -> ReconResult<Vec<RangeOpen<Self::Key>>> {
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
    state: BTreeSet<K>,
}

async fn from_setup_state(setup: SetupState<AlphaNumBytes>) -> ReconMemoryBytes {
    Recon::new(
        BTreeStore::from_set(
            setup
                .state
                .into_iter()
                // Use key as value for tests
                .map(|k| (k.clone(), k.into_inner()))
                .collect(),
        )
        .await,
        setup.interests,
        Metrics::register(&mut Registry::default()),
    )
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
    state: BTreeSet<K>,
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
                    InitiatorMessage::Value(vr) => dir.append(
                        allocator
                            .text("value")
                            .append(PrettyValueResponse(vr).pretty(allocator).parens()),
                    ),
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
                    ResponderMessage::Value(vr) => dir.append(
                        allocator
                            .text("value")
                            .append(PrettyValueResponse(vr).pretty(allocator).parens()),
                    ),
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct SequenceFinal<K: Key> {
    cat: BTreeSet<K>,
    dog: BTreeSet<K>,
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

struct PrettyRange<'a, K, H>(pub &'a RangeHash<K, H>);

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

struct PrettyValueResponse<'a, K>(pub &'a Value<K>);

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

struct PrettySet<'a, K>(pub &'a BTreeSet<K>);

impl<'a, D, A, K> Pretty<'a, D, A> for PrettySet<'a, K>
where
    A: 'a + Clone,
    D: DocAllocator<'a, A>,
    D::Doc: Clone,
    K: std::fmt::Display,
{
    fn pretty(self, allocator: &'a D) -> DocBuilder<'a, D, A> {
        allocator
            .intersperse(
                self.0.iter().map(|k| allocator.text(k.to_string())),
                allocator.text(", "),
            )
            .brackets()
    }
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn word_lists() {
    async fn recon_from_string(s: &str) -> ReconBytes {
        let r = ReconBytes::new(
            BTreeStore::default(),
            FullInterests::default(),
            Metrics::register(&mut Registry::default()),
        );
        for key in s.split_whitespace().map(|s| s.to_string()) {
            if !s.is_empty() {
                r.insert(
                    vec![ReconItem::new(
                        key.as_bytes().into(),
                        key.to_uppercase().as_bytes().to_vec(),
                    )],
                    NodeId::random().unwrap().0,
                )
                .await
                .unwrap();
            }
        }
        r
    }
    let mut peers = vec![
        recon_from_string(include_str!("./testdata/bip_39.txt")).await,
        recon_from_string(include_str!("./testdata/eff_large_wordlist.txt")).await,
        recon_from_string(include_str!("./testdata/eff_short_wordlist_1.txt")).await,
        recon_from_string(include_str!("./testdata/eff_short_wordlist_2.txt")).await,
        recon_from_string(include_str!("./testdata/wordle_words5_big.txt")).await,
        recon_from_string(include_str!("./testdata/wordle_words5.txt")).await,
    ];

    let expected_hash =
        expect![["495BF24CE0DB5C33CE846ADCD6D9A87592E05324585D85059C3DC2113B500F79#21139"]];

    // This file can be independently validated using this bash one liner (from the repo root):
    //
    //    diff -Zu ./recon/src/recon/testdata/expected/all.txt <(cat ./recon/src/recon/testdata/*.txt | grep -v '^$' | LC_ALL=C sort -u)
    //
    //  And the count can be validated with:
    //
    //    cat ./recon/src/recon/testdata/*.txt | grep -v '^$' | LC_ALL=C sort -u | wc -l
    //
    //
    let expected_word_list = expect_file!["./testdata/expected/all.txt"];

    for peer in &mut peers {
        debug!(count = peer.len().await.unwrap(), "initial peer state");
    }

    let local = ReconBytes::new(
        BTreeStore::default(),
        FullInterests::default(),
        Metrics::register(&mut Registry::default()),
    );

    async fn sync_pair(local: ReconBytes, remote: ReconBytes) {
        type InitiatorEnv = ReconMessage<InitiatorMessage<AlphaNumBytes, Sha256a>>;
        type ResponderEnv = ReconMessage<ResponderMessage<AlphaNumBytes, Sha256a>>;

        let (local_channel, remote_channel): (
            DuplexChannel<InitiatorEnv, ResponderEnv>,
            DuplexChannel<ResponderEnv, InitiatorEnv>,
        ) = duplex(10000);

        // Spawn a task for each half to make things go quick, we do not care about determinism
        // here.
        let local_handle = tokio::spawn(protocol::initiate_synchronize(
            local,
            local_channel,
            ProtocolConfig::new(100, NodeId::random().unwrap().0),
        ));
        let remote_handle = tokio::spawn(protocol::respond_synchronize(
            remote,
            remote_channel,
            ProtocolConfig::new(100, NodeId::random().unwrap().0),
        ));
        // Error if either synchronize method errors
        let (local, remote) = tokio::join!(local_handle, remote_handle);
        local.unwrap().unwrap();
        remote.unwrap().unwrap();
    }
    async fn sync_all(local: ReconBytes, peers: &[ReconBytes]) {
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

    // First ensure all peers have the same actual result.
    let mut actual = None;
    for peer in all_peers.iter() {
        let full_range = peer
            .initial_range((AlphaNumBytes::min_value(), AlphaNumBytes::max_value()).into())
            .await
            .unwrap();
        let curr_hash = full_range.hash.to_string();
        let curr_word_list = peer
            .full_range()
            .await
            .unwrap()
            .map(|w| w.to_string())
            .collect::<Vec<String>>()
            .join("\n");
        if let Some((prev_hash, prev_word_list)) = actual {
            assert_eq!(prev_hash, curr_hash, "all peers should have the same hash");
            assert_eq!(
                prev_word_list, curr_word_list,
                "all peers should have the same word list"
            );
        }
        actual = Some((curr_hash, curr_word_list));
    }
    // Compare actual result from all peers to the expected value
    // We do this because updating an expected value in a loop is error prone as the first
    // iteration updates the file and the second iteration may update the file in an invalid way
    // since the expectation may have moved after the first iteration.
    let (actual_hash, actual_word_list) = actual.unwrap();
    expected_hash.assert_eq(&actual_hash);
    expected_word_list.assert_eq(&actual_word_list);
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
        cat: [a,b,c]
        dog: [e,f,g]
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
                        ),
                        Bytes(
                            "b",
                        ),
                        Bytes(
                            "c",
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
                        ),
                        Bytes(
                            "f",
                        ),
                        Bytes(
                            "g",
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
cat: [a]
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
        cat: <(𝚨,c)> [a,b,c]
        dog: <(b,f),(g,𝛀)> [e,f,g]
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
                        ),
                        Bytes(
                            "b",
                        ),
                        Bytes(
                            "c",
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
                        ),
                        Bytes(
                            "f",
                        ),
                        Bytes(
                            "g",
                        ),
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

async fn recon_do_batch_size(
    recon: &str,
    batch_size: usize,
) -> Sequence<AlphaNumBytes, MemoryAHash> {
    async fn snapshot_state(client: &ReconMemoryBytes) -> Result<BTreeSet<AlphaNumBytes>> {
        let mut state = BTreeSet::new();
        let keys: Vec<AlphaNumBytes> = client.full_range().await?.collect();
        for key in keys {
            state.insert(key);
        }
        Ok(state)
    }

    let setup = parse_sequence(recon);

    let cat = from_setup_state(setup.cat.clone()).await;
    let dog = from_setup_state(setup.dog.clone()).await;

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
                let state = snapshot_state(&dog).await.unwrap();
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
                let state = snapshot_state(&cat).await.unwrap();
                steps.lock().unwrap().push(SequenceStep {
                    message: Message::CatToDog(message.as_ref().unwrap().body.clone()),
                    state,
                });
                message
            }
        })
    };

    let cat_fut = protocol::initiate_synchronize(
        cat.clone(),
        cat_channel,
        ProtocolConfig::new(batch_size, NodeId::random().unwrap().0),
    );
    let dog_fut = protocol::respond_synchronize(
        dog.clone(),
        dog_channel,
        ProtocolConfig::new(batch_size, NodeId::random().unwrap().0),
    );
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
            cat: snapshot_state(&cat).await.unwrap(),
            dog: snapshot_state(&dog).await.unwrap(),
        },
    }
}

// Run the recon simulation
async fn recon_do(recon: &str) -> Sequence<AlphaNumBytes, MemoryAHash> {
    recon_do_batch_size(recon, 0).await
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

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn abcde() {
    recon_test(expect![[r#"
        cat: [b, c, d, e]
        dog: [a, e]
        -> interest_req((𝚨, 𝛀 ))
            cat: [b, c, d, e]
        <- interest_resp((𝚨, 𝛀 ))
            dog: [a, e]
        -> range_req({𝚨 h(b, c, d, e)#4 𝛀 })
            cat: [b, c, d, e]
        <- range_resp({𝚨 0 a}, {a h(a)#1 e}, {e h(e)#1 𝛀 })
            dog: [a, e]
        -> range_req({a 0 b})
            cat: [b, c, d, e]
        -> range_req({b h(b, c, d)#3 e})
            cat: [b, c, d, e]
        <- value(a: a)
            dog: [a, e]
        <- range_resp()
            dog: [a, e]
        <- range_resp({b 0 e})
            dog: [a, e]
        -> value(b: b)
            cat: [a, b, c, d, e]
        -> value(c: c)
            cat: [a, b, c, d, e]
        -> value(d: d)
            cat: [a, b, c, d, e]
        -> finished
            cat: [a, b, c, d, e]
        cat: [a, b, c, d, e]
        dog: [a, b, c, d, e]
    "#]])
    .await
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn two_in_a_row() {
    recon_test(expect![[r#"
        cat: [a, b, c, d, e]
        dog: [a, d, e]
        -> interest_req((𝚨, 𝛀 ))
            cat: [a, b, c, d, e]
        <- interest_resp((𝚨, 𝛀 ))
            dog: [a, d, e]
        -> range_req({𝚨 h(a, b, c, d, e)#5 𝛀 })
            cat: [a, b, c, d, e]
        <- range_resp({𝚨 0 a}, {a h(a)#1 d}, {d h(d)#1 e}, {e h(e)#1 𝛀 })
            dog: [a, d, e]
        -> value(a: a)
            cat: [a, b, c, d, e]
        -> value(b: b)
            cat: [a, b, c, d, e]
        -> value(c: c)
            cat: [a, b, c, d, e]
        -> finished
            cat: [a, b, c, d, e]
        cat: [a, b, c, d, e]
        dog: [a, b, c, d, e]
    "#]])
    .await
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn disjoint() {
    recon_test(expect![[r#"
        cat: [a, b, c]
        dog: [e, f, g]
        -> interest_req((𝚨, 𝛀 ))
            cat: [a, b, c]
        <- interest_resp((𝚨, 𝛀 ))
            dog: [e, f, g]
        -> range_req({𝚨 h(a, b, c)#3 𝛀 })
            cat: [a, b, c]
        <- range_resp({𝚨 0 e}, {e h(e)#1 f}, {f h(f)#1 g}, {g h(g)#1 𝛀 })
            dog: [e, f, g]
        -> value(a: a)
            cat: [a, b, c]
        -> value(b: b)
            cat: [a, b, c]
        -> value(c: c)
            cat: [a, b, c]
        -> range_req({e 0 f})
            cat: [a, b, c]
        -> range_req({f 0 g})
            cat: [a, b, c]
        -> range_req({g 0 𝛀 })
            cat: [a, b, c]
        <- value(e: e)
            dog: [a, b, c, e, f, g]
        <- range_resp()
            dog: [a, b, c, e, f, g]
        <- value(f: f)
            dog: [a, b, c, e, f, g]
        <- range_resp()
            dog: [a, b, c, e, f, g]
        <- value(g: g)
            dog: [a, b, c, e, f, g]
        <- range_resp()
            dog: [a, b, c, e, f, g]
        -> finished
            cat: [a, b, c, e, f, g]
        cat: [a, b, c, e, f, g]
        dog: [a, b, c, e, f, g]
    "#]])
    .await
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn disjoint_batch_size() {
    // this sends the same messages as disjoint but they arrive in a slightly different order
    let recon = expect![[r#"
        cat: [a, b, c]
        dog: [e, f, g]
        -> interest_req((𝚨, 𝛀 ))
            cat: [a, b, c]
        <- interest_resp((𝚨, 𝛀 ))
            dog: [e, f, g]
        -> range_req({𝚨 h(a, b, c)#3 𝛀 })
            cat: [a, b, c]
        <- range_resp({𝚨 0 e}, {e h(e)#1 f}, {f h(f)#1 g}, {g h(g)#1 𝛀 })
            dog: [e, f, g]
        -> value(a: a)
            cat: [a, b, c]
        -> value(b: b)
            cat: [a, b, c]
        -> value(c: c)
            cat: [a, b, c]
        -> range_req({e 0 f})
            cat: [a, b, c]
        -> range_req({f 0 g})
            cat: [a, b, c]
        -> range_req({g 0 𝛀 })
            cat: [a, b, c]
        <- value(e: e)
            dog: [e, f, g]
        <- range_resp()
            dog: [e, f, g]
        <- value(f: f)
            dog: [e, f, g]
        <- range_resp()
            dog: [e, f, g]
        <- value(g: g)
            dog: [e, f, g]
        <- range_resp()
            dog: [e, f, g]
        -> finished
            cat: [a, b, c, e, f, g]
        cat: [a, b, c, e, f, g]
        dog: [a, b, c, e, f, g]
    "#]];
    let actual = format!("{}", recon_do_batch_size(recon.data(), 10).await);
    recon.assert_eq(&actual)
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn one_cat() {
    // if there is only one key it is its own message
    recon_test(expect![[r#"
        cat: [a]
        dog: []
        -> interest_req((𝚨, 𝛀 ))
            cat: [a]
        <- interest_resp((𝚨, 𝛀 ))
            dog: []
        -> range_req({𝚨 h(a)#1 𝛀 })
            cat: [a]
        <- range_resp({𝚨 0 𝛀 })
            dog: []
        -> value(a: a)
            cat: [a]
        -> finished
            cat: [a]
        cat: [a]
        dog: [a]
    "#]])
    .await
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn one_dog() {
    recon_test(expect![[r#"
        cat: []
        dog: [a]
        -> interest_req((𝚨, 𝛀 ))
            cat: []
        <- interest_resp((𝚨, 𝛀 ))
            dog: [a]
        -> range_req({𝚨 0 𝛀 })
            cat: []
        <- value(a: a)
            dog: [a]
        <- range_resp()
            dog: [a]
        -> finished
            cat: [a]
        cat: [a]
        dog: [a]
    "#]])
    .await
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
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
        -> finished
            cat: []
        cat: []
        dog: []
    "#]])
    .await
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn two_in_sync() {
    recon_test(expect![[r#"
        cat: [a, z]
        dog: [a, z]
        -> interest_req((𝚨, 𝛀 ))
            cat: [a, z]
        <- interest_resp((𝚨, 𝛀 ))
            dog: [a, z]
        -> range_req({𝚨 h(a, z)#2 𝛀 })
            cat: [a, z]
        <- range_resp({𝚨 h(a, z)#2 𝛀 })
            dog: [a, z]
        -> finished
            cat: [a, z]
        cat: [a, z]
        dog: [a, z]
    "#]])
    .await
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn paper() {
    recon_test(expect![[r#"
        cat: [ape, eel, fox, gnu]
        dog: [bee, cot, doe, eel, fox, hog]
        -> interest_req((𝚨, 𝛀 ))
            cat: [ape, eel, fox, gnu]
        <- interest_resp((𝚨, 𝛀 ))
            dog: [bee, cot, doe, eel, fox, hog]
        -> range_req({𝚨 h(ape, eel, fox, gnu)#4 𝛀 })
            cat: [ape, eel, fox, gnu]
        <- range_resp({𝚨 h(bee, cot, doe)#3 eel}, {eel h(eel, fox, hog)#3 𝛀 })
            dog: [bee, cot, doe, eel, fox, hog]
        -> range_req({𝚨 0 ape})
            cat: [ape, eel, fox, gnu]
        -> range_req({ape h(ape)#1 eel})
            cat: [ape, eel, fox, gnu]
        -> range_req({eel h(eel)#1 fox})
            cat: [ape, eel, fox, gnu]
        -> range_req({fox h(fox)#1 gnu})
            cat: [ape, eel, fox, gnu]
        -> range_req({gnu h(gnu)#1 𝛀 })
            cat: [ape, eel, fox, gnu]
        <- range_resp({𝚨 0 ape})
            dog: [bee, cot, doe, eel, fox, hog]
        <- range_resp({ape 0 bee}, {bee h(bee, cot, doe)#3 eel})
            dog: [bee, cot, doe, eel, fox, hog]
        <- range_resp({eel h(eel)#1 fox})
            dog: [bee, cot, doe, eel, fox, hog]
        <- range_resp({fox h(fox)#1 gnu})
            dog: [bee, cot, doe, eel, fox, hog]
        <- range_resp({gnu 0 hog}, {hog h(hog)#1 𝛀 })
            dog: [bee, cot, doe, eel, fox, hog]
        -> value(ape: ape)
            cat: [ape, eel, fox, gnu]
        -> range_req({bee 0 eel})
            cat: [ape, eel, fox, gnu]
        -> value(gnu: gnu)
            cat: [ape, eel, fox, gnu]
        -> range_req({hog 0 𝛀 })
            cat: [ape, eel, fox, gnu]
        <- value(bee: bee)
            dog: [ape, bee, cot, doe, eel, fox, gnu, hog]
        <- value(cot: cot)
            dog: [ape, bee, cot, doe, eel, fox, gnu, hog]
        <- value(doe: doe)
            dog: [ape, bee, cot, doe, eel, fox, gnu, hog]
        <- range_resp()
            dog: [ape, bee, cot, doe, eel, fox, gnu, hog]
        <- value(hog: hog)
            dog: [ape, bee, cot, doe, eel, fox, gnu, hog]
        <- range_resp()
            dog: [ape, bee, cot, doe, eel, fox, gnu, hog]
        -> finished
            cat: [ape, bee, cot, doe, eel, fox, gnu, hog]
        cat: [ape, bee, cot, doe, eel, fox, gnu, hog]
        dog: [ape, bee, cot, doe, eel, fox, gnu, hog]
    "#]])
    .await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn small_diff() {
    recon_test(expect![[r#"
        cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, o, p, q, r, s, t, u, w, x, y, z]
        -> interest_req((𝚨, 𝛀 ))
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        <- interest_resp((𝚨, 𝛀 ))
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, o, p, q, r, s, t, u, w, x, y, z]
        -> range_req({𝚨 h(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z)#26 𝛀 })
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        <- range_resp({𝚨 h(a, b, c, d, e, f, g, h, i, j, k, l)#12 m}, {m h(m, o, p, q, r, s, t, u, w, x, y, z)#12 𝛀 })
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, o, p, q, r, s, t, u, w, x, y, z]
        -> range_req({m h(m, n, o, p, q, r, s)#7 t})
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        -> range_req({t h(t, u, v, w, x, y, z)#7 𝛀 })
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        <- range_resp({m h(m, o, p)#3 q}, {q h(q, r, s)#3 t})
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, o, p, q, r, s, t, u, w, x, y, z]
        <- range_resp({t h(t, u, w)#3 x}, {x h(x, y, z)#3 𝛀 })
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, o, p, q, r, s, t, u, w, x, y, z]
        -> range_req({m h(m)#1 n})
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        -> range_req({n h(n)#1 o})
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        -> range_req({o h(o)#1 p})
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        -> range_req({p h(p)#1 q})
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        -> range_req({t h(t)#1 u})
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        -> range_req({u h(u)#1 v})
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        -> range_req({v h(v)#1 w})
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        -> range_req({w h(w)#1 x})
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        <- range_resp({m h(m)#1 n})
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, o, p, q, r, s, t, u, w, x, y, z]
        <- range_resp({n 0 o})
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, o, p, q, r, s, t, u, w, x, y, z]
        <- range_resp({o h(o)#1 p})
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, o, p, q, r, s, t, u, w, x, y, z]
        <- range_resp({p h(p)#1 q})
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, o, p, q, r, s, t, u, w, x, y, z]
        <- range_resp({t h(t)#1 u})
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, o, p, q, r, s, t, u, w, x, y, z]
        <- range_resp({u h(u)#1 v})
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, o, p, q, r, s, t, u, w, x, y, z]
        <- range_resp({v 0 w})
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, o, p, q, r, s, t, u, w, x, y, z]
        <- range_resp({w h(w)#1 x})
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, o, p, q, r, s, t, u, w, x, y, z]
        -> value(n: n)
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        -> value(v: v)
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        -> finished
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
    "#]]).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn small_diff_off_by_one() {
    recon_test(expect![[r#"
        cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u, w, x, y, z]
        -> interest_req((𝚨, 𝛀 ))
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        <- interest_resp((𝚨, 𝛀 ))
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u, w, x, y, z]
        -> range_req({𝚨 h(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z)#26 𝛀 })
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        <- range_resp({𝚨 h(a, b, c, d, e, f, g, h, i, j, k, l)#12 m}, {m h(m, n, p, q, r, s, t, u, w, x, y, z)#12 𝛀 })
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u, w, x, y, z]
        -> range_req({m h(m, n, o, p, q, r, s)#7 t})
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        -> range_req({t h(t, u, v, w, x, y, z)#7 𝛀 })
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        <- range_resp({m h(m, n, p)#3 q}, {q h(q, r, s)#3 t})
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u, w, x, y, z]
        <- range_resp({t h(t, u, w)#3 x}, {x h(x, y, z)#3 𝛀 })
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u, w, x, y, z]
        -> range_req({m h(m)#1 n})
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        -> range_req({n h(n)#1 o})
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        -> range_req({o h(o)#1 p})
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        -> range_req({p h(p)#1 q})
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        -> range_req({t h(t)#1 u})
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        -> range_req({u h(u)#1 v})
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        -> range_req({v h(v)#1 w})
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        -> range_req({w h(w)#1 x})
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        <- range_resp({m h(m)#1 n})
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u, w, x, y, z]
        <- range_resp({n h(n)#1 o})
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u, w, x, y, z]
        <- range_resp({o 0 p})
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u, w, x, y, z]
        <- range_resp({p h(p)#1 q})
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u, w, x, y, z]
        <- range_resp({t h(t)#1 u})
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u, w, x, y, z]
        <- range_resp({u h(u)#1 v})
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u, w, x, y, z]
        <- range_resp({v 0 w})
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u, w, x, y, z]
        <- range_resp({w h(w)#1 x})
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u, w, x, y, z]
        -> value(o: o)
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        -> value(v: v)
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        -> finished
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
    "#]]).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn alternating() {
    recon_test(expect![[r#"
        cat: [a, b, c, e, g, i, k, m, o, p, r, t, v, x, z]
        dog: [a, c, d, f, h, j, l, n, p, q, s, u, w, y, z]
        -> interest_req((𝚨, 𝛀 ))
            cat: [a, b, c, e, g, i, k, m, o, p, r, t, v, x, z]
        <- interest_resp((𝚨, 𝛀 ))
            dog: [a, c, d, f, h, j, l, n, p, q, s, u, w, y, z]
        -> range_req({𝚨 h(a, b, c, e, g, i, k, m, o, p, r, t, v, x, z)#15 𝛀 })
            cat: [a, b, c, e, g, i, k, m, o, p, r, t, v, x, z]
        <- range_resp({𝚨 h(a, c, d, f, h, j, l)#7 n}, {n h(n, p, q, s, u, w, y, z)#8 𝛀 })
            dog: [a, c, d, f, h, j, l, n, p, q, s, u, w, y, z]
        -> range_req({𝚨 h(a, b, c, e)#4 g})
            cat: [a, b, c, e, g, i, k, m, o, p, r, t, v, x, z]
        -> range_req({g h(g, i, k, m)#4 n})
            cat: [a, b, c, e, g, i, k, m, o, p, r, t, v, x, z]
        -> range_req({n h(o, p, r)#3 t})
            cat: [a, b, c, e, g, i, k, m, o, p, r, t, v, x, z]
        -> range_req({t h(t, v, x, z)#4 𝛀 })
            cat: [a, b, c, e, g, i, k, m, o, p, r, t, v, x, z]
        <- range_resp({𝚨 0 a}, {a h(a)#1 c}, {c h(c)#1 d}, {d h(d)#1 f}, {f h(f)#1 g})
            dog: [a, c, d, f, h, j, l, n, p, q, s, u, w, y, z]
        <- range_resp({g 0 h}, {h h(h)#1 j}, {j h(j)#1 l}, {l h(l)#1 n})
            dog: [a, c, d, f, h, j, l, n, p, q, s, u, w, y, z]
        <- range_resp({n h(n)#1 p}, {p h(p)#1 q}, {q h(q)#1 s}, {s h(s)#1 t})
            dog: [a, c, d, f, h, j, l, n, p, q, s, u, w, y, z]
        <- range_resp({t 0 u}, {u h(u)#1 w}, {w h(w)#1 y}, {y h(y)#1 z}, {z h(z)#1 𝛀 })
            dog: [a, c, d, f, h, j, l, n, p, q, s, u, w, y, z]
        -> value(a: a)
            cat: [a, b, c, e, g, i, k, m, o, p, r, t, v, x, z]
        -> value(b: b)
            cat: [a, b, c, e, g, i, k, m, o, p, r, t, v, x, z]
        -> range_req({d 0 e})
            cat: [a, b, c, e, g, i, k, m, o, p, r, t, v, x, z]
        -> range_req({e h(e)#1 f})
            cat: [a, b, c, e, g, i, k, m, o, p, r, t, v, x, z]
        -> range_req({f 0 g})
            cat: [a, b, c, e, g, i, k, m, o, p, r, t, v, x, z]
        -> value(g: g)
            cat: [a, b, c, e, g, i, k, m, o, p, r, t, v, x, z]
        -> range_req({h 0 i})
            cat: [a, b, c, e, g, i, k, m, o, p, r, t, v, x, z]
        -> range_req({i h(i)#1 j})
            cat: [a, b, c, e, g, i, k, m, o, p, r, t, v, x, z]
        -> range_req({j 0 k})
            cat: [a, b, c, e, g, i, k, m, o, p, r, t, v, x, z]
        -> range_req({k h(k)#1 l})
            cat: [a, b, c, e, g, i, k, m, o, p, r, t, v, x, z]
        -> range_req({l 0 m})
            cat: [a, b, c, e, g, i, k, m, o, p, r, t, v, x, z]
        -> range_req({m h(m)#1 n})
            cat: [a, b, c, e, g, i, k, m, o, p, r, t, v, x, z]
        -> range_req({n 0 o})
            cat: [a, b, c, e, g, i, k, m, o, p, r, t, v, x, z]
        -> range_req({o h(o)#1 p})
            cat: [a, b, c, e, g, i, k, m, o, p, r, t, v, x, z]
        -> range_req({q 0 r})
            cat: [a, b, c, e, g, i, k, m, o, p, r, t, v, x, z]
        -> range_req({r h(r)#1 s})
            cat: [a, b, c, e, g, i, k, m, o, p, r, t, v, x, z]
        -> range_req({s 0 t})
            cat: [a, b, c, e, g, i, k, m, o, p, r, t, v, x, z]
        -> value(t: t)
            cat: [a, b, c, e, g, i, k, m, o, p, r, t, v, x, z]
        -> range_req({u 0 v})
            cat: [a, b, c, e, g, i, k, m, o, p, r, t, v, x, z]
        -> range_req({v h(v)#1 w})
            cat: [a, b, c, e, g, i, k, m, o, p, r, t, v, x, z]
        -> range_req({w 0 x})
            cat: [a, b, c, e, g, i, k, m, o, p, r, t, v, x, z]
        -> range_req({x h(x)#1 y})
            cat: [a, b, c, e, g, i, k, m, o, p, r, t, v, x, z]
        -> range_req({y 0 z})
            cat: [a, b, c, e, g, i, k, m, o, p, r, t, v, x, z]
        <- value(d: d)
            dog: [a, b, c, d, f, g, h, j, l, n, p, q, s, t, u, w, y, z]
        <- range_resp()
            dog: [a, b, c, d, f, g, h, j, l, n, p, q, s, t, u, w, y, z]
        <- range_resp({e 0 f})
            dog: [a, b, c, d, f, g, h, j, l, n, p, q, s, t, u, w, y, z]
        <- value(f: f)
            dog: [a, b, c, d, f, g, h, j, l, n, p, q, s, t, u, w, y, z]
        <- range_resp()
            dog: [a, b, c, d, f, g, h, j, l, n, p, q, s, t, u, w, y, z]
        <- value(h: h)
            dog: [a, b, c, d, f, g, h, j, l, n, p, q, s, t, u, w, y, z]
        <- range_resp()
            dog: [a, b, c, d, f, g, h, j, l, n, p, q, s, t, u, w, y, z]
        <- range_resp({i 0 j})
            dog: [a, b, c, d, f, g, h, j, l, n, p, q, s, t, u, w, y, z]
        <- value(j: j)
            dog: [a, b, c, d, f, g, h, j, l, n, p, q, s, t, u, w, y, z]
        <- range_resp()
            dog: [a, b, c, d, f, g, h, j, l, n, p, q, s, t, u, w, y, z]
        <- range_resp({k 0 l})
            dog: [a, b, c, d, f, g, h, j, l, n, p, q, s, t, u, w, y, z]
        <- value(l: l)
            dog: [a, b, c, d, f, g, h, j, l, n, p, q, s, t, u, w, y, z]
        <- range_resp()
            dog: [a, b, c, d, f, g, h, j, l, n, p, q, s, t, u, w, y, z]
        <- range_resp({m 0 n})
            dog: [a, b, c, d, f, g, h, j, l, n, p, q, s, t, u, w, y, z]
        <- value(n: n)
            dog: [a, b, c, d, f, g, h, j, l, n, p, q, s, t, u, w, y, z]
        <- range_resp()
            dog: [a, b, c, d, f, g, h, j, l, n, p, q, s, t, u, w, y, z]
        <- range_resp({o 0 p})
            dog: [a, b, c, d, f, g, h, j, l, n, p, q, s, t, u, w, y, z]
        <- value(q: q)
            dog: [a, b, c, d, f, g, h, j, l, n, p, q, s, t, u, w, y, z]
        <- range_resp()
            dog: [a, b, c, d, f, g, h, j, l, n, p, q, s, t, u, w, y, z]
        <- range_resp({r 0 s})
            dog: [a, b, c, d, f, g, h, j, l, n, p, q, s, t, u, w, y, z]
        <- value(s: s)
            dog: [a, b, c, d, f, g, h, j, l, n, p, q, s, t, u, w, y, z]
        <- range_resp()
            dog: [a, b, c, d, f, g, h, j, l, n, p, q, s, t, u, w, y, z]
        <- value(u: u)
            dog: [a, b, c, d, f, g, h, j, l, n, p, q, s, t, u, w, y, z]
        <- range_resp()
            dog: [a, b, c, d, f, g, h, j, l, n, p, q, s, t, u, w, y, z]
        <- range_resp({v 0 w})
            dog: [a, b, c, d, f, g, h, j, l, n, p, q, s, t, u, w, y, z]
        <- value(w: w)
            dog: [a, b, c, d, f, g, h, j, l, n, p, q, s, t, u, w, y, z]
        <- range_resp()
            dog: [a, b, c, d, f, g, h, j, l, n, p, q, s, t, u, w, y, z]
        <- range_resp({x 0 y})
            dog: [a, b, c, d, f, g, h, j, l, n, p, q, s, t, u, w, y, z]
        <- value(y: y)
            dog: [a, b, c, d, f, g, h, j, l, n, p, q, s, t, u, w, y, z]
        <- range_resp()
            dog: [a, b, c, d, f, g, h, j, l, n, p, q, s, t, u, w, y, z]
        -> value(e: e)
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        -> value(i: i)
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        -> value(k: k)
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        -> value(m: m)
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        -> value(o: o)
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        -> value(r: r)
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        -> value(v: v)
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        -> value(x: x)
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        -> finished
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
        dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]
    "#]])
    .await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn small_diff_zz() {
    recon_test(expect![[r#"
        cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, zz]
        dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u, w, x, y, z]
        -> interest_req((𝚨, 𝛀 ))
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, zz]
        <- interest_resp((𝚨, 𝛀 ))
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u, w, x, y, z]
        -> range_req({𝚨 h(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, zz)#27 𝛀 })
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, zz]
        <- range_resp({𝚨 h(a, b, c, d, e, f, g, h, i, j, k, l)#12 m}, {m h(m, n, p, q, r, s, t, u, w, x, y, z)#12 𝛀 })
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u, w, x, y, z]
        -> range_req({m h(m, n, o, p, q, r, s)#7 t})
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, zz]
        -> range_req({t h(t, u, v, w, x, y, z, zz)#8 𝛀 })
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, zz]
        <- range_resp({m h(m, n, p)#3 q}, {q h(q, r, s)#3 t})
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u, w, x, y, z]
        <- range_resp({t h(t, u, w)#3 x}, {x h(x, y, z)#3 𝛀 })
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u, w, x, y, z]
        -> range_req({m h(m)#1 n})
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, zz]
        -> range_req({n h(n)#1 o})
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, zz]
        -> range_req({o h(o)#1 p})
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, zz]
        -> range_req({p h(p)#1 q})
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, zz]
        -> range_req({t h(t)#1 u})
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, zz]
        -> range_req({u h(u)#1 v})
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, zz]
        -> range_req({v h(v)#1 w})
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, zz]
        -> range_req({w h(w)#1 x})
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, zz]
        -> range_req({x h(x)#1 y})
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, zz]
        -> range_req({y h(y)#1 z})
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, zz]
        -> range_req({z h(z)#1 zz})
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, zz]
        -> range_req({zz h(zz)#1 𝛀 })
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, zz]
        <- range_resp({m h(m)#1 n})
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u, w, x, y, z]
        <- range_resp({n h(n)#1 o})
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u, w, x, y, z]
        <- range_resp({o 0 p})
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u, w, x, y, z]
        <- range_resp({p h(p)#1 q})
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u, w, x, y, z]
        <- range_resp({t h(t)#1 u})
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u, w, x, y, z]
        <- range_resp({u h(u)#1 v})
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u, w, x, y, z]
        <- range_resp({v 0 w})
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u, w, x, y, z]
        <- range_resp({w h(w)#1 x})
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u, w, x, y, z]
        <- range_resp({x h(x)#1 y})
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u, w, x, y, z]
        <- range_resp({y h(y)#1 z})
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u, w, x, y, z]
        <- range_resp({z h(z)#1 zz})
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u, w, x, y, z]
        <- range_resp({zz 0 𝛀 })
            dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u, w, x, y, z]
        -> value(o: o)
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, zz]
        -> value(v: v)
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, zz]
        -> value(zz: zz)
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, zz]
        -> finished
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, zz]
        cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, zz]
        dog: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, zz]
    "#]]).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn dog_linear_download() {
    let recon = expect![[r#"
        cat: [a, b, c, d, e, f, g]
        dog: []
        -> interest_req((𝚨, 𝛀 ))
            cat: [a, b, c, d, e, f, g]
        <- interest_resp((𝚨, 𝛀 ))
            dog: []
        -> range_req({𝚨 h(a, b, c, d, e, f, g)#7 𝛀 })
            cat: [a, b, c, d, e, f, g]
        <- range_resp({𝚨 0 𝛀 })
            dog: []
        -> value(a: a)
            cat: [a, b, c, d, e, f, g]
        -> value(b: b)
            cat: [a, b, c, d, e, f, g]
        -> value(c: c)
            cat: [a, b, c, d, e, f, g]
        -> value(d: d)
            cat: [a, b, c, d, e, f, g]
        -> value(e: e)
            cat: [a, b, c, d, e, f, g]
        -> value(f: f)
            cat: [a, b, c, d, e, f, g]
        -> value(g: g)
            cat: [a, b, c, d, e, f, g]
        -> finished
            cat: [a, b, c, d, e, f, g]
        cat: [a, b, c, d, e, f, g]
        dog: [a, b, c, d, e, f, g]
    "#]];
    let actual = format!("{}", recon_do(recon.data()).await);
    recon.assert_eq(&actual);
    let batching = format!("{}", recon_do_batch_size(recon.data(), 100).await);
    recon.assert_eq(&batching)
}
#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn cat_linear_download() {
    let recon = expect![[r#"
        cat: []
        dog: [a, b, c, d, e, f, g]
        -> interest_req((𝚨, 𝛀 ))
            cat: []
        <- interest_resp((𝚨, 𝛀 ))
            dog: [a, b, c, d, e, f, g]
        -> range_req({𝚨 0 𝛀 })
            cat: []
        <- value(a: a)
            dog: [a, b, c, d, e, f, g]
        <- value(b: b)
            dog: [a, b, c, d, e, f, g]
        <- value(c: c)
            dog: [a, b, c, d, e, f, g]
        <- value(d: d)
            dog: [a, b, c, d, e, f, g]
        <- value(e: e)
            dog: [a, b, c, d, e, f, g]
        <- value(f: f)
            dog: [a, b, c, d, e, f, g]
        <- value(g: g)
            dog: [a, b, c, d, e, f, g]
        <- range_resp()
            dog: [a, b, c, d, e, f, g]
        -> finished
            cat: [a, b, c, d, e, f, g]
        cat: [a, b, c, d, e, f, g]
        dog: [a, b, c, d, e, f, g]
    "#]];

    let actual = format!("{}", recon_do(recon.data()).await);
    recon.assert_eq(&actual);
    let batching = format!("{}", recon_do_batch_size(recon.data(), 100).await);
    recon.assert_eq(&batching)
}
#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn subset_interest() {
    recon_test(expect![[r#"
        cat: <(b, i), (m, r)> [c, f, g, r]
        dog: <(a, z)> [b, c, d, e, f, g, h, i, j, k, l, m, n]
        -> interest_req((b, i), (m, r))
            cat: [c, f, g, r]
        <- interest_resp((b, i), (m, r))
            dog: [b, c, d, e, f, g, h, i, j, k, l, m, n]
        -> range_req({b h(c, f, g)#3 i})
            cat: [c, f, g, r]
        -> range_req({m 0 r})
            cat: [c, f, g, r]
        <- range_resp({b h(b, c, d)#3 e}, {e h(e, f, g, h)#4 i})
            dog: [b, c, d, e, f, g, h, i, j, k, l, m, n]
        <- value(m: m)
            dog: [b, c, d, e, f, g, h, i, j, k, l, m, n]
        <- value(n: n)
            dog: [b, c, d, e, f, g, h, i, j, k, l, m, n]
        <- range_resp()
            dog: [b, c, d, e, f, g, h, i, j, k, l, m, n]
        -> range_req({b 0 c})
            cat: [c, f, g, m, n, r]
        -> range_req({c h(c)#1 e})
            cat: [c, f, g, m, n, r]
        -> range_req({e 0 f})
            cat: [c, f, g, m, n, r]
        -> range_req({f h(f)#1 g})
            cat: [c, f, g, m, n, r]
        -> range_req({g h(g)#1 i})
            cat: [c, f, g, m, n, r]
        <- value(b: b)
            dog: [b, c, d, e, f, g, h, i, j, k, l, m, n]
        <- range_resp()
            dog: [b, c, d, e, f, g, h, i, j, k, l, m, n]
        <- value(c: c)
            dog: [b, c, d, e, f, g, h, i, j, k, l, m, n]
        <- value(d: d)
            dog: [b, c, d, e, f, g, h, i, j, k, l, m, n]
        <- range_resp()
            dog: [b, c, d, e, f, g, h, i, j, k, l, m, n]
        <- value(e: e)
            dog: [b, c, d, e, f, g, h, i, j, k, l, m, n]
        <- range_resp()
            dog: [b, c, d, e, f, g, h, i, j, k, l, m, n]
        <- range_resp({f h(f)#1 g})
            dog: [b, c, d, e, f, g, h, i, j, k, l, m, n]
        <- value(g: g)
            dog: [b, c, d, e, f, g, h, i, j, k, l, m, n]
        <- value(h: h)
            dog: [b, c, d, e, f, g, h, i, j, k, l, m, n]
        <- range_resp()
            dog: [b, c, d, e, f, g, h, i, j, k, l, m, n]
        -> finished
            cat: [b, c, d, e, f, g, h, m, n, r]
        cat: [b, c, d, e, f, g, h, m, n, r]
        dog: [b, c, d, e, f, g, h, i, j, k, l, m, n]
    "#]])
    .await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn partial_interest() {
    recon_test(expect![[r#"
        cat: <(b, g), (i, q)> [a, b, c, d, e, f, g, h, i, j, k, l, m, o, p, q]
        dog: <(k, t), (u, z)> [j, k, n, o, p, q, r, s, t, u, w, x, y, z]
        -> interest_req((b, g), (i, q))
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, o, p, q]
        <- interest_resp((k, q))
            dog: [j, k, n, o, p, q, r, s, t, u, w, x, y, z]
        -> range_req({k h(k, l, m, o, p)#5 q})
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, o, p, q]
        <- range_resp({k h(k)#1 n}, {n h(n)#1 o}, {o h(o)#1 p}, {p h(p)#1 q})
            dog: [j, k, n, o, p, q, r, s, t, u, w, x, y, z]
        -> value(k: k)
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, o, p, q]
        -> value(l: l)
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, o, p, q]
        -> value(m: m)
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, o, p, q]
        -> range_req({n 0 o})
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, o, p, q]
        <- value(n: n)
            dog: [j, k, l, m, n, o, p, q, r, s, t, u, w, x, y, z]
        <- range_resp()
            dog: [j, k, l, m, n, o, p, q, r, s, t, u, w, x, y, z]
        -> finished
            cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q]
        cat: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q]
        dog: [j, k, l, m, n, o, p, q, r, s, t, u, w, x, y, z]
    "#]])
    .await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn no_interest() {
    recon_test(expect![[r#"
        cat: <(a, d)> [a, b, c]
        dog: <(x, z)> [x, y]
        -> interest_req((a, d))
            cat: [a, b, c]
        <- interest_resp()
            dog: [x, y]
        -> finished
            cat: [a, b, c]
        cat: [a, b, c]
        dog: [x, y]
    "#]])
    .await;
}
