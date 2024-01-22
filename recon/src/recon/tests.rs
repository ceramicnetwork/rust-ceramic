lalrpop_util::lalrpop_mod!(
    #[allow(clippy::all, missing_debug_implementations)]
    pub parser, "/recon/parser.rs"
); // synthesized by LALRPOP

use anyhow::Result;
use async_trait::async_trait;
use ceramic_core::{Bytes, RangeOpen};
use prometheus_client::registry::Registry;
use std::collections::BTreeSet;
use std::fmt::Display;
use tracing_test::traced_test;

use codespan_reporting::{
    diagnostic::{Diagnostic, Label},
    files::SimpleFiles,
    term::{self, termcolor::Buffer},
};
use expect_test::{expect, Expect};
use lalrpop_util::ParseError;
use pretty::{Arena, DocAllocator, DocBuilder, Pretty};

use crate::{
    recon::{FullInterests, InterestProvider},
    AssociativeHash, BTreeStore, Key, Message, Metrics, Recon, Sha256a, Store,
};

type Set = BTreeSet<Bytes>;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct MemoryAHash {
    ahash: Sha256a,
    set: BTreeSet<Bytes>,
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

impl From<Message<Bytes, MemoryAHash>> for MessageData {
    fn from(value: Message<Bytes, MemoryAHash>) -> Self {
        Self {
            // Treat min/max values as None
            start: value
                .start
                .map(|start| {
                    if start == Bytes::min_value() {
                        None
                    } else {
                        Some(start.to_string())
                    }
                })
                .unwrap_or_default(),
            end: value
                .end
                .map(|end| {
                    if end == Bytes::max_value() {
                        None
                    } else {
                        Some(end.to_string())
                    }
                })
                .unwrap_or_default(),
            keys: value.keys.iter().map(|key| key.to_string()).collect(),
            ahashs: value.hashes.into_iter().map(|h| h.set).collect(),
        }
    }
}

/// Recon type that uses Bytes for a Key and MemoryAHash for the Hash
pub type ReconMemoryBytes<I> = Recon<Bytes, MemoryAHash, BTreeStore<Bytes, MemoryAHash>, I>;

/// Recon type that uses Bytes for a Key and Sha256a for the Hash
pub type ReconBytes = Recon<Bytes, Sha256a, BTreeStore<Bytes, Sha256a>, FullInterests<Bytes>>;

/// Implement InterestProvider for a fixed set of interests.
#[derive(Debug, PartialEq)]
pub struct FixedInterests(Vec<RangeOpen<Bytes>>);

impl FixedInterests {
    pub fn full() -> Self {
        Self(vec![(Bytes::min_value(), Bytes::max_value()).into()])
    }
    pub fn is_full(&self) -> bool {
        self == &Self::full()
    }
}
#[async_trait]
impl InterestProvider for FixedInterests {
    type Key = Bytes;

    async fn interests(&self) -> anyhow::Result<Vec<RangeOpen<Self::Key>>> {
        Ok(self.0.clone())
    }
}

#[derive(Debug)]
pub struct Record {
    cat: ReconMemoryBytes<FixedInterests>,
    dog: ReconMemoryBytes<FixedInterests>,
    iterations: Vec<Iteration>,
}

impl<'a, D, A> Pretty<'a, D, A> for &'a Record
where
    A: 'a + Clone,
    D: DocAllocator<'a, A>,
    D::Doc: Clone,
{
    fn pretty(self, allocator: &'a D) -> DocBuilder<'a, D, A> {
        let peer = |name, recon: &ReconMemoryBytes<FixedInterests>| {
            let separator = allocator.text(",").append(allocator.softline_());
            let interests = if !recon.interests.is_full() {
                allocator.softline().append(
                    allocator
                        .intersperse(
                            // The call to get interests is async so we can't easily call it
                            // here. However we have a FixedInterests so we can access the
                            // interests vector directly.
                            recon.interests.0.iter().map(|range| {
                                allocator
                                    .text(range.start.to_string())
                                    .append(separator.clone())
                                    .append(allocator.text(range.end.to_string()))
                                    .parens()
                            }),
                            separator.clone(),
                        )
                        .enclose("<", ">")
                        .append(allocator.softline()),
                )
            } else {
                allocator.softline()
            };
            let set: Vec<Bytes> = recon
                .store
                .range(&Bytes::min_value(), &Bytes::max_value(), 0, usize::MAX)
                .unwrap()
                .collect();
            allocator
                .text(name)
                .append(allocator.text(":"))
                .append(interests)
                .append(
                    allocator
                        .intersperse(
                            set.iter().map(|data| allocator.text(data.to_string())),
                            separator,
                        )
                        .brackets(),
                )
        };

        allocator
            .nil()
            .append(peer("cat", &self.cat))
            .group()
            .append(allocator.hardline())
            .append(peer("dog", &self.dog))
            .group()
            .append(allocator.hardline())
            .append(allocator.intersperse(self.iterations.iter(), allocator.hardline()))
            .group()
    }
}

impl Display for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let arena: Arena<()> = Arena::new();
        let mut w = Vec::new();
        self.pretty(&arena).render(200, &mut w).unwrap();
        write!(f, "{}", String::from_utf8(w).unwrap())
    }
}

#[derive(Debug, Clone)]
pub struct MessageData {
    pub start: Option<String>,
    pub end: Option<String>,
    pub keys: Vec<String>, // keys must be 1 longer then ahashs unless both are empty
    pub ahashs: Vec<Set>,  // ahashs must be 1 shorter then keys
}

impl<H> From<MessageData> for Message<Bytes, H>
where
    H: AssociativeHash,
{
    fn from(value: MessageData) -> Self {
        Self {
            start: value.start.map(Bytes::from),
            end: value.end.map(Bytes::from),
            keys: value.keys.iter().map(Bytes::from).collect(),
            hashes: value
                .ahashs
                .into_iter()
                .map(|set| {
                    BTreeStore::from_set(set)
                        .hash_range(&Bytes::min_value(), &Bytes::max_value())
                        .unwrap()
                        .hash
                })
                .collect(),
        }
    }
}

#[derive(Debug)]
pub struct Iteration {
    dir: Direction,
    messages: Vec<MessageData>,
    set: Set,
}

impl<'a, D, A> Pretty<'a, D, A> for &'a Iteration
where
    A: 'a + Clone,
    D: DocAllocator<'a, A>,
    D::Doc: Clone,
{
    fn pretty(self, allocator: &'a D) -> DocBuilder<'a, D, A> {
        let space_sep = allocator.text(",").append(allocator.softline());
        let no_space_sep = allocator.text(",");

        // Construct doc for direction
        let dir = match self.dir {
            Direction::CatToDog => allocator.text("-> "),
            Direction::DogToCat => allocator.text("<- "),
        };
        let messages = allocator.intersperse(self.messages.iter(), space_sep);

        // Construct doc for set
        let set = allocator.intersperse(
            self.set.iter().map(|s| allocator.text(s.to_string())),
            no_space_sep,
        );

        // Put it all together
        dir.append(messages)
            .append(allocator.softline())
            .append(set.brackets())
            .hang(4)
    }
}

impl<'a, D, A> Pretty<'a, D, A> for &'a MessageData
where
    A: 'a + Clone,
    D: DocAllocator<'a, A>,
    D::Doc: Clone,
{
    fn pretty(self, allocator: &'a D) -> DocBuilder<'a, D, A> {
        let space_sep = allocator.text(",").append(allocator.softline());
        let no_space_sep = allocator.text(",");
        // Construct doc for msg
        let mut msg = allocator.nil();
        if let Some(start) = &self.start {
            msg = msg
                .append(allocator.text("<"))
                .append(start)
                .append(space_sep.clone());
        }
        let l = self.keys.len();
        for i in 0..l {
            msg = msg.append(allocator.text(self.keys[i].as_str()));
            if i < l - 1 {
                msg = msg.append(space_sep.clone());
                if self.ahashs[i].is_empty() {
                    msg = msg.append(allocator.text("0")).append(space_sep.clone());
                } else {
                    msg = msg
                        .append(allocator.text("h("))
                        .append(allocator.intersperse(
                            self.ahashs[i].iter().map(|s| allocator.text(s.to_string())),
                            no_space_sep.clone(),
                        ))
                        .append(allocator.text(")"))
                        .append(space_sep.clone());
                }
            }
        }
        if let Some(end) = &self.end {
            if l > 0 {
                msg = msg.append(space_sep);
            }
            msg = msg.append(end).append(allocator.text(">"));
        }

        msg.parens()
    }
}

#[derive(Debug)]
pub enum Direction {
    CatToDog,
    DogToCat,
}

#[derive(Debug)]
pub enum MessageItem {
    Key(String),
    Hash(Set),
    Start(String),
    End(String),
}

// Help implementation to construct a message from parsed [`MessageItem`]
impl TryFrom<(Option<MessageItem>, Vec<MessageItem>)> for MessageData {
    type Error = &'static str;

    fn try_from(value: (Option<MessageItem>, Vec<MessageItem>)) -> Result<Self, Self::Error> {
        let mut start = None;
        let mut end = None;
        let mut keys = Vec::new();
        let mut ahashs = Vec::new();
        match value.0 {
            Some(MessageItem::Start(k)) => start = Some(k),
            Some(MessageItem::Key(k)) => keys.push(k),
            Some(MessageItem::Hash(_)) => return Err("message cannot begin with a hash"),
            Some(MessageItem::End(_)) => return Err("message cannot begin with an end bound"),
            None => {}
        };
        for item in value.1 {
            if end.is_some() {
                return Err("end bound must be the final message item");
            }
            match item {
                MessageItem::Key(k) => keys.push(k),
                MessageItem::Hash(set) => {
                    ahashs.push(set);
                }
                MessageItem::Start(_) => return Err("start bound must start the message"),
                MessageItem::End(k) => end = Some(k),
            }
        }
        if !keys.is_empty() && keys.len() - 1 != ahashs.len() {
            return Err("invalid message, unmatched keys and hashes");
        }
        Ok(MessageData {
            start,
            end,
            keys,
            ahashs,
        })
    }
}

#[tokio::test]
async fn word_lists() {
    async fn recon_from_string(s: &str) -> ReconBytes {
        let mut r = ReconBytes::new(
            BTreeStore::default(),
            FullInterests::default(),
            Metrics::register(&mut Registry::default()),
        );
        for key in s.split([' ', '\n']).map(|s| s.to_string()) {
            if !s.is_empty() {
                r.insert(&key.as_bytes().into()).await.unwrap();
            }
        }
        r
    }
    let mut peers = vec![
        recon_from_string(include_str!("../tests/bip_39.txt")).await,
        recon_from_string(include_str!("../tests/eff_large_wordlist.txt")).await,
        recon_from_string(include_str!("../tests/eff_short_wordlist_1.txt")).await,
        recon_from_string(include_str!("../tests/eff_short_wordlist_2.txt")).await,
        recon_from_string(include_str!("../tests/wordle_words5_big.txt")).await,
        recon_from_string(include_str!("../tests/wordle_words5.txt")).await,
        // recon_from_string(include_str!("../tests/connectives.txt")).await,
        // recon_from_string(include_str!("../tests/propernames.txt")).await,
        // recon_from_string(include_str!("../tests/web2.txt")).await,
        // recon_from_string(include_str!("../tests/web2a.txt")).await,
    ];
    let keys_len = 21139;
    // let expected_first = "aahed";
    // let expected_last = "zythum";
    // let expected_ahash = "13BA255FBD4C2566CB2564EFA0C1782ABA61604AC07A8789D1DF9E391D73584E";

    for peer in &mut peers {
        println!(
            "peer  {} {} {}",
            peer.store
                .first(&Bytes::min_value(), &Bytes::max_value())
                .await
                .unwrap()
                .unwrap(),
            peer.store.len().await.unwrap(),
            peer.store
                .last(&Bytes::min_value(), &Bytes::max_value())
                .await
                .unwrap()
                .unwrap(),
        )
    }

    // We are using a FullInterest so we can assume there is only ever one message per exchange.
    let mut local = ReconBytes::new(
        BTreeStore::default(),
        FullInterests::default(),
        Metrics::register(&mut Registry::default()),
    );
    async fn sync(local: &mut ReconBytes, peers: &mut [ReconBytes]) {
        for j in 0..3 {
            for (i, peer) in peers.iter_mut().enumerate() {
                println!(
                    "round:{} peer:{}\n\t[{}]\n\t[{}]",
                    j,
                    i,
                    local.store.len().await.unwrap(),
                    peer.store.len().await.unwrap(),
                );
                let mut next = local.initial_messages().await.unwrap();
                for k in 0..50 {
                    println!(
                        "\t{}: -> {}[{}]",
                        k,
                        if next[0].keys.len() < 10 {
                            format!("{:?}", next[0])
                        } else {
                            format!("({})", next[0].keys.len())
                        },
                        local.store.len().await.unwrap(),
                    );

                    let response = peer.process_messages(&next).await.unwrap();

                    println!(
                        "\t{}: <- {}[{}]",
                        k,
                        if response.messages[0].keys.len() < 10 {
                            format!("{:?}", response.messages[0])
                        } else {
                            format!("({})", response.messages[0].keys.len())
                        },
                        peer.store.len().await.unwrap(),
                    );

                    next = local
                        .process_messages(&response.messages)
                        .await
                        .unwrap()
                        .messages;

                    if response.messages[0].keys.len() < 3 && next[0].keys.len() < 3 {
                        println!("\tpeers[{}] in sync", i);
                        break;
                    }
                }
            }
        }
    }
    sync(&mut local, &mut peers).await;
    for peer in &mut peers {
        println!(
            "after {} {} {}",
            peer.store
                .first(&Bytes::min_value(), &Bytes::max_value())
                .await
                .unwrap()
                .unwrap(),
            peer.store.len().await.unwrap(),
            peer.store
                .last(&Bytes::min_value(), &Bytes::max_value())
                .await
                .unwrap()
                .unwrap(),
        );
    }

    assert_eq!(local.store.len().await.unwrap(), keys_len);
    for peer in &mut peers {
        assert_eq!(peer.store.len().await.unwrap(), keys_len)
    }
    expect![[r#"
        [
            "aahed",
            "zymic",
        ]
    "#]]
    .assert_debug_eq(
        &local
            .initial_messages()
            .await
            .unwrap()
            .iter()
            .flat_map(|msg| msg.keys.iter().map(|k| k.to_string()))
            .collect::<Vec<String>>(),
    );
    expect![["13BA255FBD4C2566CB2564EFA0C1782ABA61604AC07A8789D1DF9E391D73584E"]]
        .assert_eq(&local.initial_messages().await.unwrap()[0].hashes[0].to_hex());

    local.insert(&b"ceramic".as_slice().into()).await.unwrap();
    sync(&mut local, &mut peers).await;
}
#[tokio::test]
async fn response_is_synchronized() {
    let mut client = ReconMemoryBytes::new(
        BTreeStore::from_set(BTreeSet::from_iter([
            Bytes::from("a"),
            Bytes::from("b"),
            Bytes::from("c"),
            Bytes::from("n"),
        ])),
        FullInterests::default(),
        Metrics::register(&mut Registry::default()),
    );
    let mut server = ReconMemoryBytes::new(
        BTreeStore::from_set(BTreeSet::from_iter([
            Bytes::from("x"),
            Bytes::from("y"),
            Bytes::from("z"),
            Bytes::from("n"),
        ])),
        FullInterests::default(),
        Metrics::register(&mut Registry::default()),
    );
    // Client -> Server: Init
    let server_first_response = server
        .process_messages(&client.initial_messages().await.unwrap())
        .await
        .unwrap();
    // Server -> Client: Not yet in sync
    assert!(!server_first_response.is_synchronized);
    let client_first_response = client
        .process_messages(&server_first_response.messages)
        .await
        .unwrap();
    // Client -> Server: Not yet in sync
    assert!(!client_first_response.is_synchronized);

    // After this message we should be synchronized
    let server_second_response = server
        .process_messages(&client_first_response.messages)
        .await
        .unwrap();
    // Server -> Client: Synced
    assert!(server_second_response.is_synchronized);
    let client_second_response = client
        .process_messages(&server_second_response.messages)
        .await
        .unwrap();
    // Client -> Server: Synced
    assert!(client_second_response.is_synchronized);
    // Check that we remained in sync. This exchange is not needed for a real sync.
    let server_third_response = server
        .process_messages(&client_second_response.messages)
        .await
        .unwrap();
    // Once synced, always synced.
    assert!(server_third_response.is_synchronized);
}

#[test]
fn hello() {
    let other_hash = ReconBytes::new(
        BTreeStore::from_set(BTreeSet::from_iter([
            Bytes::from("hello"),
            Bytes::from("world"),
        ])),
        FullInterests::default(),
        Metrics::register(&mut Registry::default()),
    );
    expect![[r#"
        Recon {
            interests: FullInterests(
                PhantomData<ceramic_core::bytes::Bytes>,
            ),
            store: BTreeStore {
                keys: {
                    Bytes(
                        "hello",
                    ): Sha256a {
                        hex: "2CF24DBA5FB0A30E26E83B2AC5B9E29E1B161E5C1FA7425E73043362938B9824",
                        u32_8: [
                            3125670444,
                            245608543,
                            708569126,
                            2665658821,
                            1545475611,
                            1581426463,
                            1647510643,
                            613976979,
                        ],
                    },
                    Bytes(
                        "world",
                    ): Sha256a {
                        hex: "486EA46224D1BB4FB680F34F7C9AD96A8F24EC88BE73EA8E5A6C65260E9CB8A7",
                        u32_8: [
                            1654943304,
                            1337708836,
                            1341358262,
                            1792645756,
                            2297177231,
                            2397729726,
                            644181082,
                            2813893646,
                        ],
                    },
                },
            },
            metrics: Metrics {
                key_insert_count: Counter {
                    value: 0,
                    phantom: PhantomData<u64>,
                },
                store_query_durations: Family {
                    metrics: RwLock {
                        data: {},
                    },
                },
            },
        }
    "#]]
    .assert_debug_eq(&other_hash)
}

#[tokio::test]
async fn abcde() {
    recon_test(expect![[r#"
        cat: [b,c,d,e]
        dog: [a,e]
        -> (b, h(c,d), e) [a,b,e]
        <- (a, 0, b, 0, e) [a,b,c,d,e]
        -> (a, 0, b, 0, c, 0, d, 0, e) [a,b,c,d,e]
        <- (a, h(b,c,d), e) [a,b,c,d,e]"#]])
    .await
}

#[tokio::test]
async fn two_in_a_row() {
    recon_test(expect![[r#"
    cat: [a,b,c,d,e]
    dog: [a,d,e]
    -> (a, h(b,c,d), e) [a,d,e]
    <- (a, 0, d, 0, e) [a,b,c,d,e]
    -> (a, 0, b, 0, c, h(d), e) [a,b,c,d,e]
    <- (a, h(b,c,d), e) [a,b,c,d,e]"#]])
    .await
}

#[test]
fn test_parse_recon() {
    let recon = r#"
        cat: [a,b,c]
        dog: [e,f,g]
        -> (a, h(b), c) [a,c,e,f,g]
        <- (a, 0, c, h(e,f), g) [a,b,c,g]
        -> (a, 0, b, 0, c, 0, g) [a,b,c,e,f,g]
        <- (a, h(b), c, 0, e, 0, f, 0, g) [a,b,c,e,f,g]
        "#;
    let record = parser::RecordParser::new().parse(recon).unwrap();

    expect![[r#"
        Record {
            cat: Recon {
                interests: FixedInterests(
                    [
                        RangeOpen {
                            start: Bytes(
                                "",
                            ),
                            end: Bytes(
                                "0xFFFFFFFFFFFFFFFFFF",
                            ),
                        },
                    ],
                ),
                store: BTreeStore {
                    keys: {
                        Bytes(
                            "a",
                        ): MemoryAHash {
                            ahash: Sha256a {
                                hex: "CA978112CA1BBDCAFAC231B39A23DC4DA786EFF8147C4E72B9807785AFEE48BB",
                                u32_8: [
                                    310482890,
                                    3401391050,
                                    3006382842,
                                    1306272666,
                                    4176447143,
                                    1917746196,
                                    2239201465,
                                    3142119087,
                                ],
                            },
                            set: {
                                Bytes(
                                    "a",
                                ),
                            },
                        },
                        Bytes(
                            "b",
                        ): MemoryAHash {
                            ahash: Sha256a {
                                hex: "3E23E8160039594A33894F6564E1B1348BBD7A0088D42C4ACB73EEAED59C009D",
                                u32_8: [
                                    384312126,
                                    1247361280,
                                    1699711283,
                                    884072804,
                                    8043915,
                                    1244451976,
                                    2934862795,
                                    2634063061,
                                ],
                            },
                            set: {
                                Bytes(
                                    "b",
                                ),
                            },
                        },
                        Bytes(
                            "c",
                        ): MemoryAHash {
                            ahash: Sha256a {
                                hex: "2E7D2C03A9507AE265ECF5B5356885A53393A2029D241394997265A1A25AEFC6",
                                u32_8: [
                                    53247278,
                                    3799666857,
                                    3052792933,
                                    2776983605,
                                    44208947,
                                    2484282525,
                                    2707780249,
                                    3337575074,
                                ],
                            },
                            set: {
                                Bytes(
                                    "c",
                                ),
                            },
                        },
                    },
                },
                metrics: Metrics {
                    key_insert_count: Counter {
                        value: 0,
                        phantom: PhantomData<u64>,
                    },
                    store_query_durations: Family {
                        metrics: RwLock {
                            data: {},
                        },
                    },
                },
            },
            dog: Recon {
                interests: FixedInterests(
                    [
                        RangeOpen {
                            start: Bytes(
                                "",
                            ),
                            end: Bytes(
                                "0xFFFFFFFFFFFFFFFFFF",
                            ),
                        },
                    ],
                ),
                store: BTreeStore {
                    keys: {
                        Bytes(
                            "e",
                        ): MemoryAHash {
                            ahash: Sha256a {
                                hex: "3F79BB7B435B05321651DAEFD374CDC681DC06FAA65E374E38337B88CA046DEA",
                                u32_8: [
                                    2075883839,
                                    839211843,
                                    4024062230,
                                    3335353555,
                                    4194753665,
                                    1312251558,
                                    2289775416,
                                    3933013194,
                                ],
                            },
                            set: {
                                Bytes(
                                    "e",
                                ),
                            },
                        },
                        Bytes(
                            "f",
                        ): MemoryAHash {
                            ahash: Sha256a {
                                hex: "252F10C83610EBCA1A059C0BAE8255EBA2F95BE4D1D7BCFA89D7248A82D9F111",
                                u32_8: [
                                    3356503845,
                                    3404402742,
                                    194774298,
                                    3948249774,
                                    3831232930,
                                    4206680017,
                                    2317670281,
                                    301062530,
                                ],
                            },
                            set: {
                                Bytes(
                                    "f",
                                ),
                            },
                        },
                        Bytes(
                            "g",
                        ): MemoryAHash {
                            ahash: Sha256a {
                                hex: "CD0AA9856147B6C5B4FF2B7DFEE5DA20AA38253099EF1B4A64ACED233C9AFE29",
                                u32_8: [
                                    2242448077,
                                    3317057377,
                                    2100035508,
                                    551216638,
                                    807745706,
                                    1243344793,
                                    602778724,
                                    704551484,
                                ],
                            },
                            set: {
                                Bytes(
                                    "g",
                                ),
                            },
                        },
                    },
                },
                metrics: Metrics {
                    key_insert_count: Counter {
                        value: 0,
                        phantom: PhantomData<u64>,
                    },
                    store_query_durations: Family {
                        metrics: RwLock {
                            data: {},
                        },
                    },
                },
            },
            iterations: [
                Iteration {
                    dir: CatToDog,
                    messages: [
                        MessageData {
                            start: None,
                            end: None,
                            keys: [
                                "a",
                                "c",
                            ],
                            ahashs: [
                                {
                                    Bytes(
                                        "b",
                                    ),
                                },
                            ],
                        },
                    ],
                    set: {
                        Bytes(
                            "a",
                        ),
                        Bytes(
                            "c",
                        ),
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
                Iteration {
                    dir: DogToCat,
                    messages: [
                        MessageData {
                            start: None,
                            end: None,
                            keys: [
                                "a",
                                "c",
                                "g",
                            ],
                            ahashs: [
                                {},
                                {
                                    Bytes(
                                        "e",
                                    ),
                                    Bytes(
                                        "f",
                                    ),
                                },
                            ],
                        },
                    ],
                    set: {
                        Bytes(
                            "a",
                        ),
                        Bytes(
                            "b",
                        ),
                        Bytes(
                            "c",
                        ),
                        Bytes(
                            "g",
                        ),
                    },
                },
                Iteration {
                    dir: CatToDog,
                    messages: [
                        MessageData {
                            start: None,
                            end: None,
                            keys: [
                                "a",
                                "b",
                                "c",
                                "g",
                            ],
                            ahashs: [
                                {},
                                {},
                                {},
                            ],
                        },
                    ],
                    set: {
                        Bytes(
                            "a",
                        ),
                        Bytes(
                            "b",
                        ),
                        Bytes(
                            "c",
                        ),
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
                Iteration {
                    dir: DogToCat,
                    messages: [
                        MessageData {
                            start: None,
                            end: None,
                            keys: [
                                "a",
                                "c",
                                "e",
                                "f",
                                "g",
                            ],
                            ahashs: [
                                {
                                    Bytes(
                                        "b",
                                    ),
                                },
                                {},
                                {},
                                {},
                            ],
                        },
                    ],
                    set: {
                        Bytes(
                            "a",
                        ),
                        Bytes(
                            "b",
                        ),
                        Bytes(
                            "c",
                        ),
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
            ],
        }
    "#]]
        .assert_debug_eq(&record);
}
#[test]
fn test_parse_recon_empty_set() {
    let recon = r#"
cat: [a]
dog: []
-> (a) [a]
<- (a) [a]
"#;
    let record = parser::RecordParser::new().parse(recon).unwrap();

    expect![[r#"
        Record {
            cat: Recon {
                interests: FixedInterests(
                    [
                        RangeOpen {
                            start: Bytes(
                                "",
                            ),
                            end: Bytes(
                                "0xFFFFFFFFFFFFFFFFFF",
                            ),
                        },
                    ],
                ),
                store: BTreeStore {
                    keys: {
                        Bytes(
                            "a",
                        ): MemoryAHash {
                            ahash: Sha256a {
                                hex: "CA978112CA1BBDCAFAC231B39A23DC4DA786EFF8147C4E72B9807785AFEE48BB",
                                u32_8: [
                                    310482890,
                                    3401391050,
                                    3006382842,
                                    1306272666,
                                    4176447143,
                                    1917746196,
                                    2239201465,
                                    3142119087,
                                ],
                            },
                            set: {
                                Bytes(
                                    "a",
                                ),
                            },
                        },
                    },
                },
                metrics: Metrics {
                    key_insert_count: Counter {
                        value: 0,
                        phantom: PhantomData<u64>,
                    },
                    store_query_durations: Family {
                        metrics: RwLock {
                            data: {},
                        },
                    },
                },
            },
            dog: Recon {
                interests: FixedInterests(
                    [
                        RangeOpen {
                            start: Bytes(
                                "",
                            ),
                            end: Bytes(
                                "0xFFFFFFFFFFFFFFFFFF",
                            ),
                        },
                    ],
                ),
                store: BTreeStore {
                    keys: {},
                },
                metrics: Metrics {
                    key_insert_count: Counter {
                        value: 0,
                        phantom: PhantomData<u64>,
                    },
                    store_query_durations: Family {
                        metrics: RwLock {
                            data: {},
                        },
                    },
                },
            },
            iterations: [
                Iteration {
                    dir: CatToDog,
                    messages: [
                        MessageData {
                            start: None,
                            end: None,
                            keys: [
                                "a",
                            ],
                            ahashs: [],
                        },
                    ],
                    set: {
                        Bytes(
                            "a",
                        ),
                    },
                },
                Iteration {
                    dir: DogToCat,
                    messages: [
                        MessageData {
                            start: None,
                            end: None,
                            keys: [
                                "a",
                            ],
                            ahashs: [],
                        },
                    ],
                    set: {
                        Bytes(
                            "a",
                        ),
                    },
                },
            ],
        }
    "#]]
        .assert_debug_eq(&record);
}

fn parse_recon(recon: &str) -> Record {
    // Setup codespan-reporting
    let mut files = SimpleFiles::new();
    let file_id = files.add("test.recon", recon);
    match parser::RecordParser::new().parse(recon) {
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

// Run the recon simulation ignoring the expected iterations
async fn recon_do(recon: &str) -> Record {
    let mut record = parse_recon(recon);

    // Remember initial state
    let cat = record.cat.store.clone();
    let dog = record.dog.store.clone();

    let n = record.iterations.len();
    record.iterations.clear();

    // Run simulation for the number of iterations in the original record
    let mut dir = Direction::CatToDog;
    let mut messages: Vec<Message<Bytes, MemoryAHash>> =
        record.cat.initial_messages().await.unwrap();
    for _ in 0..n {
        let (next_dir, response, mut set) = match dir {
            Direction::CatToDog => (
                Direction::DogToCat,
                record.dog.process_messages(&messages).await.unwrap(),
                record.dog.store.clone(),
            ),
            Direction::DogToCat => (
                Direction::CatToDog,
                record.cat.process_messages(&messages).await.unwrap(),
                record.cat.store.clone(),
            ),
        };
        record.iterations.push(Iteration {
            dir,
            messages: messages.into_iter().map(MessageData::from).collect(),
            set: set.full_range().await.unwrap().collect(),
        });
        dir = next_dir;
        messages = response.messages
    }
    // Restore initial sets
    record.cat.store = cat;
    record.dog.store = dog;
    record
}

async fn recon_test(recon: Expect) {
    let actual = format!("{}", recon_do(recon.data()).await);
    recon.assert_eq(&actual)
}

#[tokio::test]
async fn abcd() {
    recon_test(expect![[r#"
        cat: [b,c,d,e]
        dog: [a,e]
        -> (b, h(c,d), e) [a,b,e]
        <- (a, 0, b, 0, e) [a,b,c,d,e]
        -> (a, 0, b, 0, c, 0, d, 0, e) [a,b,c,d,e]
        <- (a, h(b,c,d), e) [a,b,c,d,e]"#]])
    .await
}
#[tokio::test]
async fn test_letters() {
    recon_test(expect![[r#"
        cat: [a,b,c]
        dog: [e,f,g]
        -> (a, h(b), c) [a,c,e,f,g]
        <- (a, 0, c, 0, e, 0, f, 0, g) [a,b,c,e,f,g]
        -> (a, 0, b, h(c,e,f), g) [a,b,c,e,f,g]
        <- (a, h(b,c,e,f), g) [a,b,c,e,f,g]"#]])
    .await
}

#[tokio::test]
async fn test_one_us() {
    // if there is only one key it is its own message
    recon_test(expect![[r#"
        cat: [a]
        dog: []
        -> (a) [a]
        <- (a) [a]"#]])
    .await
}

#[tokio::test]
async fn test_one_them() {
    recon_test(expect![[r#"
        cat: []
        dog: [a]
        -> () [a]
        <- (a) [a]
        -> (a) [a]"#]])
    .await
}

#[tokio::test]
#[traced_test]
async fn test_none() {
    recon_test(expect![[r#"
        cat: []
        dog: []
        -> () []
        <- () []
        -> () []"#]])
    .await
}

#[tokio::test]
async fn test_two() {
    recon_test(expect![[r#"
        cat: [a,z]
        dog: [a,z]
        -> (a, 0, z) [a,z]
        <- (a, 0, z) [a,z]"#]])
    .await
}

#[tokio::test]
async fn paper() {
    recon_test(expect![[r#"
        cat: [ape,eel,fox,gnu]
        dog: [bee,cat,doe,eel,fox,hog]
        -> (ape, h(eel,fox), gnu) [ape,bee,cat,doe,eel,fox,gnu,hog]
        <- (ape, h(bee,cat), doe, h(eel,fox), gnu, 0, hog) [ape,doe,eel,fox,gnu,hog]
        -> (ape, 0, doe, h(eel,fox,gnu), hog) [ape,bee,cat,doe,eel,fox,gnu,hog]
        <- (ape, 0, bee, 0, cat, h(doe,eel,fox,gnu), hog) [ape,bee,cat,doe,eel,fox,gnu,hog]
        -> (ape, h(bee,cat,doe,eel,fox,gnu), hog) [ape,bee,cat,doe,eel,fox,gnu,hog]
        <- (ape, h(bee,cat,doe,eel,fox,gnu), hog) [ape,bee,cat,doe,eel,fox,gnu,hog]"#]])
    .await;
}

#[tokio::test]
async fn test_small_diff() {
    recon_test(expect![[r#"
        cat: [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z]
        dog: [a,b,c,d,e,f,g,h,i,j,k,l,m,o,p,q,r,s,t,u,w,x,y,z]
        -> (a, h(b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y), z) [a,b,c,d,e,f,g,h,i,j,k,l,m,o,p,q,r,s,t,u,w,x,y,z]
        <- (a, h(b,c,d,e,f,g,h,i,j,k), l, h(m,o,p,q,r,s,t,u,w,x,y), z) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z]
        -> (a, h(b,c,d,e,f,g,h,i,j,k), l, h(m,n,o,p,q,r), s, h(t,u,v,w,x,y), z) [a,b,c,d,e,f,g,h,i,j,k,l,m,o,p,q,r,s,t,u,w,x,y,z]
        <- (a, h(b,c,d,e,f,g,h,i,j,k), l, h(m,o), p, h(q,r), s, h(t,u), w, h(x,y), z) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z]
        -> (a, h(b,c,d,e,f,g,h,i,j,k), l, 0, m, 0, n, 0, o, h(p,q,r), s, 0, t, 0, u, 0, v, h(w,x,y), z) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z]
        <- (a, h(b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y), z) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z]"#]]).await;
}

#[tokio::test]
async fn test_small_example() {
    recon_test(expect![[r#"
    cat: [ape,eel,fox,gnu]
    dog: [bee,cat,doe,eel,fox,hog]
    -> (ape, h(eel,fox), gnu) [ape,bee,cat,doe,eel,fox,gnu,hog]
    <- (ape, h(bee,cat), doe, h(eel,fox), gnu, 0, hog) [ape,doe,eel,fox,gnu,hog]
    -> (ape, 0, doe, h(eel,fox,gnu), hog) [ape,bee,cat,doe,eel,fox,gnu,hog]
    <- (ape, 0, bee, 0, cat, h(doe,eel,fox,gnu), hog) [ape,bee,cat,doe,eel,fox,gnu,hog]
    -> (ape, h(bee,cat,doe,eel,fox,gnu), hog) [ape,bee,cat,doe,eel,fox,gnu,hog]
    <- (ape, h(bee,cat,doe,eel,fox,gnu), hog) [ape,bee,cat,doe,eel,fox,gnu,hog]"#]])
    .await;
}

#[tokio::test]
async fn test_small_diff_off_by_one() {
    recon_test(expect![[r#"
        cat: [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z]
        dog: [a,b,c,d,e,f,g,h,i,j,k,l,m,n,p,q,r,s,t,u,w,x,y,z]
        -> (a, h(b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y), z) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,p,q,r,s,t,u,w,x,y,z]
        <- (a, h(b,c,d,e,f,g,h,i,j,k), l, h(m,n,p,q,r,s,t,u,w,x,y), z) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z]
        -> (a, h(b,c,d,e,f,g,h,i,j,k), l, h(m,n,o,p,q,r), s, h(t,u,v,w,x,y), z) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,p,q,r,s,t,u,w,x,y,z]
        <- (a, h(b,c,d,e,f,g,h,i,j,k), l, h(m,n), p, h(q,r), s, h(t,u), w, h(x,y), z) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z]
        -> (a, h(b,c,d,e,f,g,h,i,j,k), l, 0, m, 0, n, 0, o, h(p,q,r), s, 0, t, 0, u, 0, v, h(w,x,y), z) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z]
        <- (a, h(b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y), z) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z]"#]]).await;
}

#[tokio::test]
async fn test_alternating() {
    recon_test(expect![[r#"
        cat: [a,b,c,e,g,i,k,m,o,p,r,t,v,x,z]
        dog: [a,c,d,f,h,j,l,n,p,q,s,u,w,y,z]
        -> (a, h(b,c,e,g,i,k,m,o,p,r,t,v,x), z) [a,c,d,f,h,j,l,n,p,q,s,u,w,y,z]
        <- (a, h(c,d,f,h,j,l), n, h(p,q,s,u,w,y), z) [a,b,c,e,g,i,k,m,n,o,p,r,t,v,x,z]
        -> (a, h(b,c,e), g, h(i,k,m), n, h(o,p), r, h(t,v,x), z) [a,c,d,f,g,h,j,l,n,p,q,r,s,u,w,y,z]
        <- (a, 0, c, 0, d, 0, f, 0, g, 0, h, 0, j, 0, l, 0, n, 0, p, 0, q, 0, r, h(s), u, h(w,y), z) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,t,u,v,x,z]
        -> (a, 0, b, h(c), d, 0, e, h(f,g), h, 0, i, 0, j, 0, k, 0, l, 0, m, 0, n, 0, o, h(p,q), r, 0, t, 0, u, 0, v, 0, x, 0, z) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z]
        <- (a, h(b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q), r, 0, s, h(t,u), v, 0, w, 0, x, 0, y, 0, z) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z]
        -> (a, h(b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y), z) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z]
        <- (a, h(b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y), z) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z]"#]]).await;
}

#[tokio::test]
#[traced_test]
async fn test_small_diff_zz() {
    recon_test(expect![[r#"
        cat: [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z,zz]
        dog: [a,b,c,d,e,f,g,h,i,j,k,l,m,n,p,q,r,s,t,u,w,x,y,z]
        -> (a, h(b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z), zz) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,p,q,r,s,t,u,w,x,y,z,zz]
        <- (a, h(b,c,d,e,f,g,h,i,j,k,l), m, h(n,p,q,r,s,t,u,w,x,y,z), zz) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z,zz]
        -> (a, h(b,c,d,e,f,g,h,i,j,k,l), m, h(n,o,p,q,r,s), t, h(u,v,w,x,y,z), zz) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,p,q,r,s,t,u,w,x,y,z,zz]
        <- (a, h(b,c,d,e,f,g,h,i,j,k,l), m, h(n,p), q, h(r,s), t, h(u,w), x, h(y,z), zz) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z,zz]
        -> (a, h(b,c,d,e,f,g,h,i,j,k,l), m, 0, n, 0, o, 0, p, h(q,r,s), t, 0, u, 0, v, 0, w, h(x,y,z), zz) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z,zz]
        <- (a, h(b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z), zz) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z,zz]"#]]).await;
}

#[tokio::test]
#[traced_test]
async fn test_subset_interest() {
    recon_test(expect![[r#"
        cat: <(b,i),(m,r)> [c,f,g]
        dog: <(a,z)> [b,c,d,e,f,g,h,i,j,k,l,m,n]
        -> (<b, c, h(f), g, i>), (<m, r>) [b,c,d,e,f,g,h,i,j,k,l,m,n]
        <- (<b, c, 0, d, 0, e, 0, f, h(g), h, i>), (<m, n, r>) [c,d,e,f,g,h,n]
        -> (<b, c, h(d,e,f,g), h, i>), (<m, n, r>) [b,c,d,e,f,g,h,i,j,k,l,m,n]
        <- (<b, c, h(d,e,f,g), h, i>), (<m, n, r>) [c,d,e,f,g,h,n]"#]])
    .await;
}

#[tokio::test]
async fn test_partial_interest() {
    recon_test(expect![[r#"
        cat: <(b,g),(i,q)> [a,b,c,d,e,f,g,h,i,j,k,l,m,o,p,q]
        dog: <(k,t),(u,z)> [j,k,n,o,p,q,r,s,t,u,w,x,y,z]
        -> (<b, c, h(d,e), f, g>), (<i, j, h(k,l,m,o), p, q>) [j,k,n,o,p,q,r,s,t,u,w,x,y,z]
        <- (<k, n, 0, o, 0, p, q>) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q]
        -> (<k, l, 0, m, h(n,o), p, q>) [j,k,l,m,n,o,p,q,r,s,t,u,w,x,y,z]
        <- (<k, l, h(m,n,o), p, q>) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q]
        -> (<k, l, h(m,n,o), p, q>) [j,k,l,m,n,o,p,q,r,s,t,u,w,x,y,z]"#]])
    .await;
}

#[tokio::test]
async fn message_cbor_serialize_test() {
    let received: Message<Bytes, Sha256a> = parse_recon(
        r#"cat: [] dog: []
        -> (a, h(b), c) []"#,
    )
    .iterations[0]
        .messages[0]
        .clone()
        .into();
    let received_cbor = hex::encode(serde_ipld_dagcbor::ser::to_vec(&received).unwrap());
    println!("serde_json {}", serde_json::to_string(&received).unwrap()); // Message as json
    expect!["a4657374617274f663656e64f6646b6579738241614163666861736865738158203e23e8160039594a33894f6564e1b1348bbd7a0088d42c4acb73eeaed59c009d"].assert_eq(&received_cbor);
}

#[test]
fn message_cbor_deserialize_test() {
    let bytes = hex::decode("a2646b6579738241614163666861736865738158203e23e8160039594a33894f6564e1b1348bbd7a0088d42c4acb73eeaed59c009d").unwrap();
    let x = serde_ipld_dagcbor::de::from_slice(bytes.as_slice());
    println!("{:?}", x);
    let received: Message<Bytes, Sha256a> = x.unwrap();
    expect![[r#"
        Message {
            start: None,
            end: None,
            keys: [
                Bytes(
                    "a",
                ),
                Bytes(
                    "c",
                ),
            ],
            hashes: [
                Sha256a {
                    hex: "3E23E8160039594A33894F6564E1B1348BBD7A0088D42C4ACB73EEAED59C009D",
                    u32_8: [
                        384312126,
                        1247361280,
                        1699711283,
                        884072804,
                        8043915,
                        1244451976,
                        2934862795,
                        2634063061,
                    ],
                },
            ],
        }
    "#]]
    .assert_debug_eq(&received);
}

#[test]
fn message_cbor_serialize_zero_hash() {
    let received: Message<Bytes, Sha256a> = parse_recon(
        r#"cat: [] dog: []
        -> (a, 0, c) []"#,
    )
    .iterations[0]
        .messages[0]
        .clone()
        .into();
    let received_cbor = hex::encode(serde_ipld_dagcbor::ser::to_vec(&received).unwrap());
    println!("serde_json {}", serde_json::to_string(&received).unwrap()); // Message as json
    expect!["a4657374617274f663656e64f6646b6579738241614163666861736865738140"]
        .assert_eq(&received_cbor);
}
#[test]
fn message_cbor_deserialize_zero_hash() {
    let bytes = hex::decode("a2646b6579738241614163666861736865738140").unwrap();
    let x = serde_ipld_dagcbor::de::from_slice(bytes.as_slice());
    println!("{:?}", x);
    let received: Message<Bytes, Sha256a> = x.unwrap();
    expect![[r#"
        Message {
            start: None,
            end: None,
            keys: [
                Bytes(
                    "a",
                ),
                Bytes(
                    "c",
                ),
            ],
            hashes: [
                Sha256a {
                    hex: "0000000000000000000000000000000000000000000000000000000000000000",
                    u32_8: [
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                    ],
                },
            ],
        }
    "#]]
    .assert_debug_eq(&received);
}
