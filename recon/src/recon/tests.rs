lalrpop_mod!(
    #[allow(clippy::all, missing_debug_implementations)]
    pub parser, "/recon/parser.rs"
); // synthesized by LALRPOP

use crate::Sha256a;
use rusqlite::{Connection, Result};
use std::collections::BTreeSet;
use std::fmt::Display;

use super::*;
use codespan_reporting::{
    diagnostic::{Diagnostic, Label},
    files::SimpleFiles,
    term::{self, termcolor::Buffer},
};
use expect_test::{expect, Expect};
use lalrpop_util::ParseError;
use pretty::{Arena, DocAllocator, DocBuilder, Pretty};

pub use super::Recon;
pub type Set = BTreeSet<String>;

#[derive(Debug, Clone, Default, PartialEq)]
struct MemoryAHash {
    ahash: Sha256a,
    set: BTreeSet<EventId>,
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

impl crate::recon::AssociativeHash for MemoryAHash {
    fn is_zero(&self) -> bool {
        self.ahash.is_zero()
    }

    fn clear(&mut self) {
        self.ahash.clear();
        self.set.clear();
    }

    fn push(&mut self, key: (&EventId, &Sha256a)) {
        self.ahash.push(key);
        self.set.insert(key.0.to_owned());
    }

    fn digest_many<'a, I>(keys: I) -> Self
    where
        I: Iterator<Item = (&'a EventId, &'a Sha256a)>,
    {
        let mut hash = Self::default();
        for i in keys {
            hash.push(i);
        }
        hash
    }

    fn to_bytes(&self) -> Vec<u8> {
        self.ahash.to_bytes()
    }

    fn to_hex(&self) -> String {
        self.ahash.to_hex()
    }
}

impl From<Message<MemoryAHash>> for MessageData {
    fn from(value: Message<MemoryAHash>) -> Self {
        Self {
            keys: value.keys.iter().map(|key| key.to_string()).collect(),
            ahashs: value
                .ahashs
                .into_iter()
                .map(|h| h.set.iter().map(|key| key.to_string()).collect())
                .collect(),
        }
    }
}

#[derive(Debug)]
pub struct Record {
    cat: Recon,
    dog: Recon,
    iterations: Vec<Iteration>,
}

impl<'a, D, A> Pretty<'a, D, A> for &'a Record
where
    A: 'a + Clone,
    D: DocAllocator<'a, A>,
    D::Doc: Clone,
{
    fn pretty(self, allocator: &'a D) -> DocBuilder<'a, D, A> {
        let peer = |name, set: &BTreeMap<EventId, Sha256a>| {
            let separator = allocator.text(",").append(allocator.softline_());
            allocator.text(name).append(allocator.text(": ")).append(
                allocator
                    .intersperse(
                        set.iter()
                            .map(|x: (&EventId, &Sha256a)| allocator.text(x.0.to_string())),
                        separator,
                    )
                    .brackets(),
            )
        };

        allocator
            .nil()
            .append(peer("cat", &self.cat.keys))
            .group()
            .append(allocator.hardline())
            .append(peer("dog", &self.dog.keys))
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
    pub keys: Vec<String>, // keys must be 1 longer then ahashs unless both are empty
    pub ahashs: Vec<BTreeSet<String>>, // ahashs must be 1 shorter then keys
}

impl<H> From<MessageData> for Message<H>
where
    H: AssociativeHash,
{
    fn from(value: MessageData) -> Self {
        Self {
            keys: value.keys.iter().map(|key| key.as_bytes().into()).collect(),
            ahashs: value
                .ahashs
                .into_iter()
                .map(|set| H::digest_many(Recon::from_set(set).keys.iter()))
                .collect(),
        }
    }
}

#[derive(Debug)]
pub struct Iteration {
    dir: Direction,
    msg: MessageData,
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
        // Construct doc for msg
        let mut msg = allocator.nil();
        let l = self.msg.keys.len();
        for i in 0..l {
            msg = msg.append(allocator.text(self.msg.keys[i].as_str()));
            if i < l - 1 {
                msg = msg.append(space_sep.clone());
                if self.msg.ahashs[i].is_empty() {
                    msg = msg.append(allocator.text("0")).append(space_sep.clone());
                } else {
                    msg = msg
                        .append(allocator.text("h("))
                        .append(allocator.intersperse(
                            self.msg.ahashs[i].iter().map(|s| allocator.text(s)),
                            no_space_sep.clone(),
                        ))
                        .append(allocator.text(")"))
                        .append(space_sep.clone());
                }
            }
        }
        // Construct doc for set
        let set = allocator.intersperse(self.set.iter().map(|s| allocator.text(s)), no_space_sep);

        // Put it all together
        dir.append(msg.parens())
            .append(allocator.softline())
            .append(set.brackets())
            .hang(4)
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
}

// Help implementation to construct a message from parsed [`MessageItem`]
impl TryFrom<(Option<MessageItem>, Vec<MessageItem>)> for MessageData {
    type Error = &'static str;

    fn try_from(value: (Option<MessageItem>, Vec<MessageItem>)) -> Result<Self, Self::Error> {
        let mut keys = Vec::new();
        let mut ahashs = Vec::new();
        match value.0 {
            Some(MessageItem::Key(k)) => keys.push(k),
            Some(MessageItem::Hash(_)) => return Err("message cannot begin with a hash"),
            None => {}
        };
        for item in value.1 {
            match item {
                MessageItem::Key(k) => keys.push(k),
                MessageItem::Hash(set) => {
                    ahashs.push(set);
                }
            }
        }
        if !keys.is_empty() && keys.len() - 1 != ahashs.len() {
            return Err("invalid message, unmatched keys and hashes");
        }
        Ok(MessageData { keys, ahashs })
    }
}

#[test]
fn word_lists() {
    fn recon_from_string(s: &str) -> Recon {
        let mut r = Recon::default();
        for key in s.split([' ', '\n']).map(|s| s.to_string()) {
            if !s.is_empty() {
                r.insert(&key.as_bytes().into());
            }
        }
        r
    }
    let mut peers = vec![
        recon_from_string(include_str!("../tests/bip_39.txt")),
        recon_from_string(include_str!("../tests/eff_large_wordlist.txt")),
        recon_from_string(include_str!("../tests/eff_short_wordlist_1.txt")),
        recon_from_string(include_str!("../tests/eff_short_wordlist_2.txt")),
        recon_from_string(include_str!("../tests/wordle_words5_big.txt")),
        recon_from_string(include_str!("../tests/wordle_words5.txt")),
        // recon_from_string(include_str!("../tests/connectives.txt")),
        // recon_from_string(include_str!("../tests/propernames.txt")),
        // recon_from_string(include_str!("../tests/web2.txt")),
        // recon_from_string(include_str!("../tests/web2a.txt")),
    ];
    let keys_len = 21139;
    // let expected_first = "aahed";
    // let expected_last = "zythum";
    // let expected_ahash = "13BA255FBD4C2566CB2564EFA0C1782ABA61604AC07A8789D1DF9E391D73584E";

    for peer in &peers {
        println!(
            "peer  {} {} {}",
            peer.keys.first_key_value().unwrap().0,
            peer.keys.len(),
            peer.keys.last_key_value().unwrap().0,
        )
    }

    let mut local = Recon::default();
    fn sync(local: &mut Recon, peers: &mut [Recon]) {
        for j in 0..3 {
            for (i, peer) in peers.iter_mut().enumerate() {
                println!(
                    "round:{} peer:{}\n\t[{}]\n\t[{}]",
                    j,
                    i,
                    local.keys.len(),
                    peer.keys.len()
                );
                let mut next = local.first_message();
                for k in 0..50 {
                    println!(
                        "\t{}: -> {}[{}]",
                        k,
                        if next.keys.len() < 10 {
                            format!("{}", next)
                        } else {
                            format!("({})", next.keys.len())
                        },
                        local.keys.len()
                    );

                    let response = peer.process_message::<Sha256a>(&next);

                    println!(
                        "\t{}: <- {}[{}]",
                        k,
                        if response.msg.keys.len() < 10 {
                            format!("{}", response.msg)
                        } else {
                            format!("({})", response.msg.keys.len())
                        },
                        peer.keys.len(),
                    );

                    next = local.process_message(&response.msg).msg;

                    if response.msg.keys.len() < 3 && next.keys.len() < 3 {
                        println!("\tpeers[{}] in sync", i);
                        break;
                    }
                }
            }
        }
    }
    sync(&mut local, &mut peers);
    for peer in &peers {
        println!(
            "after {} {} {}",
            peer.keys.first_key_value().unwrap().0,
            peer.keys.len(),
            peer.keys.last_key_value().unwrap().0,
        )
    }

    assert_eq!(local.keys.len(), keys_len);
    for peer in &peers {
        assert_eq!(peer.keys.len(), keys_len)
    }
    expect![[r#"
        [
            "aahed",
            "zymic",
        ]
    "#]]
    .assert_debug_eq(
        &local
            .first_message::<Sha256a>()
            .keys
            .iter()
            .map(|k| k.to_string())
            .collect::<Vec<String>>(),
    );
    expect![["13BA255FBD4C2566CB2564EFA0C1782ABA61604AC07A8789D1DF9E391D73584E"]]
        .assert_eq(&local.first_message::<Sha256a>().ahashs[0].to_hex());

    local.insert(&b"ceramic".as_slice().into());
    sync(&mut local, &mut peers);
}
#[test]
fn response_is_synchronized() {
    let mut a = Recon::from_set(BTreeSet::from_iter([
        "a".to_owned(),
        "b".to_owned(),
        "c".to_owned(),
        "n".to_owned(),
    ]));
    let mut x = Recon::from_set(BTreeSet::from_iter([
        "x".to_owned(),
        "y".to_owned(),
        "z".to_owned(),
        "n".to_owned(),
    ]));
    let response = x.process_message::<Sha256a>(&a.first_message());
    assert!(!response.is_synchronized);
    let response = a.process_message(&response.msg);
    assert!(!response.is_synchronized);
    let response = x.process_message(&response.msg);
    assert!(!response.is_synchronized);

    // After this message we should be synchronized
    let response = a.process_message(&response.msg);
    assert!(response.is_synchronized);
    let response = x.process_message(&response.msg);
    assert!(response.is_synchronized);
}

#[test]
fn hello() {
    let other_hash = Recon {
        keys: BTreeMap::from([
            (b"hello".as_slice().into(), Sha256a::digest("hello")),
            (b"world".as_slice().into(), Sha256a::digest("world")),
        ]),
    };
    expect![[r#"
    Recon {
        keys: {
            EventId(
                [
                    104,
                    101,
                    108,
                    108,
                    111,
                ],
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
            EventId(
                [
                    119,
                    111,
                    114,
                    108,
                    100,
                ],
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
    }
    "#]]
    .assert_debug_eq(&other_hash)
}

#[test]
fn abcde() {
    recon_test(expect![[r#"
        cat: [b,c,d,e]
        dog: [a,e]
        -> (b, h(c,d), e) [a,b,e]
        <- (a, 0, b, 0, e) [a,b,c,d,e]
        -> (a, 0, b, 0, c, 0, d, 0, e) [a,b,c,d,e]
        <- (a, h(b,c,d), e) [a,b,c,d,e]"#]])
}

#[test]
fn two_in_a_row() {
    recon_test(expect![[r#"
    cat: [a,b,c,d,e]
    dog: [a,d,e]
    -> (a, h(b,c,d), e) [a,d,e]
    <- (a, 0, d, 0, e) [a,b,c,d,e]
    -> (a, 0, b, 0, c, h(d), e) [a,b,c,d,e]
    <- (a, h(b,c,d), e) [a,b,c,d,e]"#]])
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
            keys: {
                EventId(
                    [
                        97,
                    ],
                ): Sha256a {
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
                EventId(
                    [
                        98,
                    ],
                ): Sha256a {
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
                EventId(
                    [
                        99,
                    ],
                ): Sha256a {
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
            },
        },
        dog: Recon {
            keys: {
                EventId(
                    [
                        101,
                    ],
                ): Sha256a {
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
                EventId(
                    [
                        102,
                    ],
                ): Sha256a {
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
                EventId(
                    [
                        103,
                    ],
                ): Sha256a {
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
            },
        },
        iterations: [
            Iteration {
                dir: CatToDog,
                msg: MessageData {
                    keys: [
                        "a",
                        "c",
                    ],
                    ahashs: [
                        {
                            "b",
                        },
                    ],
                },
                set: {
                    "a",
                    "c",
                    "e",
                    "f",
                    "g",
                },
            },
            Iteration {
                dir: DogToCat,
                msg: MessageData {
                    keys: [
                        "a",
                        "c",
                        "g",
                    ],
                    ahashs: [
                        {},
                        {
                            "e",
                            "f",
                        },
                    ],
                },
                set: {
                    "a",
                    "b",
                    "c",
                    "g",
                },
            },
            Iteration {
                dir: CatToDog,
                msg: MessageData {
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
                set: {
                    "a",
                    "b",
                    "c",
                    "e",
                    "f",
                    "g",
                },
            },
            Iteration {
                dir: DogToCat,
                msg: MessageData {
                    keys: [
                        "a",
                        "c",
                        "e",
                        "f",
                        "g",
                    ],
                    ahashs: [
                        {
                            "b",
                        },
                        {},
                        {},
                        {},
                    ],
                },
                set: {
                    "a",
                    "b",
                    "c",
                    "e",
                    "f",
                    "g",
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
            keys: {
                EventId(
                    [
                        97,
                    ],
                ): Sha256a {
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
            },
        },
        dog: Recon {
            keys: {},
        },
        iterations: [
            Iteration {
                dir: CatToDog,
                msg: MessageData {
                    keys: [
                        "a",
                    ],
                    ahashs: [],
                },
                set: {
                    "a",
                },
            },
            Iteration {
                dir: DogToCat,
                msg: MessageData {
                    keys: [
                        "a",
                    ],
                    ahashs: [],
                },
                set: {
                    "a",
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
fn recon_do(recon: &str) -> Record {
    let mut record = parse_recon(recon);
    // Remember initial state
    let cat = record.cat.keys.clone();
    let dog = record.dog.keys.clone();

    let n = record.iterations.len();
    record.iterations.clear();

    // Run simulation for the number of iterations in the original record
    let mut dir = Direction::CatToDog;
    let mut msg: Message<MemoryAHash> = record.cat.first_message();
    for _ in 0..n {
        let (next_dir, response, set) = match dir {
            Direction::CatToDog => (
                Direction::DogToCat,
                record.dog.process_message(&msg),
                record.dog.keys.clone(),
            ),
            Direction::DogToCat => (
                Direction::CatToDog,
                record.cat.process_message(&msg),
                record.cat.keys.clone(),
            ),
        };
        record.iterations.push(Iteration {
            dir,
            msg: msg.into(),
            set: set
                .iter()
                .map(|entry: (&EventId, &Sha256a)| entry.0.to_string())
                .collect(),
        });
        dir = next_dir;
        msg = response.msg;
    }
    // Restore initial sets
    record.cat.keys = cat;
    record.dog.keys = dog;
    record
}

fn recon_test(recon: Expect) {
    let actual = format!("{}", recon_do(recon.data()));
    recon.assert_eq(&actual)
}

#[test]
fn abcd() {
    recon_test(expect![[r#"
        cat: [b,c,d,e]
        dog: [a,e]
        -> (b, h(c,d), e) [a,b,e]
        <- (a, 0, b, 0, e) [a,b,c,d,e]
        -> (a, 0, b, 0, c, 0, d, 0, e) [a,b,c,d,e]
        <- (a, h(b,c,d), e) [a,b,c,d,e]"#]])
}
#[test]
fn test_letters() {
    recon_test(expect![[r#"
        cat: [a,b,c]
        dog: [e,f,g]
        -> (a, h(b), c) [a,c,e,f,g]
        <- (a, 0, c, 0, e, 0, f, 0, g) [a,b,c,e,f,g]
        -> (a, 0, b, h(c,e,f), g) [a,b,c,e,f,g]
        <- (a, h(b,c,e,f), g) [a,b,c,e,f,g]"#]]);
}

#[test]
fn test_one_us() {
    // if there is only one key it is its own message
    recon_test(expect![[r#"
        cat: [a]
        dog: []
        -> (a) [a]
        <- (a) [a]"#]])
}

#[test]
fn test_one_them() {
    recon_test(expect![[r#"
        cat: []
        dog: [a]
        -> () [a]
        <- (a) [a]
        -> (a) [a]"#]])
}

#[test]
fn test_none() {
    recon_test(expect![[r#"
        cat: []
        dog: []
        -> () []
        <- () []
        -> () []"#]])
}

#[test]
fn test_two() {
    recon_test(expect![[r#"
        cat: [a,z]
        dog: [a,z]
        -> (a, 0, z) [a,z]
        <- (a, 0, z) [a,z]"#]])
}

#[test]
fn paper() {
    recon_test(expect![[r#"
        cat: [ape,eel,fox,gnu]
        dog: [bee,cat,doe,eel,fox,hog]
        -> (ape, h(eel,fox), gnu) [ape,bee,cat,doe,eel,fox,gnu,hog]
        <- (ape, h(bee,cat), doe, h(eel,fox), gnu, 0, hog) [ape,doe,eel,fox,gnu,hog]
        -> (ape, 0, doe, h(eel,fox,gnu), hog) [ape,bee,cat,doe,eel,fox,gnu,hog]
        <- (ape, 0, bee, 0, cat, h(doe,eel,fox,gnu), hog) [ape,bee,cat,doe,eel,fox,gnu,hog]
        -> (ape, h(bee,cat,doe,eel,fox,gnu), hog) [ape,bee,cat,doe,eel,fox,gnu,hog]
        <- (ape, h(bee,cat,doe,eel,fox,gnu), hog) [ape,bee,cat,doe,eel,fox,gnu,hog]"#]]);
}

#[test]
fn test_small_diff() {
    recon_test(expect![[r#"
        cat: [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z]
        dog: [a,b,c,d,e,f,g,h,i,j,k,l,m,o,p,q,r,s,t,u,w,x,y,z]
        -> (a, h(b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y), z) [a,b,c,d,e,f,g,h,i,j,k,l,m,o,p,q,r,s,t,u,w,x,y,z]
        <- (a, h(b,c,d,e,f,g,h,i,j,k), l, h(m,o,p,q,r,s,t,u,w,x,y), z) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z]
        -> (a, h(b,c,d,e,f,g,h,i,j,k), l, h(m,n,o,p,q,r), s, h(t,u,v,w,x,y), z) [a,b,c,d,e,f,g,h,i,j,k,l,m,o,p,q,r,s,t,u,w,x,y,z]
        <- (a, h(b,c,d,e,f,g,h,i,j,k), l, h(m,o), p, h(q,r), s, h(t,u), w, h(x,y), z) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z]
        -> (a, h(b,c,d,e,f,g,h,i,j,k), l, 0, m, 0, n, 0, o, h(p,q,r), s, 0, t, 0, u, 0, v, h(w,x,y), z) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z]
        <- (a, h(b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y), z) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z]"#]]);
}

#[test]
fn test_small_example() {
    recon_test(expect![[r#"
    cat: [ape,eel,fox,gnu]
    dog: [bee,cat,doe,eel,fox,hog]
    -> (ape, h(eel,fox), gnu) [ape,bee,cat,doe,eel,fox,gnu,hog]
    <- (ape, h(bee,cat), doe, h(eel,fox), gnu, 0, hog) [ape,doe,eel,fox,gnu,hog]
    -> (ape, 0, doe, h(eel,fox,gnu), hog) [ape,bee,cat,doe,eel,fox,gnu,hog]
    <- (ape, 0, bee, 0, cat, h(doe,eel,fox,gnu), hog) [ape,bee,cat,doe,eel,fox,gnu,hog]
    -> (ape, h(bee,cat,doe,eel,fox,gnu), hog) [ape,bee,cat,doe,eel,fox,gnu,hog]
    <- (ape, h(bee,cat,doe,eel,fox,gnu), hog) [ape,bee,cat,doe,eel,fox,gnu,hog]"#]]);
}

#[test]
fn test_small_diff_off_by_one() {
    recon_test(expect![[r#"
        cat: [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z]
        dog: [a,b,c,d,e,f,g,h,i,j,k,l,m,n,p,q,r,s,t,u,w,x,y,z]
        -> (a, h(b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y), z) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,p,q,r,s,t,u,w,x,y,z]
        <- (a, h(b,c,d,e,f,g,h,i,j,k), l, h(m,n,p,q,r,s,t,u,w,x,y), z) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z]
        -> (a, h(b,c,d,e,f,g,h,i,j,k), l, h(m,n,o,p,q,r), s, h(t,u,v,w,x,y), z) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,p,q,r,s,t,u,w,x,y,z]
        <- (a, h(b,c,d,e,f,g,h,i,j,k), l, h(m,n), p, h(q,r), s, h(t,u), w, h(x,y), z) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z]
        -> (a, h(b,c,d,e,f,g,h,i,j,k), l, 0, m, 0, n, 0, o, h(p,q,r), s, 0, t, 0, u, 0, v, h(w,x,y), z) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z]
        <- (a, h(b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y), z) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z]"#]]);
}

#[test]
fn test_alternating() {
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
        <- (a, h(b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y), z) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z]"#]]);
}

#[test]
fn test_small_diff_zz() {
    recon_test(expect![[r#"
        cat: [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z,zz]
        dog: [a,b,c,d,e,f,g,h,i,j,k,l,m,n,p,q,r,s,t,u,w,x,y,z]
        -> (a, h(b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z), zz) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,p,q,r,s,t,u,w,x,y,z,zz]
        <- (a, h(b,c,d,e,f,g,h,i,j,k,l), m, h(n,p,q,r,s,t,u,w,x,y,z), zz) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z,zz]
        -> (a, h(b,c,d,e,f,g,h,i,j,k,l), m, h(n,o,p,q,r,s), t, h(u,v,w,x,y,z), zz) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,p,q,r,s,t,u,w,x,y,z,zz]
        <- (a, h(b,c,d,e,f,g,h,i,j,k,l), m, h(n,p), q, h(r,s), t, h(u,w), x, h(y,z), zz) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z,zz]
        -> (a, h(b,c,d,e,f,g,h,i,j,k,l), m, 0, n, 0, o, 0, p, h(q,r,s), t, 0, u, 0, v, 0, w, h(x,y,z), zz) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z,zz]
        <- (a, h(b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z), zz) [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z,zz]"#]]);
}

#[test]
fn message_cbor_serialize_test() {
    let received: Message<Sha256a> = parse_recon(
        r#"cat: [] dog: []
        -> (a, h(b), c) []"#,
    )
    .iterations[0]
        .msg
        .clone()
        .into();
    let received_cbor = hex::encode(serde_ipld_dagcbor::ser::to_vec(&received).unwrap());
    println!("serde_json {}", serde_json::to_string(&received).unwrap()); // Message as json
    expect![["a2616b824161416361688158203e23e8160039594a33894f6564e1b1348bbd7a0088d42c4acb73eeaed59c009d"]].assert_eq(&received_cbor);
}

#[test]
fn message_cbor_deserialize_test() {
    let bytes = hex::decode("a2616b824161416361688158203e23e8160039594a33894f6564e1b1348bbd7a0088d42c4acb73eeaed59c009d").unwrap();
    let x = serde_ipld_dagcbor::de::from_slice(bytes.as_slice());
    println!("{:?}", x);
    let received: Message<Sha256a> = x.unwrap();
    expect![[r#"
        Message {
            keys: [
                EventId(
                    [
                        97,
                    ],
                ),
                EventId(
                    [
                        99,
                    ],
                ),
            ],
            ahashs: [
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
fn sqlite3_test() {
    // If we store all the Recon keys in sqlite
    // then we can use the sum function to calculate ahashs for the ranges
    // remember to take the mod 2^32 before reconstructing AHash.
    fn store() -> Result<()> {
        let conn = Connection::open(":memory:")?; // "my_database.db"
        conn.execute(
            r#"
        CREATE TABLE data (
            key TEXT,
            h0 INTEGER, h1 INTEGER, h2 INTEGER, h3 INTEGER,
            h4 INTEGER, h5 INTEGER, h6 INTEGER, h7 INTEGER
        )
        "#,
            (),
        )?;

        println!("key2 {:?}", Sha256a::digest("key2"));
        // Insert the data into the table
        let r1 = conn.execute(
            r#"
            INSERT INTO data (
                key, 
                h0, h1, h2, h3, h4, h5, h6, h7
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?
            )"#,
            (
                "key1",
                2517202049_u32,
                56037440_u32,
                3620594420_u32,
                3669165004_u32,
                3107969998_u32,
                442180962_u32,
                3392393391_u32,
                806716350_u32,
            ),
        )?;
        let r2 = conn.execute(
            r#"
              INSERT INTO data (key, h0, h1, h2, h3, h4, h5, h6, h7) 
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
            (
                "key2",
                1985151665_u32,
                1059294028_u32,
                3796006323_u32,
                3032940852_u32,
                4188464464_u32,
                1513824117_u32,
                1735347969_u32,
                2644098280_u32,
            ),
        )?;
        let mut stmt = conn.prepare(
            r#"
        SELECT key, h0, h1, h2, h3, h4, h5, h6, h7 
        FROM data
        "#,
        )?;
        let r3 = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0).unwrap(),
                row.get::<_, u32>(1).unwrap(),
                row.get::<_, u32>(2).unwrap(),
                row.get::<_, u32>(3).unwrap(),
                row.get::<_, u32>(4).unwrap(),
                row.get::<_, u32>(5).unwrap(),
                row.get::<_, u32>(6).unwrap(),
                row.get::<_, u32>(7).unwrap(),
                row.get::<_, u32>(8).unwrap(),
            ))
        });
        println!("{:?} {:?}", r1, r2);
        for row in r3? {
            println!("{:?}", row.unwrap())
        }

        let mut stmt2 = conn.prepare(
            r#"
            SELECT
              sum(h0), sum(h1), sum(h2), sum(h3), sum(h4), sum(h5), sum(h6), sum(h7)
            FROM data
            WHERE key > 'k' AND key < 'l';
            "#,
        )?;
        let r4 = stmt2.query_map([], |row| {
            Ok((
                // here be the integer overflow dragon
                // if there are more then 2^32 ints we risk u64 overflow
                row.get::<_, u64>(0).unwrap() & 0xFFFFFFFF,
                row.get::<_, u64>(1).unwrap() & 0xFFFFFFFF,
                row.get::<_, u64>(2).unwrap() & 0xFFFFFFFF,
                row.get::<_, u64>(3).unwrap() & 0xFFFFFFFF,
                row.get::<_, u64>(4).unwrap() & 0xFFFFFFFF,
                row.get::<_, u64>(5).unwrap() & 0xFFFFFFFF,
                row.get::<_, u64>(6).unwrap() & 0xFFFFFFFF,
                row.get::<_, u64>(7).unwrap() & 0xFFFFFFFF,
            ))
        });
        for row in r4? {
            println!("{:?}", row.unwrap())
        }

        Ok(())
    }
    store().unwrap();
}
