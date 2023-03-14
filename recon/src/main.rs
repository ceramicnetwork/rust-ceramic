use recon::{AHash, Hash, Message, Recon};
// use libp2p::request_response;

fn main() {
    println!("AHash:");
    println!("{}", AHash::digest("").to_hex());

    let ah = AHash::digest("hello") + AHash::digest("world");
    println!("{:#?}", ah);
    println!("{}", ah);

    println!("\nRecon:");
    let mut cat = Recon::default();
    cat.insert("a");
    cat.insert("c");
    cat.insert("e");
    let received = Message {
        keys: vec!["b".to_owned(), "d".to_owned()],
        ahashs: vec![AHash::identity()],
    };

    println!("Before {:?}", cat);
    println!("-> {}", received);
    let response = cat.process_message(&received);
    println!("<- {}", response);
    println!("After {:?}", cat);

    println!("serde_json {}", serde_json::to_string(&received).unwrap()); // Message as json
    println!(
        "serde_ipld_dagcbor_hex {}",
        hex::encode(serde_ipld_dagcbor::ser::to_vec(&received).unwrap())
    ); // Message as dag cbor hex

    // request_response
}
