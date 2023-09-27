use crate::sql;
use crate::Network;
use anyhow::Result;
use ceramic_core::EventId;
use ceramic_p2p::SQLiteBlockStore;
use cid::{
    multihash::{Code, MultihashDigest},
    Cid,
};
use clap::Args;
use futures_util::StreamExt;
use minicbor::{data::Type, display, Decoder};
use std::collections::BTreeSet;
use std::collections::btree_map::BTreeMap;
use std::path::PathBuf;
use tracing::debug;

#[derive(Args, Debug)]
pub struct EventsOpts {
    /// scan the blockstore for events at startup
    #[arg(long, default_value_t = false, env = "CERAMIC_SCAN_BLOCKSTORE")]
    scan_blockstore: bool,

    /// Path to storage directory
    #[arg(short, long, env = "CERAMIC_ONE_STORE_DIR")]
    store_dir: Option<PathBuf>,

    /// Unique key used to find other Ceramic peers via the DHT
    #[arg(long, default_value = "testnet-clay", env = "CERAMIC_ONE_NETWORK")]
    network: Network,

    /// Unique key used to find other Ceramic peers via the DHT
    #[arg(long, env = "CERAMIC_ONE_LOCAL_NETWORK_ID")]
    local_network_id: Option<u32>,
}

pub async fn events(opts: EventsOpts) -> Result<()> {
    let dir = match opts.store_dir.clone() {
        Some(dir) => dir,
        None => match home::home_dir() {
            Some(home_dir) => home_dir.join(".ceramic-one"),
            None => PathBuf::from(".ceramic-one"),
        },
    };
    debug!("using directory: {}", dir.display());
    println!("using directory: {}", dir.display());
    let sql_db_path: PathBuf = dir.join("db.sqlite3");
    let sql_pool: sqlx::Pool<sqlx::Sqlite> = sql::connect(&sql_db_path).await?;
    let sql_store: SQLiteBlockStore = SQLiteBlockStore::new(sql_pool).await.unwrap();

    if opts.scan_blockstore {
        scan_blockstore(opts, sql_store).await
    } else {
        Ok(())
    }
}

// scan the blockstore and add found events to the recon table.
async fn scan_blockstore(opts: EventsOpts, sql_store: SQLiteBlockStore) -> Result<()> {
    // dag-cbor
    // {"header": {"model": "kmodelid"
    //             "controllers": ["did:method:method-specific-id"]}}
    //
    // dag-jose
    // {"payload": CID,
    //  "signatures": [{"protected": b'{"alg":"EdDSA","cap":"ipfs://CID","kid":"did:key:*"}',
    //                  "signature": SIG}]}
    //
    // if not cbor continue
    // if not a map continue
    // if map lacks header key continue
    // if header is not map continue
    // if header lacks controllers continue
    // if controllers is not array continue
    // if header lacks model continue
    // if prev in header follow prev back to init to find hight
    // build eventID.new(
    //   network: &Network, // e.g. Network::Mainnet
    //   sort_key: &str,    // e.g. "model"
    //   sort_value: &str,  // e.g. "kh4q0ozorrgaq2mezktnrmdwleo1d" // cspell:disable-line
    //   controller: &str,  // e.g. "did:key:z6MkgSV3tAuw7gUWqKCUY7ae6uWNxqYgdwPhUJbJhF9EFXm9"
    //   init: &Cid, // e.g. Cid::from_str("bagcqceraplay4erv6l32qrki522uhiz7rf46xccwniw7ypmvs3cvu2b3oulq") // cspell:disable-line
    //   event_height: u64, // e.g. 1
    //   event_cid: &Cid, // e.g. Cid::from_str("bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy") // cspell:disable-line
    // )

    //for (multihash, bytes) in store.getAllBlocks() {
    let mut key_sets = BTreeMap::<String, u64>::new();
    let mut count = 0;
    let mut count_map = 0;
    let mut count_not_map = 0;
    let mut rows = sql_store.scan();
    while let Some(row) = rows.next().await {
        let mut keys = BTreeSet::<String>::default();
        count += 1;
        if let Ok(row) = row {
            let mut decoder = Decoder::new(&row.bytes);
            // println!("");
            // println!("{}:", count);
            let decode_type = decoder.datatype();
            if decode_type.is_err() || decode_type.unwrap() != Type::Map {
                count_not_map += 1;
                continue;
            }
            count_map += 1;
            for _ in 0..decoder.map().unwrap().unwrap() {
                let decode_type = decoder.datatype();
                if decode_type.is_ok() || decoder.datatype().unwrap() == Type::String {
                    let key = decoder.str().map_or("".to_string(), |s| s.to_string());
                    keys.insert(key); //  keys.get(&key).unwrap_or(&0) + 1
                    let _value = decoder.skip(); // skip value
                }
            }
            let keys_string = keys.iter().map(|s| s.to_string()).collect::<Vec<String>>().join(",");
            key_sets.insert(keys_string.clone(), key_sets.get(&keys_string).unwrap_or(&0) + 1);

            // let eventID = EventId::new(
            //     &opts.network.to_network(&opts.local_network_id)?, // e.g. Network::Mainnet
            //     "model",                                           // e.g. "model"
            //     sort_value, // e.g. "kh4q0ozorrgaq2mezktnrmdwleo1d" // cspell:disable-line
            //     controller, // e.g. "did:key:z6MkgSV3tAuw7gUWqKCUY7ae6uWNxqYgdwPhUJbJhF9EFXm9"
            //     init, // e.g. Cid::from_str("bagcqceraplay4erv6l32qrki522uhiz7rf46xccwniw7ypmvs3cvu2b3oulq") // cspell:disable-line
            //     event_height, // e.g. 1
            //     &Cid::new_v1(0x71, Code::Sha2_256.digest(&row.bytes)),
            // );

            if key_sets.get(&keys_string) == Some(&1) {
                println!(
                    "{} display: {} {}",
                    count,
                    Cid::new_v1(0x71, Code::Sha2_256.digest(&row.bytes)),
                    display(&row.bytes)
                );
            }

            if count % 100_000 == 0 {
                println!(
                    "{}: count_map={}, count_not_map={}, key_sets={}",
                    count,
                    count_map,
                    count_not_map,
                    key_sets.len()
                );
            }
        } else {
            continue;
        }
    }
    println!(
        "{}: count_map={}, count_not_map={}, key_sets={:?}",
        count, count_map, count_not_map, key_sets
    );
    Ok(())
}
