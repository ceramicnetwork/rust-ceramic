use ceramic_p2p::SQLiteBlockStore;
use chrono::{SecondsFormat, Utc};
use cid::{multibase, multihash, Cid};
use clap::Parser;
use clap_verbosity_flag::{InfoLevel, Verbosity};
use clio::{ClioPath, InputPath, OutputPath};
use glob::{glob, Paths};
use sqlx::sqlite::SqlitePool;
use std::{fs, path::PathBuf};

/// Migrate from kubo to ceramic-one
#[derive(Parser, Debug)]
#[clap(name = "migration", about, author, version, about)]
struct Cli {
    /// The path to the ipfs_repo [eg: ~/.ipfs/blocks]
    #[clap(long, short, value_parser)]
    input_ipfs_path: Option<ClioPath>,

    /// The path to the input_ceramic_db [eg: ~/.ceramic-one/db.sqlite3]
    #[clap(long, short = 'c', value_parser)]
    input_ceramic_db: Option<InputPath>,

    /// The path to the output_ceramic_db [eg: ~/.ceramic-one/db.sqlite3]
    #[clap(long, short, value_parser)]
    output_ceramic_path: Option<OutputPath>,

    #[clap(flatten)]
    verbose: Verbosity<InfoLevel>,
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();

    let home: PathBuf = dirs::home_dir().unwrap_or("/data/".into());
    let default_output_ceramic_path: PathBuf = home.join(".ceramic-one/db.sqlite3");

    let output_ceramic_path = args
        .output_ceramic_path
        .unwrap_or_else(|| OutputPath::new(default_output_ceramic_path.as_path()).unwrap());
    println!(
        "{} Opening output ceramic SQLite DB at: {}",
        Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        output_ceramic_path
    );

    let pool: sqlx::Pool<sqlx::Sqlite> = SqlitePool::connect(&format!(
        "sqlite:{}?mode=rwc",
        output_ceramic_path.path().path().display()
    ))
    .await
    .unwrap();
    let store = SQLiteBlockStore::new(pool).await.unwrap();

    if args.input_ceramic_db.is_some() {
        migrate_from_database(args.input_ceramic_db, store.clone()).await;
    }
    if args.input_ipfs_path.is_some() {
        migrate_from_filesystem(args.input_ipfs_path, store.clone()).await;
    }
}

async fn migrate_from_filesystem(input_ipfs_path: Option<ClioPath>, store: SQLiteBlockStore) {
    // the block store is split in to 1024 directories and then the blocks stored as files.
    // the dir structure is the penultimate two characters as dir then the b32 sha256 multihash of the block
    // The leading "B" for the b32 sha256 multihash is left off
    // ~/.ipfs/blocks/QV/CIQOHMGEIKMPYHAUTL57JSEZN64SIJ5OIHSGJG4TJSSJLGI3PBJLQVI.data // cspell:disable-line
    let p = input_ipfs_path
        .unwrap()
        .path()
        .join("**/*")
        .to_str()
        .unwrap()
        .to_owned();
    println!(
        "{} Opening IPFS Repo at: {}",
        Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        &p
    );
    let paths: Paths = glob(&p).unwrap();

    let mut count = 0;
    let mut err_count = 0;

    for path in paths {
        let path = path.unwrap().as_path().to_owned();
        if !path.is_file() {
            continue;
        }

        let Ok((_base, hash_bytes)) =
            multibase::decode("B".to_string() + path.file_stem().unwrap().to_str().unwrap())
        else {
            println!(
                "{} {:?} is not a base32upper multihash.",
                Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
                path.display()
            );
            err_count += 1;
            continue;
        };
        let Ok(hash) = multihash::Multihash::from_bytes(&hash_bytes) else {
            println!(
                "{} {:?} is not a base32upper multihash.",
                Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
                path.display()
            );
            err_count += 1;
            continue;
        };
        let cid = Cid::new_v1(0x71, hash);
        let blob = fs::read(&path).unwrap();

        if count % 10000 == 0 {
            println!(
                "{} {} {} ok:{}, err:{}",
                Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
                path.display(),
                cid,
                count,
                err_count
            );
        }

        let result = store.put(cid, blob.into(), vec![]).await;
        if result.is_err() {
            println!(
                "{} err: {} {:?}",
                Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
                path.display(),
                result
            );
            err_count += 1;
            continue;
        }
        count += 1;
    }

    println!(
        "{} count={}, err_count={}",
        Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        count,
        err_count
    );
}

async fn migrate_from_database(input_ceramic_db: Option<InputPath>, store: SQLiteBlockStore) {
    let other = match input_ceramic_db {
        Some(input_ceramic_db) => input_ceramic_db,
        None => return,
    };
    let input_ceramic_db_filename = other.path().as_os_str().to_str().unwrap();
    println!(
        "{} Importing blocks from {}.",
        Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        input_ceramic_db_filename
    );
    let result = store.merge_from_sqlite(input_ceramic_db_filename).await;
    println!(
        "{} Done importing blocks from {}.",
        Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        input_ceramic_db_filename
    );
    result.unwrap()
}
