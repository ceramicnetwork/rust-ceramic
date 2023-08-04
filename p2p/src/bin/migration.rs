use ceramic_p2p::SQLiteBlockStore;
use cid::Cid;
use clap::Parser;
use clio::{ClioPath, OutputPath};
use glob::{glob, Paths};
use sqlx::sqlite::SqlitePool;
use std::{fs, path::PathBuf};

/// Migrate from kubo to ceramic-one
#[derive(Parser, Debug)]
#[clap(name = "migration", about, author, version, about)]
struct Cli {
    /// The path to the ipfs_repo [default: ~/.ipfs/]
    // todo: use the env var $IPFS_PATH then ~/.ipfs
    #[clap(long, short, value_parser)]
    input_ipfs_path: Option<ClioPath>,

    /// The path to the ceramic_db [default: ~/.ceramic-one/db.sqlite3]
    // todo use the env var $CERAMIC_PATH then ~/.ceramic-one/db.sqlite3
    #[clap(long, short, value_parser)]
    output_ceramic_path: Option<OutputPath>,

    #[clap(flatten)]
    verbose: clap_verbosity_flag::Verbosity,
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();

    let home: PathBuf = dirs::home_dir().unwrap_or("/data/".into());
    let default_input_ipfs_path: PathBuf = home.join(".ipfs");
    let default_output_ceramic_path: PathBuf = home.join(".ceramic-one/db.sqlite3");

    let output_ceramic_path = args
        .output_ceramic_path
        .unwrap_or_else(|| OutputPath::new(default_output_ceramic_path.as_path()).unwrap());
    println!("opening SQLite at: {}", output_ceramic_path);
    let pool: sqlx::Pool<sqlx::Sqlite> = SqlitePool::connect(&format!(
        "sqlite:{}?mode=rwc",
        output_ceramic_path.path().path().display()
    ))
    .await
    .unwrap();
    let store = SQLiteBlockStore::new(pool).await.unwrap();

    // the block store is split in to 1024 directories and then the blocks stored as files.
    // the dir structure is the penultimate two characters as dir then the b32 sha256 multihash of the block
    // The leading "B" for the b32 sha256 multihash is left off
    // ~/.ipfs/blocks/QV/CIQOHMGEIKMPYHAUTL57JSEZN64SIJ5OIHSGJG4TJSSJLGI3PBJLQVI.data // cspell:disable-line
    let p = args
        .input_ipfs_path
        .unwrap_or(ClioPath::new(default_input_ipfs_path).unwrap())
        .path()
        .join("blocks/*/*.data")
        .to_str()
        .unwrap()
        .to_owned();
    println!("opening IPFS Repo at: {}", &p);
    let paths: Paths = glob(&p).unwrap();

    let mut count = 0;
    let mut err_count = 0;

    for path in paths {
        let path = path.unwrap().as_path().to_owned();
        let Ok(cid) = Cid::try_from("B".to_string() + path.file_stem().unwrap().to_str().unwrap())
        else {
            println!("err at {:?}", path.display());
            err_count += 1;
            continue;
        };
        let blob = fs::read(&path).unwrap();

        if count % 1000 == 0 {
            println!(
                "Name: {}, {} count={}, err_count={}",
                path.display(),
                cid,
                count,
                err_count
            );
        }

        let result = store.put(cid, blob.into(), vec![]).await;
        if result.is_err() {
            println!("err: {:?}", result)
        }
        count += 1;
    }

    println!("count={}, err_count={}", count, err_count);
}
