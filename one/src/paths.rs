use std::path::PathBuf;

// Determine the store dir path:
//
//  1. path from options
//  2. path $HOME/.ceramic-one
//  3. ./.ceramic-one
pub fn store_dir(opts_path: &Option<PathBuf>) -> PathBuf {
    match opts_path.clone() {
        Some(dir) => dir,
        None => match home::home_dir() {
            Some(home_dir) => home_dir.join(".ceramic-one"),
            None => PathBuf::from(".ceramic-one"),
        },
    }
}

pub fn ceramic_store_db_path(dir: &PathBuf) -> PathBuf {
    dir.join("db.sqlite3")
}
