//! Lib

use std::time::SystemTime;

pub mod csv2db;
pub mod csv2json;
pub mod db2csv;
pub mod db2parquet;
pub mod error;
pub mod responses;
pub mod xl2db;
pub mod xl2json;

pub use error::*;
pub(crate) use responses::*;

pub(crate) const MULTIPART_KEY_FILE: &str = "file";
pub(crate) const FILE_TYPE_CSV: &str = "text/csv";
pub(crate) const DB_CONN: &str = "sqlite://ds.sqlite";

pub(crate) fn get_current_time() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
