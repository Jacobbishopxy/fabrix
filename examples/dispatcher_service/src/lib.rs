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

pub use csv2db::csv_to_db;
pub use csv2json::csv_to_json;
pub use db2csv::db_to_csv;
pub use db2parquet::db_to_parquet;
pub use error::*;
pub use xl2db::xl_to_db;
pub use xl2json::xl_to_json;

pub(crate) use responses::*;

pub(crate) const MULTIPART_KEY_FILE: &str = "file";
pub(crate) const FILE_TYPE_CSV: &str = "text/csv";
pub(crate) const FILE_TYPE_XL: &str =
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
pub(crate) const DB_CONN: &str = "sqlite://ds.sqlite";

pub(crate) fn get_current_time() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
