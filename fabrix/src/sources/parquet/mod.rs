//! Parquet
//!
//! Parquet is a module for reading and writing Parquet files.

use std::fs::File;
use std::io::Cursor;

pub mod reader;
pub mod writer;

pub use reader::{ParquetReadOptions, Reader as ParquetReader};
pub use writer::{ParquetWriteOptions, Writer as ParquetWriter};

pub(crate) const UNSUPPORTED_TYPE: &str = "Unsupported ParquetSource type";

#[derive(Debug)]
pub enum ParquetSource {
    File(File),
    Path(String),
    Bytes(Cursor<Vec<u8>>),
}
