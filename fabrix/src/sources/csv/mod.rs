//! Csv
//!
//! Csv is a module for reading and writing CSV files.

pub mod executor;
pub mod reader;
pub mod writer;

use std::fs::File;
use std::io::Cursor;

pub const UNSUPPORTED_TYPE: &str = "Unsupported CSVSource type";

#[derive(Debug)]
pub enum CsvSource {
    File(File),
    Path(String),
    Bytes(Cursor<Vec<u8>>),
}
