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
pub enum CsvSource<'a> {
    File(File),
    Path(&'a str),
    // Bytes(Cursor<bytes::Bytes>),
    Bytes(Cursor<Vec<u8>>),
}
