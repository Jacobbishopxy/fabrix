//! Csv
//!
//! Csv is a module for reading and writing CSV files.

use std::fs::File;
use std::io::Cursor;

pub mod reader;
pub mod writer;

pub use reader::{CsvReadOptions, Reader as CsvReader};
pub use writer::{CsvWriteOptions, Writer as CsvWriter};

pub(crate) const UNSUPPORTED_TYPE: &str = "Unsupported CSVSource type";

#[derive(Debug)]
pub enum CsvSource<'a> {
    File(File),
    Path(&'a str),
    Uri(&'a str),
    BuffRead(Cursor<Vec<u8>>),
    BuffWrite(&'a mut Cursor<Vec<u8>>),
}
