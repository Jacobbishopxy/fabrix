//! Json
//!
//! Json module

use std::fs::File;
use std::io::Cursor;

pub mod reader;
pub mod writer;

pub use reader::{JsonReadOptions, Reader as JsonReader};
pub use writer::{JsonWriteOptions, Writer as JsonWriter};

pub(crate) const UNSUPPORTED_TYPE: &str = "Unsupported JsonSource type";

#[derive(Debug)]
pub enum JsonSource<'a> {
    File(File),
    Path(&'a str),
    Uri(&'a str),
    BuffRead(Cursor<Vec<u8>>),
    BuffWrite(&'a mut Cursor<Vec<u8>>),
}
