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
pub enum JsonSource {
    File(File),
    Path(String),
    Bytes(Cursor<Vec<u8>>),
}
