//! Json
//!
//! Json module

pub mod reader;
pub mod writer;

pub use reader::{JsonReadOptions, Reader as JsonReader};
pub use writer::{JsonWriteOptions, Writer as JsonWriter};

// TODO:
// JsonSource
