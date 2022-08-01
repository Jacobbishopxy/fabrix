//! JsonWriter
//!
//! Write to Json, sources: std::io::Write

use std::io::Write;

use crate::JsonType;

pub struct JsonWriter<W>
where
    W: Write,
{
    reader: W,
    json_format: JsonType,
}
