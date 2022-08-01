//! JsonReader
//!
//! Read from Json, sources: File/BufReader<File>/Cursor<T>/Box<T>

use fabrix_core::polars::io::mmap::MmapBytesReader;

use crate::JsonType;

pub struct JsonReader<R>
where
    R: MmapBytesReader,
{
    reader: R,
    json_format: JsonType,
}
