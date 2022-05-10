//! Parquet Reader
//!
//! Reading Parquet files.

use std::fs::File;
use std::io::Cursor;

use polars::io::mmap::MmapBytesReader;
use polars::prelude::{ParquetReader, SerReader};

use crate::FabrixError;

const UNSUPPORTED_TYPE: &str = "Unsupported ParquetSource type";

#[allow(dead_code)]
pub struct Reader<READER: MmapBytesReader> {
    parquet_reader: ParquetReader<READER>,
}

impl<READER: MmapBytesReader> Reader<READER> {
    fn new(reader: READER) -> Self {
        Self {
            parquet_reader: ParquetReader::new(reader),
        }
    }
}

#[derive(Debug)]
pub enum ParquetSource {
    File(File),
    Path(String),
    Bytes(Cursor<Vec<u8>>),
}

impl TryFrom<ParquetSource> for Reader<File> {
    type Error = FabrixError;

    fn try_from(value: ParquetSource) -> Result<Self, Self::Error> {
        match value {
            ParquetSource::File(file) => Ok(Self::new(file)),
            ParquetSource::Path(path) => {
                let file = File::open(path)?;
                Ok(Self::new(file))
            }
            _ => Err(FabrixError::new_common_error(UNSUPPORTED_TYPE)),
        }
    }
}

impl TryFrom<ParquetSource> for Reader<Cursor<Vec<u8>>> {
    type Error = FabrixError;

    fn try_from(value: ParquetSource) -> Result<Self, Self::Error> {
        match value {
            ParquetSource::Bytes(bytes) => Ok(Self::new(bytes)),
            _ => Err(FabrixError::new_common_error(UNSUPPORTED_TYPE)),
        }
    }
}

#[cfg(test)]
mod test_parquet_reader {
    // use super::*;

    // TODO:
}
