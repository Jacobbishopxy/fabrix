//! CSV Reader
//!
//! Reading CSV files.

use std::fs::File;
use std::io::Cursor;

use polars::io::mmap::MmapBytesReader;
use polars::prelude::{CsvReader, SerReader};

use crate::FabrixError;

// use crate::{DataFrame, FabrixError, FabrixResult};

#[allow(dead_code)]
pub struct Reader<'a, READER: MmapBytesReader> {
    csv_reader: CsvReader<'a, READER>,
}

impl<'a, READER: MmapBytesReader> Reader<'a, READER> {
    fn new(reader: READER) -> Self {
        Self {
            csv_reader: CsvReader::new(reader),
        }
    }
}

#[derive(Debug)]
pub enum CsvSource<'a> {
    File(File),
    Path(&'a str),
    Bytes(Cursor<bytes::Bytes>),
}

impl<'a> TryFrom<CsvSource<'a>> for Reader<'a, File> {
    type Error = FabrixError;

    fn try_from(value: CsvSource<'a>) -> Result<Self, Self::Error> {
        match value {
            CsvSource::File(file) => Ok(Self::new(file)),
            CsvSource::Path(path) => {
                let file = File::open(path)?;
                Ok(Self::new(file))
            }
            _ => Err(FabrixError::new_common_error("Unsupported CsvSource type")),
        }
    }
}

impl<'a> TryFrom<CsvSource<'a>> for Reader<'a, Cursor<bytes::Bytes>> {
    type Error = FabrixError;

    fn try_from(value: CsvSource<'a>) -> Result<Self, Self::Error> {
        match value {
            CsvSource::Bytes(bytes) => Ok(Self::new(bytes)),
            _ => Err(FabrixError::new_common_error("Unsupported CsvSource type")),
        }
    }
}

#[cfg(test)]
mod test_csv_reader {
    use super::*;

    const CSV_FILE_PATH: &str = "../mock/test.csv";

    #[test]
    fn raw_read() {
        let foo = CsvReader::from_path(CSV_FILE_PATH)
            .expect("file exists")
            .has_header(true)
            .finish();

        println!("{:?}", foo.unwrap());
    }

    #[test]
    fn file_reader() {
        let reader: Reader<File> = CsvSource::Path(CSV_FILE_PATH).try_into().unwrap();

        let foo = reader.csv_reader.finish();

        println!("{:?}", foo.unwrap());
    }
}
