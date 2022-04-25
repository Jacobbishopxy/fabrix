//! CSV Reader
//!
//! Reading CSV files.

use std::fs::File;
use std::io::Cursor;

use polars::io::mmap::MmapBytesReader;
use polars::prelude::{CsvReader, SerReader};

use crate::FabrixError;

// use crate::{DataFrame, FabrixError, FabrixResult};

const UNSUPPORTED_TYPE: &str = "Unsupported CSVSource type";

#[allow(dead_code)]
pub struct Reader<'a, READER: MmapBytesReader + 'a> {
    csv_reader: CsvReader<'a, READER>,
}

impl<'a, READER: MmapBytesReader> Reader<'a, READER> {
    pub fn new(reader: READER) -> Self {
        Self {
            csv_reader: CsvReader::new(reader),
        }
    }

    pub fn has_header(mut self, has_header: bool) -> Self {
        self.csv_reader = self.csv_reader.has_header(has_header);
        self
    }

    pub fn with_ignore_parser_errors(mut self, ignore: bool) -> Self {
        self.csv_reader = self.csv_reader.with_ignore_parser_errors(ignore);
        self
    }

    pub fn with_skip_rows(mut self, skip_rows: usize) -> Self {
        self.csv_reader = self.csv_reader.with_skip_rows(skip_rows);
        self
    }

    pub fn with_rechunk(mut self, rechunk: bool) -> Self {
        self.csv_reader = self.csv_reader.with_rechunk(rechunk);
        self
    }

    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.csv_reader = self.csv_reader.with_delimiter(delimiter);
        self
    }

    pub fn with_comment_char(mut self, comment_char: Option<u8>) -> Self {
        self.csv_reader = self.csv_reader.with_comment_char(comment_char);
        self
    }

    // TODO:
    // pub fn with_dtypes(mut self, schema: Option<&'a Schema>) -> Self {
    //     self
    // }
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
            _ => Err(FabrixError::new_common_error(UNSUPPORTED_TYPE)),
        }
    }
}

impl<'a> TryFrom<CsvSource<'a>> for Reader<'a, Cursor<bytes::Bytes>> {
    type Error = FabrixError;

    fn try_from(value: CsvSource<'a>) -> Result<Self, Self::Error> {
        match value {
            CsvSource::Bytes(bytes) => Ok(Self::new(bytes)),
            _ => Err(FabrixError::new_common_error(UNSUPPORTED_TYPE)),
        }
    }
}

#[cfg(test)]
mod test_csv_reader {
    use polars::io::RowCount;

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

        let rc = RowCount {
            name: "new_index".to_owned(),
            offset: 0,
        };
        let foo = reader.csv_reader.with_row_count(Some(rc)).finish();

        println!("{:?}", foo.unwrap());
    }
}
