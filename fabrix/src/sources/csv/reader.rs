//! CSV Reader
//!
//! Reading CSV files.

use std::fs::File;
use std::io::Cursor;

use polars::io::mmap::MmapBytesReader;
use polars::prelude::{CsvReader, SerReader};

use crate::{DataFrame, FabrixError, FabrixResult, Schema, ValueTypes};

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

    pub fn with_comment_char(mut self, comment_char: u8) -> Self {
        self.csv_reader = self.csv_reader.with_comment_char(Some(comment_char));
        self
    }

    // schema must be a subset of the total schema
    pub fn with_dtypes(mut self, schema: &'a Schema) -> Self {
        self.csv_reader = self.csv_reader.with_dtypes(Some(schema.as_ref()));
        self
    }

    pub fn with_dtypes_slice(mut self, dtypes: &'a ValueTypes) -> Self {
        self.csv_reader = self
            .csv_reader
            .with_dtypes_slice(Some(dtypes.polars_dtypes()));
        self
    }

    pub fn with_projection(mut self, projection: &'a [usize]) -> Self {
        self.csv_reader = self.csv_reader.with_projection(Some(projection.to_vec()));
        self
    }

    pub fn finish(self, index: Option<&str>) -> FabrixResult<DataFrame> {
        let polars_df = self.csv_reader.finish()?;
        let df = if let Some(index) = index {
            DataFrame::new_with_index_name(polars_df, index)
        } else {
            DataFrame::new_default_index(polars_df)
        };

        Ok(df)
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

    use crate::{FieldInfo, ValueType};

    use super::*;

    const CSV_FILE_PATH: &str = "../mock/test.csv";

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

    #[test]
    fn with_dtypes() {
        // WARNING: ValueType such as Time/Date/DateTime and etc are not supported by polars' CsvReader
        let fi = vec![
            FieldInfo::new("id", ValueType::U32),
            FieldInfo::new("issued_times", ValueType::U8),
        ];
        let foo = Schema::from_field_infos(fi);

        let reader: Reader<File> = CsvSource::Path(CSV_FILE_PATH).try_into().unwrap();

        let foo = reader.with_dtypes(&foo).finish(None);

        assert!(foo.is_ok());
    }
}
