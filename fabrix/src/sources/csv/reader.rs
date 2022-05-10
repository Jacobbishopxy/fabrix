//! CSV Reader
//!
//! Reading CSV files.

use std::fs::File;
use std::io::Cursor;

use async_trait::async_trait;
use polars::io::mmap::MmapBytesReader;
use polars::prelude::{CsvReader, SerReader};

use super::{CsvSource, UNSUPPORTED_TYPE};
use crate::{Fabrix, FabrixError, FabrixResult, FromSource, ReadOptions, Schema, ValueTypes};

pub struct Reader<'a, R: MmapBytesReader + 'a> {
    csv_reader: CsvReader<'a, R>,
}

impl<'a, R: MmapBytesReader> Reader<'a, R> {
    pub fn new(reader: R) -> Self {
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

    pub fn finish(self, index: Option<&str>) -> FabrixResult<Fabrix> {
        let df = self.csv_reader.finish()?;
        if let Some(index) = index {
            Ok(Fabrix::new(df, index)?)
        } else {
            Ok(Fabrix::new_no_index(df))
        }
    }
}

impl<'a> TryFrom<CsvSource> for Reader<'a, File> {
    type Error = FabrixError;

    fn try_from(source: CsvSource) -> FabrixResult<Self> {
        match source {
            CsvSource::File(file) => Ok(Self::new(file)),
            CsvSource::Path(path) => {
                let file = File::open(path)?;
                Ok(Self::new(file))
            }
            _ => Err(FabrixError::new_common_error(UNSUPPORTED_TYPE)),
        }
    }
}

impl<'a> TryFrom<CsvSource> for Reader<'a, Cursor<Vec<u8>>> {
    type Error = FabrixError;

    fn try_from(source: CsvSource) -> FabrixResult<Self> {
        match source {
            CsvSource::Bytes(bytes) => Ok(Self::new(bytes)),
            _ => Err(FabrixError::new_common_error(UNSUPPORTED_TYPE)),
        }
    }
}

#[async_trait]
impl<'a, R> FromSource<CsvSource> for Reader<'a, R>
where
    R: MmapBytesReader + 'a,
{
    async fn async_read<O>(&mut self, _options: O) -> FabrixResult<()>
    where
        O: ReadOptions<CsvSource>,
    {
        todo!()
    }

    fn sync_read<O>(&mut self, _options: O) -> FabrixResult<()>
    where
        O: ReadOptions<CsvSource>,
    {
        todo!()
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
        let reader: Reader<File> = CsvSource::Path(CSV_FILE_PATH.to_owned())
            .try_into()
            .unwrap();

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

        let reader: Reader<File> = CsvSource::Path(CSV_FILE_PATH.to_owned())
            .try_into()
            .unwrap();

        let foo = reader.with_dtypes(&foo).finish(None);

        assert!(foo.is_ok());
    }
}
