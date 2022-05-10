//! CSV Writer
//!
//! Writing CSV files.

use std::fs::File;
use std::io::{Cursor, Write};

use async_trait::async_trait;
use polars::prelude::{CsvWriter, SerWriter};

use super::{CsvSource, UNSUPPORTED_TYPE};
use crate::{Fabrix, FabrixError, FabrixResult, IntoSource, WriteOptions};

// TODO:
// custom value types cannot be written to csv files

// ================================================================================================
// CSV Writer
// ================================================================================================

pub struct Writer<W: Write> {
    csv_writer: CsvWriter<W>,
}

impl<W: Write> Writer<W> {
    pub fn new(writer: W) -> Self {
        Self {
            csv_writer: CsvWriter::new(writer),
        }
    }

    pub fn has_header(mut self, has_header: bool) -> Self {
        self.csv_writer = self.csv_writer.has_header(has_header);
        self
    }

    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.csv_writer = self.csv_writer.with_delimiter(delimiter);
        self
    }

    pub fn with_date_format(mut self, format: String) -> Self {
        self.csv_writer = self.csv_writer.with_date_format(Some(format));
        self
    }

    pub fn with_time_format(mut self, format: String) -> Self {
        self.csv_writer = self.csv_writer.with_time_format(Some(format));
        self
    }

    pub fn with_timestamp_format(mut self, format: String) -> Self {
        self.csv_writer = self.csv_writer.with_timestamp_format(Some(format));
        self
    }

    pub fn with_quoting_char(mut self, char: u8) -> Self {
        self.csv_writer = self.csv_writer.with_quoting_char(char);
        self
    }

    pub fn finish(self, fabrix: &Fabrix) -> FabrixResult<()> {
        let mut data = fabrix.data().clone();
        self.csv_writer.finish(&mut data)?;
        Ok(())
    }
}

// ================================================================================================
// CsvWriter TryFrom CsvSource
// ================================================================================================

impl TryFrom<CsvSource> for Writer<File> {
    type Error = FabrixError;

    fn try_from(source: CsvSource) -> FabrixResult<Self> {
        match source {
            CsvSource::File(file) => Ok(Self::new(file)),
            CsvSource::Path(path) => {
                let file = File::create(path)?;
                Ok(Self::new(file))
            }
            _ => Err(FabrixError::new_common_error(UNSUPPORTED_TYPE)),
        }
    }
}

impl TryFrom<CsvSource> for Writer<Cursor<Vec<u8>>> {
    type Error = FabrixError;

    fn try_from(source: CsvSource) -> FabrixResult<Self> {
        match source {
            CsvSource::Bytes(bytes) => Ok(Self::new(bytes)),
            _ => Err(FabrixError::new_common_error(UNSUPPORTED_TYPE)),
        }
    }
}

// ================================================================================================
// Csv write options & FromSource impl
// ================================================================================================

pub struct CsvWriteOptions {
    pub has_header: bool,
    pub delimiter: u8,
    pub date_format: String,
    pub time_format: String,
    pub timestamp_format: String,
    pub quoting_char: u8,
}

impl WriteOptions for CsvWriteOptions {
    fn source_type(&self) -> &str {
        "csv"
    }
}

#[async_trait]
impl<W> IntoSource<CsvWriteOptions> for Writer<W>
where
    W: Write + Send,
{
    async fn async_write(&mut self, _options: CsvWriteOptions) -> FabrixResult<()> {
        todo!()
    }

    fn sync_write(&mut self, _options: CsvWriteOptions) -> FabrixResult<()> {
        todo!()
    }
}

#[cfg(test)]
mod test_csv_writer {

    use crate::{date, datetime, fx, time};

    use super::*;

    const CSV_FILE_PATH: &str = "../cache/write.csv";

    #[test]
    fn file_writer() {
        let writer: Writer<File> = CsvSource::Path(CSV_FILE_PATH.to_owned())
            .try_into()
            .unwrap();

        let fx = fx![
            "id";
            "id" => [1, 2, 3],
            "name" => ["a", "b", "c"],
            "date" => [date!(2020,1,1), date!(2020,1,2), date!(2020,1,3)],
            "time" => [time!(12,0,0), time!(12,0,1), time!(12,0,2)],
            "datetime" => [datetime!(2020,1,1,12,0,0), datetime!(2020,1,1,12,0,1), datetime!(2020,1,1,12,0,2)],
        ]
        .unwrap();

        let foo = writer.finish(&fx);

        println!("{:?}", foo);
    }
}
