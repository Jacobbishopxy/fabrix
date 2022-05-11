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
    csv_writer: Option<CsvWriter<W>>,
}

impl<W: Write> Writer<W> {
    pub fn new(writer: W) -> Self {
        Self {
            csv_writer: Some(CsvWriter::new(writer)),
        }
    }

    pub fn has_source(&self) -> bool {
        self.csv_writer.is_some()
    }

    pub fn new_writer(&mut self, writer: W) -> &mut Self {
        self.csv_writer = Some(CsvWriter::new(writer));
        self
    }

    pub fn has_header(&mut self, has_header: bool) -> &mut Self {
        self.csv_writer = self.csv_writer.take().map(|r| r.has_header(has_header));
        self
    }

    pub fn with_delimiter(&mut self, delimiter: u8) -> &mut Self {
        self.csv_writer = self.csv_writer.take().map(|r| r.with_delimiter(delimiter));
        self
    }

    pub fn with_date_format(&mut self, format: String) -> &mut Self {
        self.csv_writer = self
            .csv_writer
            .take()
            .map(|r| r.with_date_format(Some(format)));
        self
    }

    pub fn with_time_format(&mut self, format: String) -> &mut Self {
        self.csv_writer = self
            .csv_writer
            .take()
            .map(|r| r.with_time_format(Some(format)));
        self
    }

    pub fn with_timestamp_format(&mut self, format: String) -> &mut Self {
        self.csv_writer = self
            .csv_writer
            .take()
            .map(|r| r.with_timestamp_format(Some(format)));
        self
    }

    pub fn with_quoting_char(&mut self, char: u8) -> &mut Self {
        self.csv_writer = self.csv_writer.take().map(|r| r.with_quoting_char(char));
        self
    }

    pub fn finish(&mut self, mut fabrix: Fabrix) -> FabrixResult<()> {
        let writer = self
            .csv_writer
            .take()
            .ok_or_else(|| FabrixError::new_common_error("CsvWriter is not initialized"))?;

        writer.finish(&mut fabrix.data)?;
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
    has_header: Option<bool>,
    delimiter: Option<u8>,
    date_format: Option<String>,
    time_format: Option<String>,
    timestamp_format: Option<String>,
    quoting_char: Option<u8>,
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
    async fn async_write(&mut self, fabrix: Fabrix, options: CsvWriteOptions) -> FabrixResult<()> {
        self.sync_write(fabrix, options)
    }

    fn sync_write(&mut self, fabrix: Fabrix, options: CsvWriteOptions) -> FabrixResult<()> {
        if let Some(has_header) = options.has_header {
            self.has_header(has_header);
        }
        if let Some(delimiter) = options.delimiter {
            self.with_delimiter(delimiter);
        }
        if let Some(date_format) = options.date_format {
            self.with_date_format(date_format);
        }
        if let Some(time_format) = options.time_format {
            self.with_time_format(time_format);
        }
        if let Some(timestamp_format) = options.timestamp_format {
            self.with_timestamp_format(timestamp_format);
        }
        if let Some(quoting_char) = options.quoting_char {
            self.with_quoting_char(quoting_char);
        }

        self.finish(fabrix)
    }
}

#[cfg(test)]
mod test_csv_writer {

    use crate::{date, datetime, fx, time};

    use super::*;

    const CSV_FILE_PATH: &str = "../cache/write.csv";

    #[test]
    fn file_writer() {
        let mut writer: Writer<File> = CsvSource::Path(CSV_FILE_PATH.to_owned())
            .try_into()
            .unwrap();

        assert!(writer.has_source());

        let fx = fx![
            "id";
            "id" => [1, 2, 3],
            "name" => ["a", "b", "c"],
            "date" => [date!(2020,1,1), date!(2020,1,2), date!(2020,1,3)],
            "time" => [time!(12,0,0), time!(12,0,1), time!(12,0,2)],
            "datetime" => [datetime!(2020,1,1,12,0,0), datetime!(2020,1,1,12,0,1), datetime!(2020,1,1,12,0,2)],
        ]
        .unwrap();

        let foo = writer.finish(fx);

        assert!(foo.is_ok());

        assert!(!writer.has_source());
    }
}
