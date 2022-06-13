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

    pub fn has_writer(&self) -> bool {
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

    pub fn with_date_format(&mut self, format: &str) -> &mut Self {
        self.csv_writer = self
            .csv_writer
            .take()
            .map(|r| r.with_date_format(Some(format.to_owned())));
        self
    }

    pub fn with_time_format(&mut self, format: &str) -> &mut Self {
        self.csv_writer = self
            .csv_writer
            .take()
            .map(|r| r.with_time_format(Some(format.to_owned())));
        self
    }

    pub fn with_timestamp_format(&mut self, format: &str) -> &mut Self {
        self.csv_writer = self
            .csv_writer
            .take()
            .map(|r| r.with_timestamp_format(Some(format.to_owned())));
        self
    }

    pub fn with_quoting_char(&mut self, char: u8) -> &mut Self {
        self.csv_writer = self.csv_writer.take().map(|r| r.with_quoting_char(char));
        self
    }

    pub fn finish(&mut self, mut fabrix: Fabrix) -> FabrixResult<()> {
        let mut writer = self
            .csv_writer
            .take()
            .ok_or(FabrixError::NotInitialized("CsvWriter"))?;

        writer.finish(&mut fabrix.data)?;
        Ok(())
    }
}

// ================================================================================================
// CsvWriter TryFrom CsvSource
// ================================================================================================

impl<'a> TryFrom<CsvSource<'a>> for Writer<File> {
    type Error = FabrixError;

    fn try_from(source: CsvSource<'a>) -> FabrixResult<Self> {
        match source {
            CsvSource::File(file) => Ok(Self::new(file)),
            CsvSource::Path(path) => Ok(Self::new(File::create(path)?)),
            _ => Err(FabrixError::UnsupportedType(UNSUPPORTED_TYPE)),
        }
    }
}

impl<'a> TryFrom<CsvSource<'a>> for Writer<&'a mut Cursor<Vec<u8>>> {
    type Error = FabrixError;

    fn try_from(source: CsvSource<'a>) -> FabrixResult<Self> {
        match source {
            CsvSource::BuffWrite(bytes) => Ok(Self::new(bytes)),
            _ => Err(FabrixError::UnsupportedType(UNSUPPORTED_TYPE)),
        }
    }
}

// ================================================================================================
// Csv write options & IntoSource impl
// ================================================================================================

#[derive(Default)]
pub struct CsvWriteOptions<'a> {
    pub has_header: Option<bool>,
    pub delimiter: Option<u8>,
    pub date_format: Option<&'a str>,
    pub time_format: Option<&'a str>,
    pub timestamp_format: Option<&'a str>,
    pub quoting_char: Option<u8>,
}

impl<'a> WriteOptions for CsvWriteOptions<'a> {
    fn source_type() -> &'static str {
        "csv"
    }
}

#[async_trait]
impl<'a, W> IntoSource<'a, CsvWriteOptions<'_>> for Writer<W>
where
    W: Write + Send,
{
    async fn async_write<'o>(
        &mut self,
        fabrix: Fabrix,
        options: &'o CsvWriteOptions,
    ) -> FabrixResult<()>
    where
        'o: 'a,
    {
        self.sync_write(fabrix, options)
    }

    fn sync_write<'o>(&mut self, fabrix: Fabrix, options: &'o CsvWriteOptions) -> FabrixResult<()>
    where
        'o: 'a,
    {
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
    use super::*;
    use crate::{date, datetime, fx, time};

    const CSV_FILE_PATH: &str = "../cache/write.csv";

    #[test]
    fn file_writer() {
        let mut writer: Writer<File> = CsvSource::Path(CSV_FILE_PATH).try_into().unwrap();

        assert!(writer.has_writer());

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
        assert!(!writer.has_writer());
    }
}
