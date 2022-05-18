//! Parquet Writer
//!
//! Writing Parquet files.

// ================================================================================================
// Parquet Writer
// ================================================================================================

use std::fs::File;
use std::io::{Cursor, Write};

use async_trait::async_trait;
use polars::prelude::ParquetWriter;

use crate::{Fabrix, FabrixError, FabrixResult, IntoSource, WriteOptions};

use super::{ParquetSource, UNSUPPORTED_TYPE};

pub struct Writer<W: Write> {
    parquet_writer: Option<ParquetWriter<W>>,
}

impl<W: Write> Writer<W> {
    pub fn new(writer: W) -> Self {
        Self {
            parquet_writer: Some(ParquetWriter::new(writer)),
        }
    }

    pub fn has_writer(&self) -> bool {
        self.parquet_writer.is_some()
    }

    pub fn new_writer(&mut self, writer: W) -> &mut Self {
        self.parquet_writer = Some(ParquetWriter::new(writer));
        self
    }

    // TODO:
    // pub fn with_compression()

    pub fn with_statistics(&mut self, statistics: bool) -> &mut Self {
        self.parquet_writer = self
            .parquet_writer
            .take()
            .map(|r| r.with_statistics(statistics));
        self
    }

    pub fn finish(&mut self, mut fabrix: Fabrix) -> FabrixResult<()> {
        let writer = self
            .parquet_writer
            .take()
            .ok_or_else(|| FabrixError::new_common_error("ParquetWriter is not initialized"))?;

        writer.finish(&mut fabrix.data)?;
        Ok(())
    }
}

// ================================================================================================
// ParquetWriter TryFrom ParquetSource
// ================================================================================================

impl<'a> TryFrom<ParquetSource<'a>> for Writer<File> {
    type Error = FabrixError;

    fn try_from(source: ParquetSource<'a>) -> FabrixResult<Self> {
        match source {
            ParquetSource::File(file) => Ok(Self::new(file)),
            ParquetSource::Path(path) => Ok(Self::new(File::create(path)?)),
            _ => Err(FabrixError::new_common_error(UNSUPPORTED_TYPE)),
        }
    }
}

impl<'a> TryFrom<ParquetSource<'a>> for Writer<&'a mut Cursor<Vec<u8>>> {
    type Error = FabrixError;

    fn try_from(source: ParquetSource<'a>) -> FabrixResult<Self> {
        match source {
            ParquetSource::BuffWrite(bytes) => Ok(Self::new(bytes)),
            _ => Err(FabrixError::new_common_error(UNSUPPORTED_TYPE)),
        }
    }
}

// ================================================================================================
// Parquet write options & FromSource impl
// ================================================================================================

#[derive(Default)]
pub struct ParquetWriteOptions {
    pub statistics: Option<bool>,
}

impl WriteOptions for ParquetWriteOptions {
    fn source_type() -> &'static str {
        "parquet"
    }
}

#[async_trait]
impl<'a, W> IntoSource<ParquetWriteOptions, 'a> for Writer<W>
where
    W: Write + Send,
{
    async fn async_write<'o>(
        &mut self,
        fabrix: Fabrix,
        options: &'o ParquetWriteOptions,
    ) -> FabrixResult<()>
    where
        'o: 'a,
    {
        self.sync_write(fabrix, options)
    }

    fn sync_write<'o>(
        &mut self,
        fabrix: Fabrix,
        options: &'o ParquetWriteOptions,
    ) -> FabrixResult<()>
    where
        'o: 'a,
    {
        if let Some(statistics) = options.statistics {
            self.with_statistics(statistics);
        }

        self.finish(fabrix)
    }
}

#[cfg(test)]
mod test_parquet_writer {
    use super::*;
    use crate::{date, datetime, fx, time};

    const PARQUET_FILE_PATH: &str = "../cache/write.parquet";

    #[test]
    fn file_writer() {
        let mut writer: Writer<File> = ParquetSource::Path(PARQUET_FILE_PATH).try_into().unwrap();

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
