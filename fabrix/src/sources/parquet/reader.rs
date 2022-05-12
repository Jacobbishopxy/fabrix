//! Parquet Reader
//!
//! Reading Parquet files.

use std::fs::File;
use std::io::Cursor;

use async_trait::async_trait;
use polars::io::mmap::MmapBytesReader;
use polars::io::RowCount;
use polars::prelude::{ParquetReader, SerReader};

use super::{ParquetSource, UNSUPPORTED_TYPE};
use crate::{Fabrix, FabrixError, FabrixResult, FromSource, ReadOptions};

// ================================================================================================
// Parquet Reader
// ================================================================================================

/// Parquet Reader
pub struct Reader<R: MmapBytesReader> {
    parquet_reader: Option<ParquetReader<R>>,
}

impl<R: MmapBytesReader> Reader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            parquet_reader: Some(ParquetReader::new(reader)),
        }
    }

    pub fn has_reader(&self) -> bool {
        self.parquet_reader.is_some()
    }

    pub fn new_reader(&mut self, reader: R) -> &mut Self {
        self.parquet_reader = Some(ParquetReader::new(reader));
        self
    }

    pub fn with_read_parallel(&mut self, parallel: bool) -> &mut Self {
        self.parquet_reader = self
            .parquet_reader
            .take()
            .map(|r| r.read_parallel(parallel));
        self
    }

    pub fn with_n_rows(&mut self, num_rows: usize) -> &mut Self {
        self.parquet_reader = self
            .parquet_reader
            .take()
            .map(|r| r.with_n_rows(Some(num_rows)));
        self
    }

    pub fn with_columns(&mut self, columns: Vec<String>) -> &mut Self {
        self.parquet_reader = self
            .parquet_reader
            .take()
            .map(|r| r.with_columns(Some(columns)));
        self
    }

    pub fn with_projection(&mut self, projection: Vec<usize>) -> &mut Self {
        self.parquet_reader = self
            .parquet_reader
            .take()
            .map(|r| r.with_projection(Some(projection)));
        self
    }

    pub fn with_row_count(&mut self, name: &str, offset: usize) -> &mut Self {
        let name = name.to_string();
        let offset = offset as u64;
        self.parquet_reader = self
            .parquet_reader
            .take()
            .map(|r| r.with_row_count(Some(RowCount { name, offset })));
        self
    }

    pub fn finish(&mut self, index: Option<usize>) -> FabrixResult<Fabrix> {
        let reader = self
            .parquet_reader
            .take()
            .ok_or_else(|| FabrixError::new_common_error("ParquetReader is not initialized"))?;

        let df = reader.finish()?;

        if let Some(index) = index {
            Ok(Fabrix::new(df, index)?)
        } else {
            Ok(Fabrix::new_no_index(df))
        }
    }
}

// ================================================================================================
// ParquetReader TryFrom ParquetSource
// ================================================================================================

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

// ================================================================================================
// Parquet read options & FromSource impl
// ================================================================================================

#[derive(Default)]
pub struct ParquetReadOptions {
    read_parallel: Option<bool>,
    num_rows: Option<usize>,
    select_columns: Option<Vec<String>>,
    projection: Option<Vec<usize>>,
    row_count: Option<(String, usize)>,
    index: Option<usize>,
}

impl ReadOptions for ParquetReadOptions {
    fn source_type(&self) -> &str {
        "parquet"
    }
}

#[async_trait]
impl<'a, R> FromSource<ParquetReadOptions, 'a> for Reader<R>
where
    R: MmapBytesReader,
{
    async fn async_read<'o>(&mut self, options: &'o ParquetReadOptions) -> FabrixResult<Fabrix>
    where
        'o: 'a,
    {
        self.sync_read(options)
    }

    fn sync_read<'o>(&mut self, options: &'o ParquetReadOptions) -> FabrixResult<Fabrix>
    where
        'o: 'a,
    {
        let ParquetReadOptions {
            read_parallel,
            num_rows,
            select_columns,
            projection,
            row_count,
            index,
        } = options;

        if let Some(read_parallel) = read_parallel {
            self.with_read_parallel(*read_parallel);
        }
        if let Some(num_rows) = num_rows {
            self.with_n_rows(*num_rows);
        }
        if let Some(select_columns) = select_columns {
            self.with_columns(select_columns.clone());
        }
        if let Some(projection) = projection {
            self.with_projection(projection.clone());
        }
        if let Some((name, offset)) = row_count {
            self.with_row_count(name, *offset);
        }

        self.finish(*index)
    }
}

#[cfg(test)]
mod test_parquet_reader {
    use super::*;

    const PARQUET_FILE_PATH: &str = "../mock/test.parquet";

    #[test]
    fn file_read() {
        let mut reader: Reader<File> = ParquetSource::Path(PARQUET_FILE_PATH.to_string())
            .try_into()
            .unwrap();

        assert!(reader.has_reader());

        let foo = reader.with_row_count("index_column", 0).finish(Some(0));

        assert!(foo.is_ok());

        println!("foo:\n{:?}", foo.unwrap());

        assert!(!reader.has_reader());
    }
}
