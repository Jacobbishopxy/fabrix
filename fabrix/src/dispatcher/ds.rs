//! Dispatcher
//!
//! Dispatcher consists in two parts:
//! - Reader: read from one source and save the data in memory
//! - Writer: write the data in memory to one destination
//!
//! Additionally, read & write options are traits that should be implemented

use std::marker::PhantomData;

use async_trait::async_trait;

use crate::{Fabrix, FabrixError, FabrixResult};

// ================================================================================================
// Read & Write Options
// ================================================================================================

pub trait ReadOptions: Send + Default {
    fn source_type() -> &'static str;
}

pub trait WriteOptions: Send + Default {
    fn source_type() -> &'static str;
}

// ================================================================================================
// FromSource & IntoSource
// traits that should be implemented by reader and writer respectively
// ================================================================================================

#[async_trait]
pub trait FromSource<'a, R>
where
    R: ReadOptions,
{
    fn source_type(&self) -> &str {
        R::source_type()
    }

    async fn async_read<'o>(&mut self, options: &'o R) -> FabrixResult<Fabrix>
    where
        'o: 'a;

    fn sync_read<'o>(&mut self, options: &'o R) -> FabrixResult<Fabrix>
    where
        'o: 'a;
}

#[async_trait]
pub trait IntoSource<'a, W>
where
    W: WriteOptions,
{
    fn source_type(&self) -> &str {
        W::source_type()
    }

    async fn async_write<'o>(&mut self, fabrix: Fabrix, options: &'o W) -> FabrixResult<()>
    where
        'o: 'a;

    fn sync_write<'o>(&mut self, fabrix: Fabrix, options: &'o W) -> FabrixResult<()>
    where
        'o: 'a;
}

// ================================================================================================
// Dispatcher
// ================================================================================================

pub struct Dispatcher<'a, Reader, Writer, RO, WO>
where
    Reader: FromSource<'a, RO>,
    Writer: IntoSource<'a, WO>,
    RO: ReadOptions,
    WO: WriteOptions,
{
    reader: Reader,
    writer: Writer,
    read_options: PhantomData<RO>,
    write_options: PhantomData<WO>,
    lifetime: PhantomData<&'a ()>,
    fabrix: Option<Fabrix>,
}

impl<'a, R, W, RO, WO> Dispatcher<'a, R, W, RO, WO>
where
    R: FromSource<'a, RO>,
    W: IntoSource<'a, WO>,
    RO: ReadOptions,
    WO: WriteOptions,
{
    pub fn new(reader: R, writer: W) -> Self {
        Self {
            reader,
            writer,
            read_options: PhantomData,
            write_options: PhantomData,
            lifetime: PhantomData,
            fabrix: None,
        }
    }

    pub fn fabrix_ref(&self) -> Option<&Fabrix> {
        self.fabrix.as_ref()
    }

    pub fn fabrix_mut(&mut self) -> Option<&mut Fabrix> {
        self.fabrix.as_mut()
    }

    pub fn fabrix_take(&mut self) -> Option<Fabrix> {
        self.fabrix.take()
    }

    pub fn fabrix_put<T: Into<Fabrix>>(&mut self, fx: T) {
        self.fabrix = Some(fx.into());
    }

    pub fn has_data(&self) -> bool {
        self.fabrix.is_some()
    }

    pub fn reader_type(&self) -> &str {
        self.reader.source_type()
    }

    pub fn writer_type(&self) -> &str {
        self.writer.source_type()
    }

    /// expose reader as reference to the outside
    pub fn reader(&self) -> &R {
        &self.reader
    }

    /// expose reader as mutable to the outside
    pub fn reader_mut(&mut self) -> &mut R {
        &mut self.reader
    }

    /// expose writer as reference to the outside
    pub fn writer(&self) -> &W {
        &self.writer
    }

    /// expose writer as mutable to the outside
    pub fn writer_mut(&mut self) -> &mut W {
        &mut self.writer
    }

    pub fn sync_read(&mut self, options: &'a RO) -> FabrixResult<()> {
        self.fabrix = Some(self.reader.sync_read(options)?);
        Ok(())
    }

    pub async fn async_read(&mut self, options: &'a RO) -> FabrixResult<()> {
        self.fabrix = Some(self.reader.async_read(options).await?);
        Ok(())
    }

    pub fn sync_write(&mut self, options: &'a WO) -> FabrixResult<()> {
        match self.fabrix.take() {
            Some(fx) => self.writer.sync_write(fx, options),
            None => Err(FabrixError::EmptyContent("fabrix data")),
        }
    }

    pub async fn async_write(&mut self, options: &'a WO) -> FabrixResult<()> {
        match self.fabrix.take() {
            Some(fx) => self.writer.async_write(fx, options).await,
            None => Err(FabrixError::EmptyContent("fabrix data")),
        }
    }
}

#[cfg(test)]
mod dispatcher_tests {
    use std::fs::File;

    use super::*;
    use crate::{
        CsvReadOptions, CsvReader, CsvWriteOptions, CsvWriter, JsonWriteOptions, JsonWriter,
        ParquetReadOptions, ParquetReader, ParquetWriteOptions, ParquetWriter,
    };

    const CSV_READ: &str = "../mock/test.csv";
    const CSV_WRITE: &str = "../cache/test.csv";
    const PARQUET_READ: &str = "../mock/test.parquet";
    const PARQUET_WRITE: &str = "../cache/test.parquet";
    const JSON_WRITE: &str = "../cache/test.json";

    #[derive(Default)]
    struct EmptyOption;

    impl ReadOptions for EmptyOption {
        fn source_type() -> &'static str {
            "empty"
        }
    }

    impl WriteOptions for EmptyOption {
        fn source_type() -> &'static str {
            "empty"
        }
    }

    struct EmptyRead;

    struct EmptyWrite;

    #[async_trait]
    impl<'a> FromSource<EmptyOption, 'a> for EmptyRead {
        async fn async_read<'o>(&mut self, _options: &'o EmptyOption) -> FabrixResult<Fabrix>
        where
            'o: 'a,
        {
            Ok(Fabrix::empty())
        }

        fn sync_read<'o>(&mut self, _options: &'o EmptyOption) -> FabrixResult<Fabrix>
        where
            'o: 'a,
        {
            Ok(Fabrix::empty())
        }
    }

    #[async_trait]
    impl<'a> IntoSource<EmptyOption, 'a> for EmptyWrite {
        async fn async_write<'o>(
            &mut self,
            _fabrix: Fabrix,
            _options: &'o EmptyOption,
        ) -> FabrixResult<()>
        where
            'o: 'a,
        {
            Ok(())
        }

        fn sync_write<'o>(&mut self, _fabrix: Fabrix, _options: &'o EmptyOption) -> FabrixResult<()>
        where
            'o: 'a,
        {
            Ok(())
        }
    }

    #[test]
    fn test_empty_dispatcher() {
        let mut dispatcher = Dispatcher::new(EmptyRead, EmptyWrite);

        let res = dispatcher.sync_read(&EmptyOption);
        assert!(res.is_ok());
        assert!(dispatcher.fabrix_ref().is_some());
        assert_eq!(dispatcher.fabrix_ref().unwrap().shape(), (0, 0));
    }

    #[tokio::test]
    async fn test_empty_dispatcher_async() {
        let mut dispatcher = Dispatcher::new(EmptyRead, EmptyWrite);

        let res = dispatcher.async_read(&EmptyOption).await;
        assert!(res.is_ok());
        assert!(dispatcher.fabrix_ref().is_some());
        assert_eq!(dispatcher.fabrix_ref().unwrap().shape(), (0, 0));
    }

    #[test]
    fn csv_read_write() {
        let reader = CsvReader::new(File::open(CSV_READ).unwrap());
        let writer = CsvWriter::new(File::create(CSV_WRITE).unwrap());

        let mut dispatcher = Dispatcher::new(reader, writer);

        let ro = CsvReadOptions::default();
        let res = dispatcher.sync_read(&ro);
        assert!(res.is_ok());

        let wo = CsvWriteOptions::default();
        let res = dispatcher.sync_write(&wo);
        assert!(res.is_ok());
    }

    #[test]
    fn csv_read_parquet_write() {
        let reader = CsvReader::new(File::open(CSV_READ).unwrap());
        let writer = ParquetWriter::new(File::create(PARQUET_WRITE).unwrap());

        let mut dispatcher = Dispatcher::new(reader, writer);

        let ro = CsvReadOptions::default();
        let res = dispatcher.sync_read(&ro);
        assert!(res.is_ok());

        let fx = dispatcher.fabrix_ref();
        assert!(fx.is_some());
        println!("{:?}", fx.unwrap());

        let wo = ParquetWriteOptions::default();
        let res = dispatcher.sync_write(&wo);
        assert!(res.is_ok());
    }

    #[test]
    fn parquet_read_json_write() {
        let reader = ParquetReader::new(File::open(PARQUET_READ).unwrap());
        let write = JsonWriter::new(File::create(JSON_WRITE).unwrap());

        let mut dispatcher = Dispatcher::new(reader, write);

        let ro = ParquetReadOptions::default();
        let res = dispatcher.sync_read(&ro);
        assert!(res.is_ok());

        let fx = dispatcher.fabrix_ref();
        assert!(fx.is_some());
        println!("{:?}", fx.unwrap());

        let wo = JsonWriteOptions {
            is_json: Some(true),
        };
        let res = dispatcher.sync_write(&wo);
        assert!(res.is_ok());
    }
}
