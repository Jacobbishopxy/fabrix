//! Dispatcher
//!
//! Dispatcher consists in two parts:
//! - Reader: read from one source and save the data in memory
//! - Writer: write the data in memory to one destination
//!
//! Additionally, read & write options are traits that should be implemented

use std::marker::PhantomData;

use async_trait::async_trait;

use crate::{Fabrix, FabrixResult};

// ================================================================================================
// Read & Write Options
// ================================================================================================

pub trait ReadOptions: Send {
    fn source_type(&self) -> &str;
}

pub trait WriteOptions: Send {
    fn source_type(&self) -> &str;
}

// ================================================================================================
// FromSource & IntoSource
// traits that should be implemented by reader and writer respectively
// ================================================================================================

#[async_trait]
pub trait FromSource<R>
where
    R: ReadOptions,
{
    async fn async_read(&mut self, options: R) -> FabrixResult<Fabrix>;

    fn sync_read(&mut self, options: R) -> FabrixResult<Fabrix>;
}

#[async_trait]
pub trait IntoSource<W>
where
    W: WriteOptions,
{
    async fn async_write(&mut self, fabrix: Fabrix, options: W) -> FabrixResult<()>;

    fn sync_write(&mut self, fabrix: Fabrix, options: W) -> FabrixResult<()>;
}

// ================================================================================================
// Dispatcher
// ================================================================================================

pub struct Dispatcher<Reader, Writer, RO, WO>
where
    Reader: FromSource<RO>,
    Writer: IntoSource<WO>,
    RO: ReadOptions,
    WO: WriteOptions,
{
    reader: Reader,
    writer: Writer,
    read_options: PhantomData<RO>,
    write_options: PhantomData<WO>,
    fabrix: Option<Fabrix>,
}

impl<R, W, RO, WO> Dispatcher<R, W, RO, WO>
where
    R: FromSource<RO>,
    W: IntoSource<WO>,
    RO: ReadOptions,
    WO: WriteOptions,
{
    pub fn new(reader: R, writer: W) -> Self {
        Self {
            reader,
            writer,
            read_options: PhantomData,
            write_options: PhantomData,
            fabrix: None,
        }
    }

    pub fn fabrix(&self) -> Option<&Fabrix> {
        self.fabrix.as_ref()
    }

    pub fn reader_mut(&mut self) -> &mut R {
        &mut self.reader
    }

    pub fn writer_mut(&mut self) -> &mut W {
        &mut self.writer
    }

    pub fn sync_read(&mut self, options: RO) -> FabrixResult<()> {
        self.fabrix = Some(self.reader.sync_read(options)?);
        Ok(())
    }

    pub async fn async_read(&mut self, options: RO) -> FabrixResult<()> {
        self.fabrix = Some(self.reader.async_read(options).await?);
        Ok(())
    }

    pub fn sync_write(&mut self, fabrix: Fabrix, options: WO) -> FabrixResult<()> {
        self.writer.sync_write(fabrix, options)
    }

    pub async fn async_write(&mut self, fabrix: Fabrix, options: WO) -> FabrixResult<()> {
        self.writer.async_write(fabrix, options).await
    }
}

// ================================================================================================
// Empty Read & Write
// ================================================================================================

pub struct EmptySource;
pub struct EmptyOption;

impl ReadOptions for EmptyOption {
    fn source_type(&self) -> &str {
        "empty"
    }
}

impl WriteOptions for EmptyOption {
    fn source_type(&self) -> &str {
        "empty"
    }
}

pub struct EmptyRead;

pub struct EmptyWrite;

#[async_trait]
impl FromSource<EmptyOption> for EmptyRead {
    async fn async_read(&mut self, _options: EmptyOption) -> FabrixResult<Fabrix> {
        Ok(Fabrix::empty())
    }

    fn sync_read(&mut self, _options: EmptyOption) -> FabrixResult<Fabrix> {
        Ok(Fabrix::empty())
    }
}

#[async_trait]
impl IntoSource<EmptyOption> for EmptyWrite {
    async fn async_write(&mut self, _fabrix: Fabrix, _options: EmptyOption) -> FabrixResult<()> {
        Ok(())
    }

    fn sync_write(&mut self, _fabrix: Fabrix, _options: EmptyOption) -> FabrixResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod dispatcher_tests {
    use super::*;

    #[test]
    fn test_empty_dispatcher() {
        let mut dispatcher = Dispatcher::new(EmptyRead, EmptyWrite);

        let res = dispatcher.sync_read(EmptyOption);
        assert!(res.is_ok());
        assert!(dispatcher.fabrix().is_none());
    }
}
