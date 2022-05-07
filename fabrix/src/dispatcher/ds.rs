//! Dispatcher

use std::marker::PhantomData;

use async_trait::async_trait;

use crate::{Fabrix, FabrixResult};

pub trait ReadOptions<ReaderTag>: Send {}

pub trait WriteOptions<WriterTag>: Send {}

#[async_trait]
pub trait FromSource<ReaderTag> {
    async fn async_read<O>(&mut self, options: O) -> FabrixResult<()>
    where
        O: ReadOptions<ReaderTag>;

    fn sync_read<O>(&mut self, options: O) -> FabrixResult<()>
    where
        O: ReadOptions<ReaderTag>;
}

#[async_trait]
pub trait IntoSource<WriterTag> {
    async fn async_write<O>(&mut self, options: O) -> FabrixResult<()>
    where
        O: WriteOptions<WriterTag>;

    fn sync_write<O>(&mut self, options: O) -> FabrixResult<()>
    where
        O: WriteOptions<WriterTag>;
}

pub struct Dispatcher<R, W, O>
where
    R: FromSource<O>,
    W: IntoSource<O>,
{
    reader: R,
    writer: W,
    options_type: PhantomData<O>,
    pub fabrix: Option<Fabrix>,
}

impl<R, W, O> Dispatcher<R, W, O>
where
    R: FromSource<O>,
    W: IntoSource<O>,
{
    pub fn new(reader: R, writer: W) -> Self {
        Self {
            reader,
            writer,
            options_type: PhantomData,
            fabrix: None,
        }
    }

    pub fn fabrix(&self) -> Option<&Fabrix> {
        self.fabrix.as_ref()
    }

    pub fn read_and_write(
        &mut self,
        read_options: impl ReadOptions<O>,
        write_options: impl WriteOptions<O>,
    ) -> FabrixResult<()> {
        self.reader.sync_read(read_options)?;

        self.writer.sync_write(write_options)?;

        Ok(())
    }
}

pub struct EmptySource;
pub struct EmptyOption;

pub struct EmptyRead;

impl ReadOptions<EmptyOption> for EmptyRead {}

pub struct EmptyWrite;

impl WriteOptions<EmptyOption> for EmptyWrite {}

#[async_trait]
impl FromSource<EmptyOption> for EmptySource {
    async fn async_read<O>(&mut self, _options: O) -> FabrixResult<()>
    where
        O: ReadOptions<EmptyOption>,
    {
        Ok(())
    }

    fn sync_read<O>(&mut self, _options: O) -> FabrixResult<()>
    where
        O: ReadOptions<EmptyOption>,
    {
        Ok(())
    }
}

#[async_trait]
impl IntoSource<EmptyOption> for EmptySource {
    async fn async_write<O>(&mut self, _options: O) -> FabrixResult<()>
    where
        O: WriteOptions<EmptyOption>,
    {
        Ok(())
    }

    fn sync_write<O>(&mut self, _options: O) -> FabrixResult<()>
    where
        O: WriteOptions<EmptyOption>,
    {
        Ok(())
    }
}

#[cfg(test)]
mod dispatcher_tests {
    use super::*;

    #[test]
    fn test_empty_dispatcher() {
        let mut dispatcher = Dispatcher::new(EmptySource, EmptySource);

        let foo = dispatcher.read_and_write(EmptyRead, EmptyWrite);

        assert!(foo.is_ok());

        assert!(dispatcher.fabrix.is_none());
    }
}
