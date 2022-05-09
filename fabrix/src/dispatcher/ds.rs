//! Dispatcher

use std::marker::PhantomData;

use async_trait::async_trait;

use crate::{Fabrix, FabrixResult};

pub trait ReadOptions<Tag>: Send {}

pub trait WriteOptions<Tag>: Send {}

#[async_trait]
pub trait FromSource<Tag> {
    async fn async_read<O>(&mut self, options: O) -> FabrixResult<()>
    where
        O: ReadOptions<Tag>;

    fn sync_read<O>(&mut self, options: O) -> FabrixResult<()>
    where
        O: ReadOptions<Tag>;
}

#[async_trait]
pub trait IntoSource<Tag> {
    async fn async_write<O>(&mut self, options: O) -> FabrixResult<()>
    where
        O: WriteOptions<Tag>;

    fn sync_write<O>(&mut self, options: O) -> FabrixResult<()>
    where
        O: WriteOptions<Tag>;
}

pub struct Dispatcher<Reader, Writer, Tag>
where
    Reader: FromSource<Tag>,
    Writer: IntoSource<Tag>,
{
    reader: Reader,
    writer: Writer,
    options_type: PhantomData<Tag>,
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

    pub fn sync_rw(
        &mut self,
        read_options: impl ReadOptions<O>,
        write_options: impl WriteOptions<O>,
    ) -> FabrixResult<()> {
        self.reader.sync_read(read_options)?;

        self.writer.sync_write(write_options)?;

        Ok(())
    }
}

// ================================================================================================
// Empty Read & Write
// ================================================================================================

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

        let foo = dispatcher.sync_rw(EmptyRead, EmptyWrite);

        assert!(foo.is_ok());

        assert!(dispatcher.fabrix.is_none());
    }
}
