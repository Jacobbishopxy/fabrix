//! Dispatcher

use std::marker::PhantomData;

use async_trait::async_trait;

use crate::{Fabrix, FabrixResult};

#[async_trait]
pub trait FromSource {
    async fn async_read(&mut self) -> FabrixResult<()>;

    fn sync_read(&mut self) -> FabrixResult<()>;
}

#[async_trait]
pub trait IntoSource {
    async fn async_write(&mut self) -> FabrixResult<()>;

    fn sync_write(&mut self) -> FabrixResult<()>;
}

pub struct Dispatcher<F, T>
where
    F: FromSource,
    T: IntoSource,
{
    from: PhantomData<F>,
    to: PhantomData<T>,
    pub data: Option<Fabrix>,
}

impl<F, T> Dispatcher<F, T>
where
    F: FromSource,
    T: IntoSource,
{
    // TODO:
}

pub struct EmptySource;

#[async_trait]
impl FromSource for EmptySource {
    async fn async_read(&mut self) -> FabrixResult<()> {
        Ok(())
    }

    fn sync_read(&mut self) -> FabrixResult<()> {
        Ok(())
    }
}

#[async_trait]
impl IntoSource for EmptySource {
    async fn async_write(&mut self) -> FabrixResult<()> {
        Ok(())
    }

    fn sync_write(&mut self) -> FabrixResult<()> {
        Ok(())
    }
}
