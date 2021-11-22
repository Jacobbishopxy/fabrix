//! File Xl Executor
//!
//! Executor

use std::{fs::File, marker::PhantomData};

use async_trait::async_trait;

use super::{Cell, Workbook};
use crate::FabrixResult;

// TODO: extract common parts from XlDataConsumer<CORE> & XlDataAsyncConsumer<CORE>
// TODO: extract common parts from XlExecutor<E, C> & XlAsyncExecutor<E, C>

/// Xl Data Consumer
/// A public trait that defines the interface for a Xl processor.
/// Any type that implements this trait can be treated as 'source' in a streaming process.
/// It can either send parsed data to other `source` such as database and file,
/// or via http or grpc to other services.
///
/// CORE is a generic type that is used to distinguish different consumers.
/// For instance, a database consumer will have a CORE type of `Database`.
pub trait XlDataConsumer<CORE> {
    type OutType;
    type ErrorType: XlDataConsumerErr;

    /// convert data to output type
    fn transform(cell: Cell) -> Result<Self::OutType, Self::ErrorType>;

    /// consume a single row
    fn consume_row(&mut self, batch: Vec<Self::OutType>) -> Result<(), Self::ErrorType>;

    /// consume a batch of rows
    fn consume_batch(&mut self, batch: Vec<Vec<Self::OutType>>) -> Result<(), Self::ErrorType>;
}

#[async_trait]
pub trait XlDataAsyncConsumer<CORE> {
    type OutType;
    type ErrorType: XlDataConsumerErr;

    /// convert data to output type
    fn transform(cell: Cell) -> Result<Self::OutType, Self::ErrorType>;

    /// consume a single row
    async fn consume_row(&mut self, batch: Vec<Self::OutType>) -> Result<(), Self::ErrorType>;

    /// consume a batch of rows
    async fn consume_batch(
        &mut self,
        batch: Vec<Vec<Self::OutType>>,
    ) -> Result<(), Self::ErrorType>;
}

/// Xl Data Consumer Error
pub trait XlDataConsumerErr {
    fn new<T>(msg: T) -> Self
    where
        T: AsRef<str>;
}

/// Xl file source type
#[derive(Debug)]
pub enum XlSource<'a> {
    File(File),
    Path(&'a str),
    Url(&'a str),
}

/// Xl executor
///
/// wb: Workbook
/// consumer: a concrete type who implemented XlDataConsumer
/// core: a phantom type to distinguish different consumers
pub struct XlExecutor<E, C>
where
    E: XlDataConsumer<C>,
{
    consumer: E,
    workbook: Option<Workbook>,
    core: PhantomData<C>,
}

impl<E, C> XlExecutor<E, C>
where
    E: XlDataConsumer<C>,
{
    /// constructor
    pub fn new(consumer: E) -> FabrixResult<Self> {
        Ok(Self {
            workbook: None,
            consumer,
            core: PhantomData,
        })
    }

    /// constructor
    pub fn new_with_source<'a>(consumer: E, source: XlSource<'a>) -> FabrixResult<Self> {
        let wb = match source {
            XlSource::File(file) => Workbook::new(file)?,
            XlSource::Path(path) => Workbook::new(File::open(path)?)?,
            XlSource::Url(_url) => unimplemented!(),
        };
        let wb = Some(wb);
        Ok(Self {
            workbook: wb,
            consumer,
            core: PhantomData,
        })
    }

    /// replace or set a new workbook
    pub fn add_source<'a>(&mut self, source: XlSource<'a>) -> FabrixResult<()> {
        let wb = match source {
            XlSource::File(file) => Workbook::new(file)?,
            XlSource::Path(path) => Workbook::new(File::open(path)?)?,
            XlSource::Url(_url) => unimplemented!(),
        };
        self.workbook = Some(wb);
        Ok(())
    }

    /// expose consumer as a mutable reference for external usage, this is a self-mutated method
    pub fn consumer(&mut self) -> &mut E {
        &mut self.consumer
    }

    /// read a sheet from a workbook
    /// batch size represents the number of rows to process at once
    pub fn read_sheet(
        &mut self,
        sheet: &str,
        batch_size: Option<usize>,
    ) -> Result<(), E::ErrorType> {
        // workbook must be set
        if let None = self.workbook {
            return Err(E::ErrorType::new("workbook is not initialized"));
        }

        // select a sheet from workbook
        let sheets = self.workbook.take().unwrap().sheets();
        let sheet = match sheets.get(sheet) {
            Some(ws) => ws,
            None => return Err(E::ErrorType::new("Sheet not found")),
        };

        // batch buffer
        let mut batch = Vec::new();
        let mut sz = 0usize;

        // iterate over rows
        for row in sheet.rows(&mut self.workbook.take().unwrap()) {
            let row_buf = row
                .data
                .into_iter()
                .map(|c| E::transform(c))
                .collect::<Result<Vec<_>, _>>()?;

            batch.push(row_buf);
            sz += 1;

            // if batch size is reached, process batch
            if let Some(bs) = batch_size {
                if sz == bs {
                    // swap and clear batch buffer
                    let mut cache_batch = Vec::new();
                    std::mem::swap(&mut cache_batch, &mut batch);
                    // consume batch
                    self.consumer.consume_batch(cache_batch)?;
                    sz = 0;
                } else {
                    continue;
                }
            }
        }

        // consume the remaining batch
        if batch.len() > 0 {
            let mut cache_batch = Vec::new();
            std::mem::swap(&mut cache_batch, &mut batch);
            self.consumer.consume_batch(cache_batch)?;
        }

        Ok(())
    }
}

pub struct XlAsyncExecutor<E, C>
where
    E: XlDataAsyncConsumer<C>,
{
    consumer: E,
    workbook: Option<Workbook>,
    core: PhantomData<C>,
}

impl<E, C> XlAsyncExecutor<E, C>
where
    E: XlDataAsyncConsumer<C>,
{
    /// constructor
    pub fn new(consumer: E) -> FabrixResult<Self> {
        Ok(Self {
            workbook: None,
            consumer,
            core: PhantomData,
        })
    }

    /// constructor
    pub fn new_with_source<'a>(consumer: E, source: XlSource<'a>) -> FabrixResult<Self> {
        let wb = match source {
            XlSource::File(file) => Workbook::new(file)?,
            XlSource::Path(path) => Workbook::new(File::open(path)?)?,
            XlSource::Url(_url) => unimplemented!(),
        };
        let wb = Some(wb);
        Ok(Self {
            workbook: wb,
            consumer,
            core: PhantomData,
        })
    }

    /// replace or set a new workbook
    pub fn add_source<'a>(&mut self, source: XlSource<'a>) -> FabrixResult<()> {
        let wb = match source {
            XlSource::File(file) => Workbook::new(file)?,
            XlSource::Path(path) => Workbook::new(File::open(path)?)?,
            XlSource::Url(_url) => unimplemented!(),
        };
        self.workbook = Some(wb);
        Ok(())
    }

    /// expose consumer as a mutable reference for external usage, this is a self-mutated method
    pub fn consumer(&mut self) -> &mut E {
        &mut self.consumer
    }

    /// read a sheet from a workbook
    /// batch size represents the number of rows to process at once
    pub async fn read_sheet(
        &mut self,
        sheet: &str,
        batch_size: Option<usize>,
    ) -> Result<(), E::ErrorType> {
        // workbook must be set
        if let None = self.workbook {
            return Err(E::ErrorType::new("workbook is not initialized"));
        }

        // select a sheet from workbook
        let sheets = self.workbook.take().unwrap().sheets();
        let sheet = match sheets.get(sheet) {
            Some(ws) => ws,
            None => return Err(E::ErrorType::new("Sheet not found")),
        };

        // batch buffer
        let mut batch = Vec::new();
        let mut sz = 0usize;

        // iterate over rows
        for row in sheet.rows(&mut self.workbook.take().unwrap()) {
            let row_buf = row
                .data
                .into_iter()
                .map(|c| E::transform(c))
                .collect::<Result<Vec<_>, _>>()?;

            batch.push(row_buf);
            sz += 1;

            // if batch size is reached, process batch
            if let Some(bs) = batch_size {
                if sz == bs {
                    // swap and clear batch buffer
                    let mut cache_batch = Vec::new();
                    std::mem::swap(&mut cache_batch, &mut batch);
                    // consume batch
                    self.consumer.consume_batch(cache_batch).await?;
                    sz = 0;
                } else {
                    continue;
                }
            }
        }

        // consume the remaining batch
        if batch.len() > 0 {
            let mut cache_batch = Vec::new();
            std::mem::swap(&mut cache_batch, &mut batch);
            self.consumer.consume_batch(cache_batch).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test_xl_executor {
    use super::*;

    struct TestExec;

    struct TestExecErr(String);

    impl XlDataConsumerErr for TestExecErr {
        fn new<T>(msg: T) -> Self
        where
            T: AsRef<str>,
        {
            Self(msg.as_ref().to_string())
        }
    }

    impl XlDataConsumer<u8> for TestExec {
        type OutType = String;
        type ErrorType = TestExecErr;

        fn transform(cell: Cell) -> Result<Self::OutType, Self::ErrorType> {
            Ok(cell.value.to_string())
        }

        fn consume_row(&mut self, batch: Vec<Self::OutType>) -> Result<(), Self::ErrorType> {
            println!("{:?}", batch);
            Ok(())
        }

        fn consume_batch(&mut self, batch: Vec<Vec<Self::OutType>>) -> Result<(), Self::ErrorType> {
            println!("{:?}", batch);
            Ok(())
        }
    }

    #[test]
    fn test_exec() {
        let source = XlSource::Path("test.xlsx");
        let mut xle = XlExecutor::new_with_source(TestExec, source).unwrap();

        if let Ok(_) = xle.read_sheet("Sheet1", None) {
            println!("done");
        }
    }
}
