//! File Xl Executor
//!
//! Executor

use std::{fs::File, marker::PhantomData};

use super::{Cell, Workbook};

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
    fn consume_row(batch: Vec<Self::OutType>) -> Result<(), Self::ErrorType>;

    /// consume a batch of rows
    fn consume_batch(batch: Vec<Vec<Self::OutType>>) -> Result<(), Self::ErrorType>;
}

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
pub struct XlExecutor<E, C>
where
    E: XlDataConsumer<C>,
{
    wb: Workbook,
    e: PhantomData<E>,
    c: PhantomData<C>,
}

impl<E, C> XlExecutor<E, C>
where
    E: XlDataConsumer<C>,
{
    /// constructor
    pub fn new(workbook: Workbook) -> Self {
        Self {
            wb: workbook,
            e: PhantomData,
            c: PhantomData,
        }
    }

    /// read a sheet from a workbook
    /// batch size represents the number of rows to process at once
    pub fn read_sheet(
        &mut self,
        sheet: &str,
        batch_size: Option<usize>,
    ) -> Result<(), E::ErrorType> {
        // select a sheet from workbook
        let sheets = self.wb.sheets();
        let sheet = match sheets.get(sheet) {
            Some(ws) => ws,
            None => return Err(E::ErrorType::new("Sheet not found")),
        };

        // batch buffer
        let mut batch = Vec::new();
        let mut sz = 0usize;

        // iterate over rows
        for row in sheet.rows(&mut self.wb) {
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
                    E::consume_batch(cache_batch)?;
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
            E::consume_batch(cache_batch)?;
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

        fn consume_row(batch: Vec<Self::OutType>) -> Result<(), Self::ErrorType> {
            println!("{:?}", batch);
            Ok(())
        }

        fn consume_batch(batch: Vec<Vec<Self::OutType>>) -> Result<(), Self::ErrorType> {
            println!("{:?}", batch);
            Ok(())
        }
    }

    #[test]
    fn test_exec() {
        let file = File::open("test.xlsx").unwrap();
        let wb = Workbook::new(file).unwrap();
        let mut xle = XlExecutor::<TestExec, u8>::new(wb);

        if let Ok(_) = xle.read_sheet("Sheet1", None) {
            println!("done");
        }
    }
}
