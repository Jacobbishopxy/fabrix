//! File Xl Executor
//!
//! Executor

use std::{fs::File, marker::PhantomData};

use super::{Cell, Workbook};

/// Xl Data Processor
/// A public trait that defines the interface for a Xl processor.
/// Any type that implements this trait can be treated as 'source' in a streaming process.
/// It can either send parsed data to other `source` such as database and file,
/// or via http or grpc to other services.
pub trait XlDataProcessor {
    type OutType;
    type ErrorType: XlDataProcessorErr;

    /// convert data to output type
    fn transform(cell: Cell) -> Result<Self::OutType, Self::ErrorType>;

    /// process a single row
    fn process_row(batch: Vec<Self::OutType>) -> Result<(), Self::ErrorType>;

    /// process a batch of rows
    fn process_batch(batch: Vec<Vec<Self::OutType>>) -> Result<(), Self::ErrorType>;
}

pub trait XlDataProcessorErr {
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
pub struct XlExecutor<E>
where
    E: XlDataProcessor,
{
    wb: Workbook,
    e: PhantomData<E>,
}

impl<E> XlExecutor<E>
where
    E: XlDataProcessor,
{
    /// constructor
    pub fn new(workbook: Workbook) -> Self {
        Self {
            wb: workbook,
            e: PhantomData,
        }
    }

    /// read a sheet from a workbook
    /// batch size represents the number of rows to process at once
    pub fn read_sheet(
        &mut self,
        sheet: &str,
        batch_size: Option<usize>,
    ) -> Result<(), E::ErrorType> {
        let sheets = self.wb.sheets();
        let sheet = match sheets.get(sheet) {
            Some(ws) => ws,
            None => return Err(E::ErrorType::new("Sheet not found")),
        };

        let mut batch = Vec::new();
        let mut sz = 0usize;

        for row in sheet.rows(&mut self.wb) {
            let row_buf = row
                .data
                .into_iter()
                .map(|c| E::transform(c))
                .collect::<Result<Vec<_>, _>>()?;

            batch.push(row_buf);

            sz += 1;

            if let Some(bs) = batch_size {
                if sz == bs {
                    let mut cache_batch = Vec::new();
                    std::mem::swap(&mut cache_batch, &mut batch);
                    E::process_batch(cache_batch)?;
                    sz = 0;
                } else {
                    continue;
                }
            }
        }

        if batch.len() > 0 {
            let mut cache_batch = Vec::new();
            std::mem::swap(&mut cache_batch, &mut batch);
            E::process_batch(cache_batch)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test_xl_executor {
    use super::*;

    struct TestExec;

    struct TestExecErr(String);

    impl XlDataProcessorErr for TestExecErr {
        fn new<T>(msg: T) -> Self
        where
            T: AsRef<str>,
        {
            Self(msg.as_ref().to_string())
        }
    }

    impl XlDataProcessor for TestExec {
        type OutType = String;
        type ErrorType = TestExecErr;

        fn transform(cell: Cell) -> Result<Self::OutType, Self::ErrorType> {
            Ok(cell.value.to_string())
        }

        fn process_row(batch: Vec<Self::OutType>) -> Result<(), Self::ErrorType> {
            println!("{:?}", batch);
            Ok(())
        }

        fn process_batch(batch: Vec<Vec<Self::OutType>>) -> Result<(), Self::ErrorType> {
            println!("{:?}", batch);
            Ok(())
        }
    }

    #[test]
    fn test_exec() {
        let file = File::open("test.xlsx").unwrap();
        let wb = Workbook::new(file).unwrap();
        let mut xle = XlExecutor::<TestExec>::new(wb);

        if let Ok(_) = xle.read_sheet("Sheet1", None) {
            println!("done");
        }
    }
}
