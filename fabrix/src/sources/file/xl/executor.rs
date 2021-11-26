//! File Xl Executor
//!
//! Executor

use std::{fmt::Display, fs::File, marker::PhantomData};

use async_trait::async_trait;

use super::{Cell, RowIter, Workbook};
use crate::{FabrixError, FabrixResult};

/// Xl Data Consumer
///
/// A public trait that defines the interface for a Xl processor.
/// Any type that implements this trait can be treated as 'source' in a streaming process.
/// It can either send parsed data to other `source` such as database and file,
/// or via http or grpc to other services.
///
/// CORE is a generic type that is used to distinguish different consumers.
/// For instance, a database consumer will have a CORE type of `Database`.
#[async_trait]
pub trait XlDataConsumer<CORE> {
    type OutType: Display;

    /// convert data to output type
    fn transform(cell: Cell) -> Self::OutType;

    // TODO: consume data
}

/// Xl file source type
#[derive(Debug)]
pub enum XlSource<'a> {
    File(File),
    Path(&'a str),
    Url(&'a str),
}

/// Xl sheet worker
///
/// used for processing a single sheet, who accepts a batch_size and
/// transform_fn to iterate over a worksheet
struct XlSheetWorker<'a, CONSUMER, CORE>
where
    CONSUMER: XlDataConsumer<CORE>,
{
    batch_size: Option<usize>,
    buffer: RowIter<'a>,
    consumer: PhantomData<CONSUMER>,
    core: PhantomData<CORE>,
}

impl<'a, CONSUMER, CORE> XlSheetWorker<'a, CONSUMER, CORE>
where
    CONSUMER: XlDataConsumer<CORE>,
{
    fn new(
        batch_size: Option<usize>,
        workbook: &'a mut Workbook,
        sheet_name: &str,
    ) -> FabrixResult<Self> {
        let sheets = workbook.sheets();
        let sheet = match sheets.get(sheet_name) {
            Some(ws) => ws,
            None => return Err(FabrixError::new_common_error("Sheet not found")),
        };

        let buffer = sheet.rows(workbook);

        Ok(Self {
            batch_size,
            buffer,
            consumer: PhantomData,
            core: PhantomData,
        })
    }
}

/// Xl sheet iter
pub struct XlSheetIter<'a, CONSUMER, CORE>
where
    CONSUMER: XlDataConsumer<CORE>,
{
    batch_size: Option<usize>,
    buffer: RowIter<'a>,
    consumer: PhantomData<CONSUMER>,
    core: PhantomData<CORE>,
}

impl<'a, CONSUMER, CORE> XlSheetIter<'a, CONSUMER, CORE>
where
    CONSUMER: XlDataConsumer<CORE>,
{
    fn new(batch_size: Option<usize>, buffer: RowIter<'a>) -> Self {
        Self {
            batch_size,
            buffer,
            consumer: PhantomData,
            core: PhantomData,
        }
    }
}

impl<'a, CONSUMER, CORE> Iterator for XlSheetIter<'a, CONSUMER, CORE>
where
    CONSUMER: XlDataConsumer<CORE>,
{
    type Item = Vec<Vec<CONSUMER::OutType>>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut chunk = Vec::new();
        let mut row_count = 0usize;

        for row in &mut self.buffer {
            // turn Vec<Cell> into Vec<OUT>1
            let row_buf = row
                .data
                .into_iter()
                .map(&CONSUMER::transform)
                .collect::<Vec<_>>();

            // accumulate row_buf into chunk
            chunk.push(row_buf);
            row_count += 1;

            match self.batch_size {
                // if batch_size is not None, keep collecting data until batch_size is reached
                Some(bs) => {
                    if row_count == bs {
                        return Some(chunk);
                    } else {
                        continue;
                    }
                }
                // if batch_size is None, keep collecting data until the end of the sheet
                None => {
                    continue;
                }
            }
        }

        // if batch_size is greater than the number of rows in the sheet, return the collected data
        if chunk.len() > 0 {
            Some(chunk)
        } else {
            None
        }
    }
}

impl<'a, CONSUMER, CORE> IntoIterator for XlSheetWorker<'a, CONSUMER, CORE>
where
    CONSUMER: XlDataConsumer<CORE>,
{
    type Item = Vec<Vec<CONSUMER::OutType>>;
    type IntoIter = XlSheetIter<'a, CONSUMER, CORE>;

    fn into_iter(self) -> Self::IntoIter {
        XlSheetIter::new(self.batch_size, self.buffer)
    }
}

/// Xl executor
///
/// wb: Workbook
/// consumer: a concrete type who implemented XlDataConsumer
/// core: a phantom type to distinguish different consumers
pub struct XlExecutor<CONSUMER, CORE>
where
    CONSUMER: XlDataConsumer<CORE>,
{
    consumer: CONSUMER,
    workbook: Option<Workbook>,
    core: PhantomData<CORE>,
}

impl<CONSUMER, CORE> XlExecutor<CONSUMER, CORE>
where
    CONSUMER: XlDataConsumer<CORE>,
{
    /// constructor
    pub fn new(consumer: CONSUMER) -> Self {
        Self {
            consumer,
            workbook: None,
            core: PhantomData,
        }
    }

    /// constructor
    pub fn new_with_source<'a>(consumer: CONSUMER, source: XlSource<'a>) -> FabrixResult<Self> {
        let wb = match source {
            XlSource::File(file) => Workbook::new(file)?,
            XlSource::Path(path) => Workbook::new(File::open(path)?)?,
            XlSource::Url(_url) => unimplemented!(),
        };
        let wb = Some(wb);
        Ok(Self {
            consumer,
            workbook: wb,
            core: PhantomData,
        })
    }

    /// replace or set a new workbook
    pub fn add_source(&mut self, source: XlSource) -> FabrixResult<()> {
        let wb = match source {
            XlSource::File(file) => Workbook::new(file)?,
            XlSource::Path(path) => Workbook::new(File::open(path)?)?,
            XlSource::Url(_url) => unimplemented!(),
        };
        self.workbook = Some(wb);
        Ok(())
    }

    pub fn iter_sheet(
        &mut self,
        batch_size: Option<usize>,
        sheet_name: &str,
    ) -> FabrixResult<XlSheetIter<CONSUMER, CORE>> {
        match &mut self.workbook {
            Some(wb) => {
                let worker = XlSheetWorker::new(batch_size, wb, sheet_name)?;

                Ok(worker.into_iter())
            }
            None => Err(FabrixError::new_common_error("Workbook not found")),
        }
    }
}

/// Simplest consumer that prints the data to stdout
#[cfg(test)]
mod test_xl_executor {
    use super::*;

    struct TestExec;

    impl XlDataConsumer<()> for TestExec {
        type OutType = String;

        fn transform(cell: Cell) -> Self::OutType {
            cell.value.to_string()
        }
    }

    #[test]
    fn test_exec() {
        let source = XlSource::Path("../mock/test.xlsx");
        let mut xle = XlExecutor::new_with_source(TestExec, source).unwrap();

        let foo = xle.iter_sheet(None, "data").unwrap();

        for chunk in foo {
            println!("{:?}", chunk);
        }
    }
}
