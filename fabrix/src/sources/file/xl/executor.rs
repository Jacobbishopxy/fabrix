//! File Xl Executor
//!
//! Executor

use std::{fmt::Display, fs::File};

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
pub trait XlDataConsumer<CORE> {
    type OutType;

    /// convert data to output type
    fn transform(cell: Cell) -> Self::OutType;

    /// consume a batch of rows
    fn consume_batch(&mut self, batch: Vec<Vec<Self::OutType>>) -> FabrixResult<()>;
}

#[async_trait]
pub trait XlDataConsumerAsync<CORE> {
    type OutType;

    /// convert data to output type
    fn transform(cell: Cell) -> Self::OutType;

    /// consume a batch of rows
    async fn consume_batch(&mut self, batch: Vec<Vec<Self::OutType>>) -> FabrixResult<()>;
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
pub struct XlSheetWorker<'a, FN, OUT>
where
    FN: Fn(Cell) -> OUT,
{
    transform_fn: FN,
    batch_size: Option<usize>,
    buffer: RowIter<'a>,
}

impl<'a, FN, OUT> XlSheetWorker<'a, FN, OUT>
where
    FN: Fn(Cell) -> OUT,
{
    pub fn new(
        transform_fn: FN,
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
            transform_fn,
            buffer,
        })
    }
}

/// Xl sheet iter
pub struct XlSheetIter<'a, FN, OUT>
where
    FN: Fn(Cell) -> OUT,
    OUT: Display,
{
    transform_fn: FN,
    batch_size: Option<usize>,
    buffer: RowIter<'a>,
}

impl<'a, FN, OUT> XlSheetIter<'a, FN, OUT>
where
    FN: Fn(Cell) -> OUT,
    OUT: Display,
{
    fn new(transform_fn: FN, batch_size: Option<usize>, buffer: RowIter<'a>) -> Self {
        Self {
            transform_fn,
            batch_size,
            buffer,
        }
    }
}

impl<'a, FN, OUT> Iterator for XlSheetIter<'a, FN, OUT>
where
    FN: Fn(Cell) -> OUT,
    OUT: Display,
{
    type Item = Vec<Vec<OUT>>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut chunk: Vec<Vec<OUT>> = Vec::new();
        let mut row_count = 0usize;

        for row in &mut self.buffer {
            // turn Vec<Cell> into Vec<OUT>
            let row_buf = row
                .data
                .into_iter()
                .map(&self.transform_fn)
                .collect::<Vec<OUT>>();

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
        Some(chunk)
    }
}

impl<'a, FN, OUT> IntoIterator for XlSheetWorker<'a, FN, OUT>
where
    FN: Fn(Cell) -> OUT,
    OUT: Display,
{
    type Item = Vec<Vec<OUT>>;
    type IntoIter = XlSheetIter<'a, FN, OUT>;

    fn into_iter(self) -> Self::IntoIter {
        XlSheetIter::new(self.transform_fn, self.batch_size, self.buffer)
    }
}

/// Xl executor
///
/// wb: Workbook
/// consumer: a concrete type who implemented XlDataConsumer
/// core: a phantom type to distinguish different consumers
pub struct XlExecutor {
    workbook: Option<Workbook>,
}

impl XlExecutor {
    /// constructor
    pub fn new() -> Self {
        Self { workbook: None }
    }

    /// constructor
    pub fn new_with_source<'a>(source: XlSource<'a>) -> FabrixResult<Self> {
        let wb = match source {
            XlSource::File(file) => Workbook::new(file)?,
            XlSource::Path(path) => Workbook::new(File::open(path)?)?,
            XlSource::Url(_url) => unimplemented!(),
        };
        let wb = Some(wb);
        Ok(Self { workbook: wb })
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

    pub fn iter_sheet<FN, OUT>(
        &mut self,
        f: FN,
        batch_size: Option<usize>,
        sheet_name: &str,
    ) -> FabrixResult<XlSheetIter<FN, OUT>>
    where
        FN: Fn(Cell) -> OUT,
        OUT: Display,
    {
        match &mut self.workbook {
            Some(wb) => Ok(XlSheetWorker::new(f, batch_size, wb, sheet_name)?.into_iter()),
            None => Err(FabrixError::new_common_error("Workbook not found")),
        }
    }
}

/// Simplest consumer that prints the data to stdout
#[cfg(test)]
mod test_xl_executor {
    use super::*;

    struct TestExec;

    #[test]
    fn test_exec() {
        let source = XlSource::Path("../mock/test.xlsx");
        let mut xle = XlExecutor::new_with_source(source).unwrap();

        let foo = xle.iter_sheet(|c| c.to_string(), None, "data").unwrap();

        for chunk in foo {
            println!("{:?}", chunk);
        }
    }
}
