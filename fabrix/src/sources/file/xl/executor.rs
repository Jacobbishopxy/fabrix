//! File Xl Executor
//!
//! Executor

use std::{fmt::Display, fs::File, marker::PhantomData};

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
    fn transform(cell: Cell) -> FabrixResult<Self::OutType>;

    /// consume a single row
    fn consume_row(&mut self, batch: Vec<Self::OutType>) -> FabrixResult<()>;

    /// consume a batch of rows
    fn consume_batch(&mut self, batch: Vec<Vec<Self::OutType>>) -> FabrixResult<()>;
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
    batch_size: Option<usize>,
    transform_fn: PhantomData<FN>,
    buffer: RowIter<'a>,
}

impl<'a, FN, OUT> XlSheetWorker<'a, FN, OUT>
where
    FN: Fn(Cell) -> OUT,
{
    pub fn new(
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
            transform_fn: PhantomData,
            buffer,
        })
    }
}

/// Xl sheet iter
///
pub struct XlSheetIter<'a, FN, OUT>
where
    FN: Fn(Cell) -> OUT,
    OUT: Display,
{
    batch_size: Option<usize>,
    transform_fn: FN,
    buffer: RowIter<'a>,
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
        todo!()
    }
}

/// Xl executor
///
/// wb: Workbook
/// consumer: a concrete type who implemented XlDataConsumer
/// core: a phantom type to distinguish different consumers
pub struct XlExecutor<CONSUMER, TYPE>
where
    CONSUMER: XlDataConsumer<TYPE>,
{
    consumer: CONSUMER,
    workbook: Option<Workbook>,
    core: PhantomData<TYPE>,
}

impl<CONSUMER, TYPE> XlExecutor<CONSUMER, TYPE>
where
    CONSUMER: XlDataConsumer<TYPE>,
{
    /// constructor
    pub fn new(consumer: CONSUMER) -> FabrixResult<Self> {
        Ok(Self {
            workbook: None,
            consumer,
            core: PhantomData,
        })
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
    pub fn consumer(&mut self) -> &mut CONSUMER {
        &mut self.consumer
    }

    /// read a sheet from a workbook
    /// batch size represents the number of rows to process at once
    pub fn read_sheet(&mut self, sheet: &str, batch_size: Option<usize>) -> FabrixResult<()> {
        match &mut self.workbook {
            Some(wb) => {
                // select a sheet from workbook
                let sheets = wb.sheets();
                let sheet = match sheets.get(sheet) {
                    Some(ws) => ws,
                    None => return Err(FabrixError::new_common_error("Sheet not found")),
                };

                // batch buffer
                let mut batch = Vec::new();
                let mut sz = 0usize;

                // iterate over rows
                for row in sheet.rows(wb) {
                    let row_buf = row
                        .data
                        .into_iter()
                        .map(|c| CONSUMER::transform(c))
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
            // workbook must be set
            None => return Err(FabrixError::new_common_error("workbook is not initialized")),
        }
    }
}

/// Simplest consumer that prints the data to stdout
#[cfg(test)]
mod test_xl_executor {
    use super::*;

    struct TestExec;

    impl XlDataConsumer<u8> for TestExec {
        type OutType = String;

        fn transform(cell: Cell) -> FabrixResult<Self::OutType> {
            Ok(cell.value.to_string())
        }

        fn consume_row(&mut self, batch: Vec<Self::OutType>) -> FabrixResult<()> {
            println!("{:?}", batch);
            Ok(())
        }

        fn consume_batch(&mut self, batch: Vec<Vec<Self::OutType>>) -> FabrixResult<()> {
            println!("{:?}", batch);
            Ok(())
        }
    }

    #[test]
    fn test_exec() {
        let source = XlSource::Path("../mock/test.xlsx");
        let mut xle = XlExecutor::new_with_source(TestExec, source).unwrap();

        if let Ok(_) = xle.read_sheet("data", None) {
            println!("done");
        }
    }
}
