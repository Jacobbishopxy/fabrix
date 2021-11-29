//! File Xl Executor
//!
//! Executor

use std::{fmt::Display, fs::File, marker::PhantomData};

use async_trait::async_trait;
use futures::future::BoxFuture;

use super::{Cell, RowIter, Workbook};
use crate::{FabrixError, FabrixResult, D2};

/// A convert function pointer used for converting `D2<UnitOut>` into type parameter `FinalOut`.
pub type ConvertFn<IN, OUT> = fn(IN) -> FabrixResult<OUT>;

/// A synchronous convert function pointer used for consuming type parameter `FinalOut`
pub type SyncConsumeFn<OUT> = fn(OUT) -> FabrixResult<()>;

/// A asynchronous convert function pointer used for consuming type parameter `FinalOut`
pub type AsyncConsumeFn<'a, OUT> = fn(OUT) -> BoxFuture<'a, FabrixResult<()>>;

/// Xl Consumer
///
/// A public trait that defines the interface for a Xl consumer.
///
/// To implement this trait:
/// - `UnitOut` which represents the converted type of the `Cell`,
/// - `FinalOut` which represents the output of the converted `ChunkCell`.
/// - `transform` which converts the `Cell` into `UnitOut`.
///
/// Notice `consume_sync` and `consume_async` already has default implementation,
/// so normally we don't have to implement them. The purpose of these two methods
/// is to consume the `FinalOut`, but the `FinalOut` cannot be known until the
/// `ConvertFn<IN, OUT>` is given. That is to say, the `D2<UnitOut>` -> `FinalOut`
/// process is plug-able, and we can use different `ConvertFn` to make data
/// transformation more flexible.
///
/// Any type who implements this trait can be treated as the 'source' of a stream.
/// It can either send parsed data to other `source` such as database and file,
/// or via http or grpc to other services.
///
/// CORE is a generic type that is used to distinguish different consumers.
/// For instance, a database consumer will have a CORE type of `Database`.
#[async_trait]
pub trait XlConsumer<CORE> {
    type UnitOut: Display + Send;
    type FinalOut: Display + Send;

    /// convert data to output type
    fn transform(cell: Cell) -> Self::UnitOut;

    /// consume `FinalOut` synchronously
    fn consume<'a>(
        &mut self,
        chunked_data: Self::FinalOut,
        consume_fn: SyncConsumeFn<Self::FinalOut>,
    ) -> FabrixResult<()> {
        consume_fn(chunked_data)
    }

    /// consume `FinalOut` asynchronously
    async fn consume_async<'a>(
        &mut self,
        chunked_data: Self::FinalOut,
        consume_fn: AsyncConsumeFn<'a, Self::FinalOut>,
    ) -> FabrixResult<()>
    where
        Self::FinalOut: 'a,
    {
        Box::pin(async { consume_fn(chunked_data).await }).await
    }
}

/// Xl file source type
#[derive(Debug)]
pub enum XlSource<'a> {
    File(File),
    Path(&'a str),
    Url(&'a str),
}

/// Xl worker
///
/// A private struct used for processing a single sheet, who accepts a batch_size and
/// transform_fn to iterate over a worksheet
struct XlWorker<'a, CONSUMER, CORE>
where
    CONSUMER: XlConsumer<CORE>,
{
    batch_size: Option<usize>,
    buffer: RowIter<'a>,
    consumer: PhantomData<CONSUMER>,
    core: PhantomData<CORE>,
}

impl<'a, CONSUMER, CORE> XlWorker<'a, CONSUMER, CORE>
where
    CONSUMER: XlConsumer<CORE>,
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
///
/// A public struct used for iterating over a worksheet.
pub struct XlSheetIter<'a, CONSUMER, CORE>
where
    CONSUMER: XlConsumer<CORE>,
{
    batch_size: Option<usize>,
    buffer: RowIter<'a>,
    consumer: PhantomData<CONSUMER>,
    core: PhantomData<CORE>,
}

impl<'a, CONSUMER, CORE> XlSheetIter<'a, CONSUMER, CORE>
where
    CONSUMER: XlConsumer<CORE>,
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
    CONSUMER: XlConsumer<CORE>,
{
    type Item = D2<CONSUMER::UnitOut>;

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

/// impl IntoIterator for XlSheetIter
impl<'a, CONSUMER, CORE> IntoIterator for XlWorker<'a, CONSUMER, CORE>
where
    CONSUMER: XlConsumer<CORE>,
{
    type Item = D2<CONSUMER::UnitOut>;
    type IntoIter = XlSheetIter<'a, CONSUMER, CORE>;

    fn into_iter(self) -> Self::IntoIter {
        XlSheetIter::new(self.batch_size, self.buffer)
    }
}

/// Xl executor
///
/// consumer: a concrete type who implemented XlDataConsumer
/// workbook: working resource
/// core: a phantom type to distinguish different consumers
pub struct XlExecutor<CONSUMER, CORE>
where
    CONSUMER: XlConsumer<CORE> + Send,
{
    consumer: CONSUMER,
    workbook: Option<Workbook>,
    core: PhantomData<CORE>,
}

impl<CONSUMER, CORE> XlExecutor<CONSUMER, CORE>
where
    CONSUMER: XlConsumer<CORE> + Send,
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

    pub fn consumer(&mut self) -> &mut CONSUMER {
        &mut self.consumer
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

    /// return an iterator of xl sheet data
    pub fn iter_sheet(
        &mut self,
        batch_size: Option<usize>,
        sheet_name: &str,
    ) -> FabrixResult<XlSheetIter<CONSUMER, CORE>> {
        gen_worksheet_iter(&mut self.workbook, batch_size, sheet_name)
    }

    /// consume a sheet synchronously
    pub fn consume(
        &mut self,
        batch_size: Option<usize>,
        sheet_name: &str,
        convert_fn: ConvertFn<D2<CONSUMER::UnitOut>, CONSUMER::FinalOut>,
        consume_fn: SyncConsumeFn<CONSUMER::FinalOut>,
    ) -> FabrixResult<()> {
        let iter =
            gen_worksheet_iter::<CONSUMER, CORE>(&mut self.workbook, batch_size, sheet_name)?;

        for d in iter {
            let cd = convert_fn(d)?;
            self.consumer.consume(cd, consume_fn)?;
        }

        Ok(())
    }

    /// consume a sheet asynchronously
    pub async fn async_consume<'a>(
        &mut self,
        batch_size: Option<usize>,
        sheet_name: &str,
        convert_fn: ConvertFn<D2<CONSUMER::UnitOut>, CONSUMER::FinalOut>,
        consume_fn: AsyncConsumeFn<'a, CONSUMER::FinalOut>,
    ) -> FabrixResult<()> {
        let iter =
            gen_worksheet_iter::<CONSUMER, CORE>(&mut self.workbook, batch_size, sheet_name)?;

        for d in iter {
            let cd = convert_fn(d)?;
            self.consumer.consume_async(cd, consume_fn).await?
        }

        Ok(())
    }
}

/// Generate a worksheet iterator
///
/// The purpose of this function is to simplify the construction of XlSheetIter,
/// since it will be used in `iter_sheet`, `consume` and `async_consume` methods.
fn gen_worksheet_iter<'a, CONSUMER, CORE>(
    workbook: &'a mut Option<Workbook>,
    batch_size: Option<usize>,
    sheet_name: &str,
) -> FabrixResult<XlSheetIter<'a, CONSUMER, CORE>>
where
    CONSUMER: XlConsumer<CORE>,
{
    match workbook {
        Some(wb) => {
            let worker = XlWorker::<CONSUMER, CORE>::new(batch_size, wb, sheet_name)?;

            Ok(worker.into_iter())
        }
        None => Err(FabrixError::new_common_error("Workbook not found")),
    }
}

/// Simplest consumer that prints the data to stdout
///
/// - `iter_sheet`
/// - `consume`
/// - `async_consume`
#[cfg(test)]
mod test_xl_executor {
    use super::*;

    // A test struct to implement XlConsumer
    struct TestExec;

    struct Fo(D2<String>);

    impl std::fmt::Display for Fo {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            for row in &self.0 {
                for cell in row {
                    write!(f, "{} \t|", cell)?;
                }
                write!(f, "\n")?;
            }
            Ok(())
        }
    }

    impl XlConsumer<()> for TestExec {
        type UnitOut = String;
        type FinalOut = Fo;

        fn transform(cell: Cell) -> Self::UnitOut {
            cell.value.to_string()
        }
    }

    #[test]
    fn test_exec_iter_sheet() {
        let source = XlSource::Path("../mock/test.xlsx");
        let mut xle = XlExecutor::new_with_source(TestExec, source).unwrap();

        let foo = xle.iter_sheet(None, "data").unwrap();

        for chunk in foo {
            println!("{:?}", chunk);
        }
    }

    fn convert_fn(data: D2<String>) -> FabrixResult<Fo> {
        Ok(Fo(data))
    }

    fn consume_fn(fo: Fo) -> FabrixResult<()> {
        println!("{}\n\n", fo);
        Ok(())
    }

    async fn async_consume_fn(fo: Fo) -> FabrixResult<()> {
        println!("{}\n\n", fo);
        Ok(())
    }

    // consume synchronously
    #[test]
    fn test_exec_consume() {
        let source = XlSource::Path("../mock/test.xlsx");
        let mut xle = XlExecutor::new_with_source(TestExec, source).unwrap();

        let foo = xle.consume(
            Some(20),
            "data",
            convert_fn as ConvertFn<D2<String>, Fo>,
            consume_fn as SyncConsumeFn<Fo>,
        );

        println!("{:?}", foo);
    }

    // consume synchronously
    #[tokio::test]
    async fn test_exec_async_consume() {
        let source = XlSource::Path("../mock/test.xlsx");
        let mut xle = XlExecutor::new_with_source(TestExec, source).unwrap();

        let foo = xle
            .async_consume(
                Some(20),
                "data",
                convert_fn as ConvertFn<D2<String>, Fo>,
                (|fo| Box::pin(async_consume_fn(fo))) as AsyncConsumeFn<Fo>,
            )
            .await;

        println!("{:?}", foo);
    }
}
