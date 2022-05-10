//! File Xl Executor
//!
//! Executor

use std::fmt::Display;
use std::fs::File;
use std::io::{Cursor, Read, Seek};
use std::marker::PhantomData;

use async_trait::async_trait;
use futures::future::BoxFuture;

use super::{Cell, RowIter, Workbook};
use crate::{FabrixError, FabrixResult, D2};

/// A convert function pointer used for converting `D2<UnitOut>` into type parameter `FinalOut`.
pub type ConvertFP<IN, OUT> = fn(IN) -> FabrixResult<OUT>;

/// A synchronous convert function pointer used for consuming type parameter `FinalOut`
pub type SyncConsumeFP<OUT> = fn(OUT) -> FabrixResult<()>;

/// A asynchronous convert function pointer used for consuming type parameter `FinalOut`
pub type AsyncConsumeFP<'a, OUT> = fn(OUT) -> BoxFuture<'a, FabrixResult<()>>;

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
    fn consume(
        chunked_data: Self::FinalOut,
        consume_fn: SyncConsumeFP<Self::FinalOut>,
    ) -> FabrixResult<()> {
        consume_fn(chunked_data)
    }
    /// consume `FinalOut` synchronously
    fn consume_mut<'a>(
        chunked_data: Self::FinalOut,
        mut consume_fn: impl FnMut(Self::FinalOut) -> FabrixResult<()> + 'a,
    ) -> FabrixResult<()> {
        consume_fn(chunked_data)
    }

    /// consume `FinalOut` asynchronously
    async fn consume_async<'a>(
        chunked_data: Self::FinalOut,
        consume_fn: AsyncConsumeFP<'a, Self::FinalOut>,
    ) -> FabrixResult<()>
    where
        Self::FinalOut: 'a,
    {
        Box::pin(consume_fn(chunked_data)).await
    }

    /// consume `FinalOut` asynchronously, with mutable reference `consume_fn`
    async fn consume_async_mut<'a>(
        chunked_data: Self::FinalOut,
        mut consume_fn: impl FnMut(Self::FinalOut) -> BoxFuture<'a, FabrixResult<()>>
            + Send
            + 'async_trait,
    ) -> FabrixResult<()>
    where
        Self::FinalOut: 'a,
    {
        Box::pin(consume_fn(chunked_data)).await
    }
}

/// Xl file source type
#[derive(Debug)]
pub enum XlSource {
    File(File),
    Path(String),
    Url(String),
    Bytes(Cursor<Vec<u8>>),
}

impl TryFrom<XlSource> for Workbook<File> {
    type Error = FabrixError;

    fn try_from(value: XlSource) -> Result<Self, Self::Error> {
        match value {
            XlSource::File(file) => Ok(Workbook::new(file)?),
            XlSource::Path(path) => Ok(Workbook::new(File::open(path)?)?),
            _ => Err(FabrixError::new_common_error("Unsupported XlSource type")),
        }
    }
}

impl TryFrom<XlSource> for Workbook<Cursor<Vec<u8>>> {
    type Error = FabrixError;

    fn try_from(value: XlSource) -> Result<Self, Self::Error> {
        match value {
            XlSource::Bytes(bytes) => Ok(Workbook::new(bytes)?),
            _ => Err(FabrixError::new_common_error("Unsupported XlSource type")),
        }
    }
}

/// Xl worker
///
/// A private struct used for processing a single sheet, who accepts a batch_size and
/// transform_fn to iterate over a worksheet
struct XlWorker<'a, CONSUMER, CORE, READER>
where
    CONSUMER: XlConsumer<CORE>,
    READER: Read + Seek,
{
    batch_size: Option<usize>,
    buffer: RowIter<'a>,
    consumer: PhantomData<CONSUMER>,
    core: PhantomData<CORE>,
    xl_form: PhantomData<READER>,
}

impl<'a, CONSUMER, CORE, READER> XlWorker<'a, CONSUMER, CORE, READER>
where
    CONSUMER: XlConsumer<CORE>,
    READER: Read + Seek,
{
    fn new(
        batch_size: Option<usize>,
        workbook: &'a mut Workbook<READER>,
        sheet_name: &str,
    ) -> FabrixResult<Self> {
        let sheets = workbook.sheets()?;
        let sheet = match sheets.get(sheet_name) {
            Some(ws) => ws,
            None => return Err(FabrixError::new_common_error("Sheet not found")),
        };

        let buffer = sheet.rows(workbook)?;

        Ok(Self {
            batch_size,
            buffer,
            consumer: PhantomData,
            core: PhantomData,
            xl_form: PhantomData,
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

        // TODO:
        // buffer could also contain an error message, please handle this case
        // self.buffer.error
        for row in &mut self.buffer {
            // turn Vec<Cell> into Vec<OUT>
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
        if !chunk.is_empty() {
            Some(chunk)
        } else {
            None
        }
    }
}

/// impl IntoIterator for XlSheetIter
impl<'a, CONSUMER, CORE, READER> IntoIterator for XlWorker<'a, CONSUMER, CORE, READER>
where
    CONSUMER: XlConsumer<CORE>,
    READER: Read + Seek,
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
#[derive(Default)]
pub struct XlExecutor<CONSUMER, CORE, READER>
where
    CONSUMER: XlConsumer<CORE> + Send,
    READER: Read + Seek,
{
    workbook: Option<Workbook<READER>>,
    consumer: PhantomData<CONSUMER>,
    core: PhantomData<CORE>,
    xl_form: PhantomData<READER>,
}

impl<CONSUMER, CORE, READER> XlExecutor<CONSUMER, CORE, READER>
where
    CONSUMER: XlConsumer<CORE> + Send,
    READER: Read + Seek,
{
    /// constructor
    pub fn new() -> Self {
        Self {
            workbook: None,
            consumer: PhantomData,
            core: PhantomData,
            xl_form: PhantomData,
        }
    }

    /// constructor
    pub fn new_with_source(source: Workbook<READER>) -> FabrixResult<Self> {
        Ok(Self {
            workbook: Some(source),
            consumer: PhantomData,
            core: PhantomData,
            xl_form: PhantomData,
        })
    }

    /// replace or set a new workbook
    pub fn add_source(&mut self, source: Workbook<READER>) -> FabrixResult<()> {
        self.workbook = Some(source);
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
        convert_fn: ConvertFP<D2<CONSUMER::UnitOut>, CONSUMER::FinalOut>,
        consume_fn: SyncConsumeFP<CONSUMER::FinalOut>,
    ) -> FabrixResult<()> {
        let iter = gen_worksheet_iter::<CONSUMER, CORE, READER>(
            &mut self.workbook,
            batch_size,
            sheet_name,
        )?;

        for d in iter {
            let cd = convert_fn(d)?;
            CONSUMER::consume(cd, consume_fn)?;
        }

        Ok(())
    }

    /// consume a sheet synchronously
    pub fn consume_fn(
        &mut self,
        batch_size: Option<usize>,
        sheet_name: &str,
        convert_fn: impl Fn(D2<CONSUMER::UnitOut>) -> FabrixResult<CONSUMER::FinalOut>,
        consume_fn: SyncConsumeFP<CONSUMER::FinalOut>,
    ) -> FabrixResult<()> {
        let iter = gen_worksheet_iter::<CONSUMER, CORE, READER>(
            &mut self.workbook,
            batch_size,
            sheet_name,
        )?;

        for d in iter {
            let cd = convert_fn(d)?;
            CONSUMER::consume(cd, consume_fn)?;
        }

        Ok(())
    }

    /// consume a sheet synchronously
    pub fn consume_fn_mut(
        &mut self,
        batch_size: Option<usize>,
        sheet_name: &str,
        mut convert_fn: impl FnMut(D2<CONSUMER::UnitOut>) -> FabrixResult<CONSUMER::FinalOut>,
        mut consume_fn: impl FnMut(CONSUMER::FinalOut) -> FabrixResult<()>,
    ) -> FabrixResult<()> {
        let iter = gen_worksheet_iter::<CONSUMER, CORE, READER>(
            &mut self.workbook,
            batch_size,
            sheet_name,
        )?;

        for d in iter {
            let cd = convert_fn(d)?;
            CONSUMER::consume_mut(cd, &mut consume_fn)?;
        }

        Ok(())
    }

    /// consume a sheet asynchronously
    pub async fn async_consume<'a>(
        &mut self,
        batch_size: Option<usize>,
        sheet_name: &str,
        convert_fn: ConvertFP<D2<CONSUMER::UnitOut>, CONSUMER::FinalOut>,
        consume_fn: AsyncConsumeFP<'a, CONSUMER::FinalOut>,
    ) -> FabrixResult<()> {
        let iter = gen_worksheet_iter::<CONSUMER, CORE, READER>(
            &mut self.workbook,
            batch_size,
            sheet_name,
        )?;

        for d in iter {
            let cd = convert_fn(d)?;
            CONSUMER::consume_async(cd, consume_fn).await?
        }

        Ok(())
    }

    /// consume a sheet asynchronously
    pub async fn async_consume_fn<'a>(
        &mut self,
        batch_size: Option<usize>,
        sheet_name: &str,
        convert_fn: ConvertFP<D2<CONSUMER::UnitOut>, CONSUMER::FinalOut>,
        consume_fn: AsyncConsumeFP<'a, CONSUMER::FinalOut>,
    ) -> FabrixResult<()> {
        let iter = gen_worksheet_iter::<CONSUMER, CORE, READER>(
            &mut self.workbook,
            batch_size,
            sheet_name,
        )?;

        for d in iter {
            let cd = convert_fn(d)?;
            CONSUMER::consume_async(cd, consume_fn).await?
        }

        Ok(())
    }

    /// consume a sheet asynchronously with mutable convert & consume functions
    pub async fn async_consume_fn_mut<'a>(
        &mut self,
        batch_size: Option<usize>,
        sheet_name: &str,
        mut convert_fn: impl FnMut(D2<CONSUMER::UnitOut>) -> FabrixResult<CONSUMER::FinalOut>,
        mut consume_fn: impl FnMut(CONSUMER::FinalOut) -> BoxFuture<'a, FabrixResult<()>> + Send,
    ) -> FabrixResult<()>
    where
        CONSUMER::FinalOut: 'a,
    {
        let iter = gen_worksheet_iter::<CONSUMER, CORE, READER>(
            &mut self.workbook,
            batch_size,
            sheet_name,
        )?;

        let csm = &mut consume_fn;

        for d in iter {
            let cd = convert_fn(d)?;
            CONSUMER::consume_async_mut(cd, &mut *csm).await?
        }

        Ok(())
    }
}

/// Generate a worksheet iterator
///
/// The purpose of this function is to simplify the construction of XlSheetIter,
/// since it will be used in `iter_sheet`, `consume` and `async_consume` methods.
fn gen_worksheet_iter<'a, CONSUMER, CORE, READER: Read + Seek>(
    workbook: &'a mut Option<Workbook<READER>>,
    batch_size: Option<usize>,
    sheet_name: &str,
) -> FabrixResult<XlSheetIter<'a, CONSUMER, CORE>>
where
    CONSUMER: XlConsumer<CORE>,
{
    match workbook {
        Some(wb) => {
            let worker = XlWorker::<CONSUMER, CORE, READER>::new(batch_size, wb, sheet_name)?;

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
    use std::sync::Arc;
    use tokio::sync::Mutex;

    use super::*;

    const XL_PATH: &str = "../mock/test.xlsx";
    const XL_PATH2: &str = "../mock/test2.xlsx";
    const SHEET_NAME: &str = "data";

    // A test struct to implement XlConsumer
    struct TestExec;

    struct Fo(D2<String>);

    impl std::fmt::Display for Fo {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            for row in &self.0 {
                for cell in row {
                    write!(f, "{} \t|", cell)?;
                }
                writeln!(f)?;
            }
            Ok(())
        }
    }

    impl XlConsumer<()> for TestExec {
        type UnitOut = String;
        type FinalOut = Fo;

        fn transform(cell: Cell) -> Self::UnitOut {
            // here to show what information a cell contains
            dbg!(
                &cell.value,
                &cell.formula,
                &cell.reference,
                &cell.style,
                &cell.cell_type,
                &cell.raw_value
            );
            println!("========================================================");
            cell.value.to_string()
        }
    }

    #[test]
    fn test_exec_iter_sheet() {
        let source: Workbook<File> = XlSource::Path(XL_PATH.to_owned()).try_into().unwrap();
        let mut xle = XlExecutor::<TestExec, (), File>::new_with_source(source).unwrap();

        let foo = xle.iter_sheet(None, SHEET_NAME).unwrap();

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

    struct StatefulConsumer {
        count: usize,
    }

    impl StatefulConsumer {
        fn new() -> Self {
            Self { count: 0 }
        }

        async fn async_consume_fn_mut(&mut self, fo: Fo) -> FabrixResult<()> {
            println!("{}\n\n", fo);
            self.count += 1;
            Ok(())
        }
    }

    #[test]
    fn test_value_console() {
        let source: Workbook<File> = XlSource::Path(XL_PATH2.to_owned()).try_into().unwrap();
        let mut xle = XlExecutor::<TestExec, (), File>::new_with_source(source).unwrap();

        let foo = xle.consume(Some(20), SHEET_NAME, convert_fn, consume_fn);

        assert!(foo.is_ok());
    }

    // consume synchronously
    #[test]
    fn test_exec_consume() {
        let source: Workbook<File> = XlSource::Path(XL_PATH.to_owned()).try_into().unwrap();
        let mut xle = XlExecutor::<TestExec, (), File>::new_with_source(source).unwrap();

        let foo = xle.consume(Some(20), SHEET_NAME, convert_fn, consume_fn);

        println!("{:?}", foo);
    }

    // consume synchronously
    #[tokio::test]
    async fn test_exec_async_consume() {
        let source: Workbook<File> = XlSource::Path(XL_PATH.to_owned()).try_into().unwrap();
        let mut xle = XlExecutor::<TestExec, (), File>::new_with_source(source).unwrap();

        let foo = xle
            .async_consume(Some(20), SHEET_NAME, convert_fn, |fo| {
                Box::pin(async_consume_fn(fo))
            })
            .await;

        println!("{:?}", foo);
    }

    // consume synchronously, mutable
    #[tokio::test]
    async fn test_exec_async_consume_mut() {
        let source: Workbook<File> = XlSource::Path(XL_PATH.to_owned()).try_into().unwrap();
        let mut xle = XlExecutor::<TestExec, (), File>::new_with_source(source).unwrap();

        let sc = Arc::new(Mutex::new(StatefulConsumer::new()));

        let foo = xle
            .async_consume_fn_mut(Some(20), SHEET_NAME, convert_fn, |fo| {
                Box::pin(async {
                    let am = Arc::clone(&sc);
                    let mut lk = am.lock().await;

                    lk.async_consume_fn_mut(fo).await
                })
            })
            .await;

        println!("{:?}", foo);
        println!("{:?}", sc.lock().await.count);
    }
}
