//!  xl -> db
//!
//! - XlDbConvertor
//! - XlDbConsumer
//! - XlDb

use std::str::FromStr;
use std::sync::Arc;

use fabrix_core::{value, D2Value, Fabrix, Series, Value};
use fabrix_sql::{sql_adt, DatabaseType, SqlEngine, SqlExecutor, SqlMeta};
use fabrix_xl::{ExcelValue, XlCell, XlConsumer, XlExecutor};
use itertools::Itertools;
use tokio::sync::Mutex;

use crate::{FabrixError, FabrixResult};

pub type XlDbExecutor<R, T> = XlExecutor<SqlExecutor<T>, XlDbConvertor, R>;

/// XlDbConvertor
///
/// Used for converting D2Value to DataFrame.
/// Notice V2Value can have two kinds of directions: row-wise & column-wise.
/// A convertor's method may be called several times, but only row-wise data's field
/// will be cached. this is because column-wise data should be treated as a whole
/// chunk of data (DataFrame) to be consumed.
#[derive(Debug, Default)]
pub struct XlDbConvertor {
    pub fields: Option<Vec<String>>,
}

impl XlDbConvertor {
    /// constructor
    pub fn new() -> Self {
        Self::default()
    }

    /// clean fields
    pub fn clean_stats(&mut self) {
        self.fields = None;
    }

    /// set fields, only works for row-wise data
    fn set_row_wise_fields(
        &mut self,
        data: &mut D2Value,
        index_loc: Option<usize>,
    ) -> FabrixResult<()> {
        // if no fields are defined, use the first row as the fields
        if self.fields.is_none() {
            if data.is_empty() {
                return Ok(());
            }
            // the first row is the fields, remove it
            let fld = data
                .remove(0)
                .iter_mut()
                .map(|v| v.to_string())
                .collect_vec();
            // assuming the first cell is the index name, if with_index is true, remove it
            if let Some(i) = index_loc {
                if i >= fld.len() {
                    return Err(FabrixError::OutOfRange(i, fld.len()));
                }
            }
            self.fields = Some(fld);
        };
        Ok(())
    }

    /// transform row-wise data a collection of series
    fn transform_col_wise_data(data: D2Value) -> FabrixResult<Vec<Series>> {
        // even has 1 row the first row is the index, cannot build up a dataframe
        if data.len() <= 1 {
            return Err(FabrixError::EmptyContent("data"));
        }

        let mut collection = Vec::new();
        for mut row in data.into_iter() {
            if row.is_empty() {
                return Err(FabrixError::EmptyContent("row"));
            }
            // assume the 1st cell is the series name
            let name = row.remove(0).to_string();
            collection.push(Series::from_values(row, name, true)?)
        }

        Ok(collection)
    }

    /// a row-wise 2D-value -> DataFrame, with index
    /// index is always the first column
    pub fn convert_row_wise<'a, T: Into<XlIndexSelection<'a>>>(
        &mut self,
        mut data: D2Value,
        index_col: T,
    ) -> FabrixResult<Fabrix> {
        match index_col.into() {
            XlIndexSelection::Num(num) => {
                self.set_row_wise_fields(&mut data, Some(num))?;
                let mut df = Fabrix::from_row_values(data, Some(num), false)?;
                df.set_column_names(self.fields.as_ref().unwrap())?;
                Ok(df)
            }
            XlIndexSelection::Name(name) => {
                let idx = self
                    .fields
                    .as_ref()
                    .unwrap()
                    .iter()
                    .position(|f| f == name)
                    .ok_or_else(|| {
                        FabrixError::NotFound(format!("index name: {name} not found"))
                    })?;
                self.set_row_wise_fields(&mut data, Some(idx))?;
                let mut df = Fabrix::from_row_values(data, Some(idx), false)?;
                df.set_column_names(self.fields.as_ref().unwrap())?;
                Ok(df)
            }
            XlIndexSelection::None => {
                self.set_row_wise_fields(&mut data, None)?;
                let mut df = Fabrix::from_row_values(data, None, false)?;
                df.set_column_names(self.fields.as_ref().unwrap())?;
                Ok(df)
            }
        }
    }

    /// a column-wise 2D-value -> DataFrame, with index
    /// index is always the first row
    pub fn convert_col_wise<'a, T: Into<XlIndexSelection<'a>>>(
        &self,
        data: D2Value,
        index_col: T,
    ) -> FabrixResult<Fabrix> {
        let collection = Self::transform_col_wise_data(data)?;

        match index_col.into() {
            XlIndexSelection::Num(num) => {
                // extract index
                if num >= collection.len() {
                    return Err(FabrixError::NotFound(format!(
                        "index_col: {num} is out of range"
                    )));
                }

                Ok(Fabrix::from_series(collection, num)?)
            }
            XlIndexSelection::Name(name) => {
                let idx = collection
                    .iter()
                    .position(|s| s.name() == name)
                    .ok_or_else(|| {
                        FabrixError::NotFound(format!("index name: {name} not found"))
                    })?;

                Ok(Fabrix::from_series(collection, idx)?)
            }
            XlIndexSelection::None => Ok(Fabrix::from_series_no_index(collection)?),
        }
    }
}

/// XlIndexSelection
pub enum XlIndexSelection<'a> {
    Num(usize),
    Name(&'a str),
    None,
}

impl<'a> From<usize> for XlIndexSelection<'a> {
    fn from(num: usize) -> Self {
        XlIndexSelection::Num(num)
    }
}

impl<'a> From<&'a str> for XlIndexSelection<'a> {
    fn from(name: &'a str) -> Self {
        XlIndexSelection::Name(name)
    }
}

impl<'a> From<()> for XlIndexSelection<'a> {
    fn from(_: ()) -> Self {
        XlIndexSelection::None
    }
}

/// XlToDbConsumer
///
/// Used for consuming DataFrame and interacts with database, for instance, inserting or updating data.
pub struct XlToDbConsumer<T>
where
    T: DatabaseType,
{
    pub executor: SqlExecutor<T>,
    pub consume_count: usize,
}

impl<T> XlToDbConsumer<T>
where
    T: DatabaseType,
{
    pub async fn new(conn: &str) -> FabrixResult<Self> {
        let mut executor = SqlExecutor::from_str(conn)?;
        executor.connect().await?;
        Ok(Self {
            executor,
            consume_count: 0,
        })
    }

    pub fn clean_stats(&mut self) {
        self.consume_count = 0;
    }

    /// create a table if not exists
    pub async fn create_new_table(&mut self, table_name: &str, data: Fabrix) -> FabrixResult<()> {
        let exc = match self.consume_count {
            0 => {
                self.executor
                    .save(table_name, data, &sql_adt::SaveStrategy::FailIfExists)
                    .await
            }
            _ => {
                self.executor
                    .save(table_name, data, &sql_adt::SaveStrategy::Append)
                    .await
            }
        };

        match exc {
            Ok(_) => {
                self.consume_count += 1;
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    pub async fn append_table(&mut self, table_name: &str, data: Fabrix) -> FabrixResult<()> {
        let exc = self
            .executor
            .save(table_name, data, &sql_adt::SaveStrategy::Append)
            .await;

        match exc {
            Ok(_) => {
                self.consume_count += 1;
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// replace a table
    pub async fn replace_existing_table(
        &mut self,
        table_name: &str,
        data: Fabrix,
        ignore_index: bool,
    ) -> FabrixResult<()> {
        let exc = match self.consume_count {
            0 => {
                self.executor
                    .save(table_name, data, &sql_adt::SaveStrategy::Replace)
                    .await
            }
            _ => {
                if ignore_index {
                    self.executor
                        .save(table_name, data, &sql_adt::SaveStrategy::Append)
                        .await
                } else {
                    self.executor
                        .save(table_name, data, &sql_adt::SaveStrategy::Upsert)
                        .await
                }
            }
        };

        match exc {
            Ok(_) => {
                self.consume_count += 1;
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// upsert a table
    pub async fn upsert_existing_table(
        &mut self,
        table_name: &str,
        data: Fabrix,
    ) -> FabrixResult<()> {
        self.executor
            .save(table_name, data, &sql_adt::SaveStrategy::Upsert)
            .await
            .map(|_| ())
            .map_err(|e| e.into())
    }
}

/// XlDbHelper
///
/// A XlDb is a combinator of convertor and consumer, whereas the consumer is wrapped in `Arc<Mutex<T>>`.
/// This is to ensure the consumer is thread-safe, and can be called by an `async fn`.
pub struct XlDbHelper<T>
where
    T: DatabaseType,
{
    pub convertor: XlDbConvertor,
    pub consumer: Arc<Mutex<XlToDbConsumer<T>>>,
}

impl<T> XlDbHelper<T>
where
    T: DatabaseType,
{
    pub async fn new(conn: &str) -> FabrixResult<Self> {
        let convertor = XlDbConvertor::new();
        let consumer = Arc::new(Mutex::new(XlToDbConsumer::new(conn).await?));
        Ok(Self {
            convertor,
            consumer,
        })
    }

    pub async fn clean_stats(&mut self) {
        self.convertor.clean_stats();
        self.consumer.lock().await.clean_stats();
    }
}

impl<T> XlConsumer<XlDbConvertor> for SqlExecutor<T>
where
    T: DatabaseType,
{
    type UnitOut = Value;
    type FinalOut = Fabrix;

    fn transform(cell: XlCell) -> Self::UnitOut {
        match cell.value {
            ExcelValue::Bool(v) => value!(v),
            ExcelValue::Number(v) => value!(v),
            ExcelValue::String(v) => value!(v.into_owned()),
            ExcelValue::Date(v) => value!(v),
            ExcelValue::Time(v) => value!(v),
            ExcelValue::DateTime(v) => value!(v),
            ExcelValue::None => Value::Null,
            ExcelValue::Error(v) => value!(v),
        }
    }
}

/// This test case shows a normal process of implement Xl2Db for custom biz logic.
#[cfg(test)]
mod test_xl_reader {

    use std::fs::File;

    use fabrix_sql::{DatabaseSqlite, SqlEngine};
    use fabrix_xl::{XlSource, XlWorkbook};

    use super::*;

    const CONN3: &str = "sqlite://dev.sqlite";

    const XL_SOURCE: &str = "../mock/test.xlsx";

    const XL_SHEET_NAME: &str = "data";
    const XL_SHEET_NAME2: &str = "data_t";

    #[test]
    fn test_xl_db_convertor_row_wise() {
        let source: XlWorkbook<File> = XlSource::Path(XL_SOURCE.to_owned()).try_into().unwrap();

        let mut convertor = XlDbConvertor::new();
        let mut xle = XlDbExecutor::<File, DatabaseSqlite>::new_with_source(source);
        let iter = xle.iter_sheet(None, XL_SHEET_NAME).unwrap();

        for (i, row) in iter.enumerate() {
            let df = convertor.convert_row_wise(row, 0).unwrap();

            println!("{i} ==========================================================");
            println!("{:?}", df);
        }
    }

    #[test]
    fn test_xl_db_convertor_col_wise() {
        let source: XlWorkbook<File> = XlSource::Path(XL_SOURCE.to_owned()).try_into().unwrap();

        let convertor = XlDbConvertor::new();
        let mut xle = XlDbExecutor::<File, DatabaseSqlite>::new_with_source(source);
        let iter = xle.iter_sheet(None, XL_SHEET_NAME2).unwrap();

        for (i, row) in iter.enumerate() {
            let df = convertor.convert_col_wise(row, 0).unwrap();

            println!("{i} ==========================================================");
            println!("{:?}", df);
        }
    }

    #[tokio::test]
    async fn test_xl2db_sync() {
        // Xl read from a path
        let source: XlWorkbook<File> = XlSource::Path(XL_SOURCE.to_owned()).try_into().unwrap();

        // converter & consumer instance
        let mut convertor = XlDbConvertor::new();
        let mut consumer = XlToDbConsumer::<DatabaseSqlite>::new(CONN3).await.unwrap();

        // XlExecutor instance
        let mut xle = XlDbExecutor::<File, DatabaseSqlite>::new_with_source(source);

        // xl sheet iterator
        let iter = xle.iter_sheet(Some(40), XL_SHEET_NAME).unwrap();

        // iterate through the sheet, and save the data to db
        for (i, row) in iter.enumerate() {
            let df = convertor
                .convert_row_wise(row, XlIndexSelection::None)
                .unwrap();
            if consumer
                .replace_existing_table("test_table", df, true)
                .await
                .is_ok()
            {
                println!("{:?}: success", i);
            } else {
                println!("{:?}: failed", i);
                break;
            }
        }

        // sql selection
        let select = sql_adt::Select::new("test_table").columns(&[
            "id",
            "first_name",
            "last_name",
            "email",
            "ip_address",
            "birth",
            "issued_date",
            "issued_times",
        ]);

        // selected result
        let res = consumer.executor.select(&select).await;
        assert!(res.is_ok());
        println!("{:?}", res);
    }

    #[tokio::test]
    async fn test_xl2db_async() {
        let source: XlWorkbook<File> = XlSource::Path(XL_SOURCE.to_owned()).try_into().unwrap();

        let convertor = XlDbConvertor::new();
        let consumer = XlToDbConsumer::<DatabaseSqlite>::new(CONN3).await.unwrap();
        let am_consumer = Arc::new(Mutex::new(consumer));

        let mut xle = XlDbExecutor::<File, DatabaseSqlite>::new_with_source(source);

        let foo = xle
            .async_consume_fn_mut(
                Some(40),
                XL_SHEET_NAME2,
                |d| {
                    convertor
                        .convert_col_wise(d, XlIndexSelection::None)
                        .map_err(|e| {
                            fabrix_xl::XlError::Unexpected(format!("fabrix error {:?}", e))
                        })
                },
                |d| {
                    Box::pin(async {
                        let am = Arc::clone(&am_consumer);
                        let mut lk = am.lock().await;
                        lk.replace_existing_table("test_table", d, true)
                            .await
                            .map_err(|e| {
                                fabrix_xl::XlError::Unexpected(format!("fabrix error {:?}", e))
                            })
                            .map(|_| ())
                    })
                },
            )
            .await;

        // assert!(foo.is_ok());
        println!("{:?}", foo);
        println!("{:?}", am_consumer.lock().await.consume_count);
    }

    #[tokio::test]
    async fn test_xl2db_async_helper() {
        let source: XlWorkbook<File> = XlSource::Path(XL_SOURCE.to_owned()).try_into().unwrap();

        // same as the above test case: `test_xl2db_async`
        // simplify the process from naming convertor and consumer separately;
        // however, by this way, we lose the flexibility of customizing the convertor and consumer
        let xl2db = XlDbHelper::<DatabaseSqlite>::new(CONN3).await.unwrap();

        let mut xle = XlDbExecutor::<File, DatabaseSqlite>::new_with_source(source);

        let foo = xle
            .async_consume_fn_mut(
                Some(40),
                XL_SHEET_NAME2,
                |d| {
                    xl2db
                        .convertor
                        .convert_col_wise(d, XlIndexSelection::None)
                        .map_err(|e| {
                            fabrix_xl::XlError::Unexpected(format!("fabrix error {:?}", e))
                        })
                },
                |d| {
                    Box::pin(async {
                        xl2db
                            .consumer
                            .lock()
                            .await
                            .replace_existing_table("test_table", d, true)
                            .await
                            .map_err(|e| {
                                fabrix_xl::XlError::Unexpected(format!("fabrix error {:?}", e))
                            })
                            .map(|_| ())
                    })
                },
            )
            .await;

        assert!(foo.is_ok());
        println!("{:?}", foo);
    }
}
