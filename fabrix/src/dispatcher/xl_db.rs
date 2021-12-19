//!  xl -> db
//!
//! - XlDbConvertor
//! - XlDbConsumer
//! - XlDb

use std::sync::Arc;

use itertools::Itertools;
use tokio::sync::Mutex;

use crate::sql::{SqlEngine, SqlExecutor};
use crate::{sql, value, xl, D2Value, DataFrame, FabrixError, FabrixResult, Series, Value};

pub type XlDbExecutor = xl::XlExecutor<SqlExecutor, XlDbConvertor>;

/// XlDbConvertor
///
/// Used for converting D2Value to DataFrame.
/// Notice V2Value can have two kinds of directions: row-wised & column-wised.
/// A convertor's method may be called several times, but only row-wised data's field
/// will be cached. this is because column-wised data should be treated as a whole
/// chunk of data (DataFrame) to be consumed.
pub struct XlDbConvertor {
    pub fields: Option<Vec<String>>,
}

impl XlDbConvertor {
    /// constructor
    pub fn new() -> Self {
        Self { fields: None }
    }

    /// clean fields
    pub fn clean_stats(&mut self) {
        self.fields = None;
    }

    /// set fields, only works for row-wised data
    fn set_row_wised_fields(&mut self, data: &mut D2Value, with_index: bool) {
        // if no fields are defined, use the first row as the fields
        if let None = &self.fields {
            if data.len() == 0 {
                return;
            }
            let mut fld = data
                .remove(0)
                .iter_mut()
                .map(|v| v.to_string())
                .collect_vec();
            // assuming the first cell is the index name, if with_index is true, remove it
            if with_index {
                fld.remove(0);
            }
            self.fields = Some(fld);
        };
    }

    /// transform row-wised data a collection of series
    fn transform_col_wised_data(data: D2Value) -> FabrixResult<Vec<Series>> {
        // even has 1 row the first row is the index, cannot build up a dataframe
        if data.len() <= 1 {
            return Err(FabrixError::new_common_error("data is empty"));
        }

        let mut collection = Vec::new();
        for mut row in data.into_iter() {
            if row.len() == 0 {
                return Err(FabrixError::new_common_error("empty row"));
            }
            // assume the 1st cell is the series name
            let name = row.remove(0).to_string();
            collection.push(Series::from_values(row, name, true)?)
        }

        Ok(collection)
    }

    /// a row-wised 2D-value -> DataFrame, with index
    /// index is always the first column
    pub fn convert_row_wised_with_index(&mut self, mut data: D2Value) -> FabrixResult<DataFrame> {
        self.set_row_wised_fields(&mut data, true);

        let mut df = DataFrame::from_row_values(data, Some(0))?;
        df.set_column_names(self.fields.as_ref().unwrap())?;
        Ok(df)
    }

    /// a row-wised 2D-value -> DataFrame, without index
    pub fn convert_row_wised_no_index(&mut self, mut data: D2Value) -> FabrixResult<DataFrame> {
        self.set_row_wised_fields(&mut data, false);

        let mut df = DataFrame::from_row_values(data, None)?;
        df.set_column_names(self.fields.as_ref().unwrap())?;
        Ok(df)
    }

    /// a column-wised 2D-value -> DataFrame, with index
    /// index is always the first row
    pub fn convert_col_wised_with_index(&self, data: D2Value) -> FabrixResult<DataFrame> {
        let mut collection = Self::transform_col_wised_data(data)?;

        let index = collection.remove(0);

        Ok(DataFrame::from_series(collection, index)?)
    }

    /// a column-wised 2D-value -> DataFrame, without index
    pub fn convert_col_wised_no_index(&self, data: D2Value) -> FabrixResult<DataFrame> {
        let collection = Self::transform_col_wised_data(data)?;

        Ok(DataFrame::from_series_default_index(collection)?)
    }
}

/// XlToDbConsumer
///
/// Used for consuming DataFrame and interacts with database, for instance, inserting or updating data.
///
pub struct XlToDbConsumer {
    pub executor: SqlExecutor,
    pub consume_count: usize,
}

impl XlToDbConsumer {
    pub async fn new(conn: &str) -> FabrixResult<Self> {
        let mut executor = SqlExecutor::from_str(conn);
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
    pub async fn create_new_table(
        &mut self,
        table_name: &str,
        data: DataFrame,
    ) -> FabrixResult<()> {
        let exc = match self.consume_count {
            0 => {
                self.executor
                    .save(table_name, data, &sql::sql_adt::SaveStrategy::FailIfExists)
                    .await
            }
            _ => {
                self.executor
                    .save(table_name, data, &sql::sql_adt::SaveStrategy::Append)
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

    /// replace a table
    pub async fn replace_existing_table(
        &mut self,
        table_name: &str,
        data: DataFrame,
    ) -> FabrixResult<()> {
        let exc = match self.consume_count {
            0 => {
                self.executor
                    .save(table_name, data, &sql::sql_adt::SaveStrategy::Replace)
                    .await
            }
            _ => {
                self.executor
                    .save(table_name, data, &sql::sql_adt::SaveStrategy::Append)
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

    /// upsert a table
    pub async fn upsert_existing_table(
        &mut self,
        table_name: &str,
        data: DataFrame,
    ) -> FabrixResult<()> {
        self.executor
            .save(table_name, data, &sql::sql_adt::SaveStrategy::Upsert)
            .await
            .map(|_| ())
            .map_err(|e| e.into())
    }
}

/// XlDbHelper
///
/// A XlDb is a combinator of convertor and consumer, whereas the consumer is wrapped in `Arc<Mutex<T>>`.
/// This is to ensure the consumer is thread-safe, and can be called by an `async fn`.
pub struct XlDbHelper {
    pub convertor: XlDbConvertor,
    pub consumer: Arc<Mutex<XlToDbConsumer>>,
}

impl XlDbHelper {
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

impl xl::XlConsumer<XlDbConvertor> for SqlExecutor {
    type UnitOut = Value;
    type FinalOut = DataFrame;

    fn transform(cell: xl::Cell) -> Self::UnitOut {
        match cell.value {
            xl::ExcelValue::Bool(v) => value!(v),
            xl::ExcelValue::Number(v) => value!(v),
            xl::ExcelValue::String(v) => value!(v.into_owned()),
            xl::ExcelValue::Date(v) => value!(v),
            xl::ExcelValue::Time(v) => value!(v),
            xl::ExcelValue::DateTime(v) => value!(v),
            xl::ExcelValue::None => Value::Null,
            xl::ExcelValue::Error(v) => value!(v),
        }
    }
}

/// This test case shows a normal process of implement Xl2Db for custom biz logic.
#[cfg(test)]
mod test_xl_reader {

    use super::*;
    use crate::sources::xl::XlSource;
    use crate::sql::SqlEngine;

    const CONN3: &'static str = "sqlite://dev.sqlite";

    #[tokio::test]
    async fn test_xl2db_sync() {
        // Xl read from a path
        let source = XlSource::Path("../mock/test.xlsx");

        // converter & consumer instance
        let mut convertor = XlDbConvertor::new();
        let mut consumer = XlToDbConsumer::new(CONN3).await.unwrap();

        // XlExecutor instance
        let mut xle = XlDbExecutor::new_with_source(source).unwrap();

        // xl sheet iterator
        let iter = xle.iter_sheet(Some(50), "test_table").unwrap();

        // iterate through the sheet, and save the data to db
        for (i, row) in iter.enumerate() {
            let df = convertor.convert_row_wised_no_index(row).unwrap();
            if let Ok(_) = consumer.replace_existing_table("test_table", df).await {
                println!("{:?}: success", i);
            } else {
                println!("{:?}: failed", i);
                break;
            }
        }

        // sql selection
        let select = sql::sql_adt::Select {
            table: "test_table".into(),
            columns: vec![
                "id".into(),
                "first_name".into(),
                "last_name".into(),
                "email".into(),
                "ip_address".into(),
                "birth".into(),
                "issued_date".into(),
                "issued_times".into(),
            ],
            ..Default::default()
        };

        // selected result
        let res = consumer.executor.select(&select).await.unwrap();

        println!("{:?}", res);
    }

    #[tokio::test]
    async fn test_xl2db_async() {
        let source = XlSource::Path("../mock/test.xlsx");

        let convertor = XlDbConvertor::new();
        let consumer = XlToDbConsumer::new(CONN3).await.unwrap();
        let am_consumer = Arc::new(Mutex::new(consumer));

        let mut xle = XlDbExecutor::new_with_source(source).unwrap();

        let foo = xle
            .async_consume_fn_mut(
                Some(40),
                "test_table",
                |d| convertor.convert_col_wised_no_index(d),
                |d| {
                    Box::pin(async {
                        let am = Arc::clone(&am_consumer);
                        let mut lk = am.lock().await;
                        lk.replace_existing_table("test_table", d).await.map(|_| ())
                    })
                },
            )
            .await;

        println!("{:?}", foo);
        println!("{:?}", am_consumer.lock().await.consume_count);
    }

    #[tokio::test]
    async fn test_xl2db_async_helper() {
        let source = XlSource::Path("../mock/test.xlsx");

        // same as the above test case: `test_xl2db_async`
        // simplify the process from naming convertor and consumer separately;
        // however, by this way, we lose the flexibility of customizing the convertor and consumer
        let xl2db = XlDbHelper::new(CONN3).await.unwrap();

        let mut xle = XlDbExecutor::new_with_source(source).unwrap();

        let foo = xle
            .async_consume_fn_mut(
                Some(40),
                "test_table",
                |d| xl2db.convertor.convert_col_wised_no_index(d),
                |d| {
                    Box::pin(async {
                        xl2db
                            .consumer
                            .lock()
                            .await
                            .replace_existing_table("test_table", d)
                            .await
                            .map(|_| ())
                    })
                },
            )
            .await;

        println!("{:?}", foo);
    }
}
