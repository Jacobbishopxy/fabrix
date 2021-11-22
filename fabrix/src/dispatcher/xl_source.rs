//!  Xl reader

use serde_json::Value as JsonValue;

use crate::sources::file::{Cell, ExcelValue, XlDataConsumer, XlDataConsumerErr};
use crate::{value, DataFrame, FabrixError, FabrixResult, Value, D2};

/// source: database
pub struct Db;

/// source: json
pub struct Json;

/// impl XlDataConsumer for FabrixError.
/// This is used in XlDataConsumer<Db> and XlDataConsumer<Json>.
impl XlDataConsumerErr for FabrixError {
    fn new<T>(msg: T) -> Self
    where
        T: AsRef<str>,
    {
        FabrixError::from_common_error(msg.as_ref().to_string())
    }
}

pub trait Xl2Db {
    /// D2 -> dataframe
    ///
    /// Although we already have constructors like `DataFrame::from_rows` and
    /// `DataFrame::from_row_values`, we still need customized impl for business logic.
    /// For instance, if the data layout is row-wised, we need use `DataFrame::from_series`
    /// to build the dataframe. Furthermore, data layout can be complex, customized adaptors
    /// is also required in order to extract useful information from the Xl source and then
    /// convert them into a dataframe.
    fn to_dataframe(rows: D2) -> FabrixResult<DataFrame>;

    /// save data
    ///
    /// Notice this method will be called by `consume_batch`, which is in `XlDataConsumer<Db>`
    /// implementation. If `batch_size` is not none, `consume_batch` could be called several times,
    /// so that this `save` method is in data overridden risk, hence we need to save the data in
    /// a temporary buffer or process it immediately.
    fn save(&mut self, data: DataFrame) -> FabrixResult<()>;
}

impl<T> XlDataConsumer<Db> for T
where
    T: Xl2Db,
{
    type OutType = Value;

    type ErrorType = FabrixError;

    fn transform(cell: Cell) -> Result<Self::OutType, Self::ErrorType> {
        match cell.value {
            ExcelValue::Bool(v) => Ok(value!(v)),
            ExcelValue::Number(v) => Ok(value!(v)),
            ExcelValue::String(v) => Ok(value!(v.into_owned())),
            ExcelValue::Date(v) => Ok(value!(v)),
            ExcelValue::Time(v) => Ok(value!(v)),
            ExcelValue::DateTime(v) => Ok(value!(v)),
            ExcelValue::None => Ok(Value::Null),
            ExcelValue::Error(v) => Ok(value!(v)),
        }
    }

    fn consume_row(&mut self, _batch: Vec<Self::OutType>) -> Result<(), Self::ErrorType> {
        unimplemented!()
    }

    fn consume_batch(&mut self, batch: Vec<Vec<Self::OutType>>) -> Result<(), Self::ErrorType> {
        let df = T::to_dataframe(batch)?;

        self.save(df)?;

        Ok(())
    }
}

pub trait Xl2Json {
    fn to_json(rows: D2) -> FabrixResult<JsonValue>;

    fn save(&mut self, data: JsonValue) -> FabrixResult<()>;
}

impl<T> XlDataConsumer<Json> for T
where
    T: Xl2Json,
{
    type OutType = JsonValue;

    type ErrorType = FabrixError;

    fn transform(cell: Cell) -> Result<Self::OutType, Self::ErrorType> {
        match cell.value {
            ExcelValue::Bool(v) => Ok(serde_json::json!(v)),
            ExcelValue::Number(v) => Ok(serde_json::json!(v)),
            ExcelValue::String(v) => Ok(serde_json::json!(v.into_owned())),
            ExcelValue::Date(v) => Ok(serde_json::json!(v)),
            ExcelValue::Time(v) => Ok(serde_json::json!(v)),
            ExcelValue::DateTime(v) => Ok(serde_json::json!(v)),
            ExcelValue::None => Ok(serde_json::json!(null)),
            ExcelValue::Error(_) => todo!(),
        }
    }

    fn consume_row(&mut self, _batch: Vec<Self::OutType>) -> Result<(), Self::ErrorType> {
        unimplemented!()
    }

    fn consume_batch(&mut self, batch: Vec<Vec<Self::OutType>>) -> Result<(), Self::ErrorType> {
        let json = serde_json::json!(batch);

        self.save(json)?;

        Ok(())
    }
}

/// This test case shows a normal process of implement Xl2Db for custom biz logic.
#[cfg(test)]
mod test_xl_reader {
    use itertools::Itertools;

    use super::*;
    use crate::{adt, SqlEngine, SqlExecutor};

    const CONN3: &'static str = "sqlite://dev.sqlite";

    // mocking a biz logic struct
    pub struct TestXl2Db {
        sql_executor: SqlExecutor,
        cache: Option<DataFrame>,
    }

    impl TestXl2Db {
        fn new(conn_str: &str) -> Self {
            TestXl2Db {
                sql_executor: SqlExecutor::from_str(conn_str),
                cache: None,
            }
        }

        fn sql_executor(&self) -> &SqlExecutor {
            &self.sql_executor
        }

        async fn create_table_and_insert(&mut self, table_name: &str) -> FabrixResult<()> {
            if let Some(d) = self.cache.take() {
                self.sql_executor.connect().await?;

                self.sql_executor
                    .save(table_name, d, &adt::SaveStrategy::Replace)
                    .await?;

                Ok(())
            } else {
                Err(FabrixError::from_common_error("no data"))
            }
        }
    }

    impl Xl2Db for TestXl2Db {
        fn to_dataframe(rows: D2) -> FabrixResult<DataFrame> {
            let mut iter = rows.into_iter();
            // assume the first row is the header, and the data is row-wised
            let column_name = iter
                .next()
                .unwrap()
                .into_iter()
                .map(|v| v.to_string())
                .collect_vec();

            let mut df = DataFrame::from_row_values_iter(iter)?;
            df.set_column_names(&column_name).unwrap();

            Ok(df)
        }

        // WARING: `read_sheet` can be batched-reading, if `batch_size` is not none.
        // In this case, `save` method will be called several times.
        // Hence, in that case, instead of `create_table_and_insert`, we need something like
        // `create_table_and_append` method.
        fn save(&mut self, data: DataFrame) -> FabrixResult<()> {
            self.cache = Some(data);

            Ok(())
        }
    }

    #[tokio::test]
    async fn test_xl2db() {
        use crate::sources::file::{XlExecutor, XlSource};

        // Xl read from a path
        let source = XlSource::Path("test.xlsx");

        // consumer instance
        let consumer = TestXl2Db::new(CONN3);

        // XlExecutor instance
        let mut xle = XlExecutor::new_with_source(consumer, source).unwrap();

        // read sheet, and save in memory
        xle.read_sheet("Sheet1", None).unwrap();

        // memory -> db
        let saved2db = xle.consumer().create_table_and_insert("test_table").await;

        println!("{:?}", saved2db);

        // sql selection
        let select = adt::Select {
            table: "test_table".into(),
            columns: vec![
                "name".into(),
                "title".into(),
                "note".into(),
                "apply_date".into(),
                "submit_date".into(),
                "department".into(),
            ],
            ..Default::default()
        };

        // selected result
        let res = xle.consumer().sql_executor().select(&select).await;

        println!("{:?}", res);
    }
}