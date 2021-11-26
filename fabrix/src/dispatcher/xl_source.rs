//!  Xl reader

use async_trait::async_trait;
use futures::future::BoxFuture;
use serde_json::Value as JsonValue;

use crate::sources::file::executor::XlDataConsumer;
use crate::sources::file::{Cell, ExcelValue};
use crate::{value, DataFrame, FabrixResult, Value, D2};

/// source: database
pub struct Db;

/// source: json
pub struct Json;

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
    ///
    /// The reason why we need a `save` method is because save data into a database is a async
    /// process, and the function who called it is not async, so we cannot simply put an async
    /// function in to a sync function.
    fn save(&mut self, data: DataFrame) -> FabrixResult<()>;
}

impl<T> XlDataConsumer<Db> for T
where
    T: Xl2Db,
{
    type OutType = Value;

    fn transform(cell: Cell) -> Self::OutType {
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

#[async_trait]
pub trait Xl2DbAsync: Send + Sync {
    fn to_dataframe(rows: D2) -> FabrixResult<DataFrame>;

    async fn save_fn(&mut self, df: DataFrame) -> BoxFuture<'static, FabrixResult<()>>;
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

    fn transform(cell: Cell) -> Self::OutType {
        match cell.value {
            ExcelValue::Bool(v) => serde_json::json!(v),
            ExcelValue::Number(v) => serde_json::json!(v),
            ExcelValue::String(v) => serde_json::json!(v.into_owned()),
            ExcelValue::Date(v) => serde_json::json!(v),
            ExcelValue::Time(v) => serde_json::json!(v),
            ExcelValue::DateTime(v) => serde_json::json!(v),
            ExcelValue::None => serde_json::json!(null),
            ExcelValue::Error(_) => todo!(),
        }
    }
}

/// This test case shows a normal process of implement Xl2Db for custom biz logic.
///
/// This is actually not a good experience, since streaming process is required when saving
/// converted data into a database, whereas a sync process is still required for other
/// use cases, such as writing to another file.
#[cfg(test)]
mod test_xl_reader {
    use itertools::Itertools;

    use super::*;
    use crate::{sql_adt, FabrixError, SqlEngine, SqlExecutor};

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
                    .save(table_name, d, &sql_adt::SaveStrategy::Replace)
                    .await?;

                Ok(())
            } else {
                Err(FabrixError::new_common_error("no data"))
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
            dbg!(&df);
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
        let source = XlSource::Path("../mock/test.xlsx");

        // consumer instance
        let consumer = TestXl2Db::new(CONN3);

        // XlExecutor instance
        // let mut xle = XlExecutor::new_with_source(source).unwrap();

        // read sheet, and save converted data into memory
        // let iter = xle.iter_sheet(|c| c.to_string(), None, "data").unwrap();

        // memory -> db
        // let saved2db = xle.consumer().create_table_and_insert("test_table").await;

        // println!("{:?}", saved2db);

        // sql selection
        let select = sql_adt::Select {
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
        // let res = xle.consumer().sql_executor().select(&select).await;

        // println!("{:?}", res);
    }
}
