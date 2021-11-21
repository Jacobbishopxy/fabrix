//!  Xl reader

use serde_json::Value as JsonValue;

use crate::sources::file::{Cell, ExcelValue, XlDataConsumer, XlDataConsumerErr};
use crate::{value, DataFrame, FabrixError, FabrixResult, Value, D2};

impl XlDataConsumerErr for FabrixError {
    fn new<T>(msg: T) -> Self
    where
        T: AsRef<str>,
    {
        FabrixError::from_common_error(msg.as_ref().to_string())
    }
}

/// source: database
pub struct Db;

/// source: json
pub struct Json;

pub trait Xl2Db {
    /// D2 -> dataframe
    /// Although we already have `DataFrame::from_rows` and ``DataFrame::from_row_values`,
    /// we still need customized impl for business logic.
    fn to_dataframe(rows: D2) -> FabrixResult<DataFrame>;

    /// save data
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

#[cfg(test)]
mod test_xl_reader {
    use super::*;
    use crate::{adt, SqlEngine, SqlExecutor};

    const CONN3: &'static str = "sqlite://dev.sqlite";

    pub struct TestXl2Db {
        conn_str: String,
        cache: Option<DataFrame>,
    }

    impl Xl2Db for TestXl2Db {
        fn to_dataframe(rows: D2) -> FabrixResult<DataFrame> {
            let df = DataFrame::from_row_values(rows)?;

            Ok(df)
        }

        fn save(&mut self, data: DataFrame) -> FabrixResult<()> {
            self.cache = Some(data);

            Ok(())
        }
    }

    impl TestXl2Db {
        fn new(conn_str: &str) -> Self {
            TestXl2Db {
                conn_str: conn_str.to_string(),
                cache: None,
            }
        }

        async fn create_table_and_insert(&mut self, table_name: &str) -> FabrixResult<()> {
            if let Some(d) = self.cache.take() {
                let mut exc = SqlExecutor::from_str(&self.conn_str);
                exc.connect().await?;

                exc.save(table_name, d, &adt::SaveStrategy::Replace).await?;

                Ok(())
            } else {
                Err(FabrixError::from_common_error("no data"))
            }
        }
    }

    #[tokio::test]
    async fn test_xl2db() {
        use crate::sources::file::{XlExecutor, XlSource};

        let source = XlSource::Path("test.xlsx");

        let test_piper = TestXl2Db::new(CONN3);

        let mut xle = XlExecutor::new_with_source(test_piper, source).unwrap();

        xle.read_sheet("Sheet1", None).unwrap();

        let foo = xle.consumer().create_table_and_insert("test_table").await;

        println!("{:?}", foo);
    }
}
