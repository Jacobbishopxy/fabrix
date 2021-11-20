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

    fn save(data: DataFrame) -> FabrixResult<()>;
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

    fn consume_row(_batch: Vec<Self::OutType>) -> Result<(), Self::ErrorType> {
        unimplemented!()
    }

    fn consume_batch(batch: Vec<Vec<Self::OutType>>) -> Result<(), Self::ErrorType> {
        let df = T::to_dataframe(batch)?;

        T::save(df)?;

        Ok(())
    }
}

pub trait Xl2Json {
    fn to_json(rows: D2) -> FabrixResult<JsonValue>;

    fn save(data: JsonValue) -> FabrixResult<()>;
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

    fn consume_row(_batch: Vec<Self::OutType>) -> Result<(), Self::ErrorType> {
        unimplemented!()
    }

    fn consume_batch(batch: Vec<Vec<Self::OutType>>) -> Result<(), Self::ErrorType> {
        let json = serde_json::json!(batch);

        T::save(json)?;

        Ok(())
    }
}
