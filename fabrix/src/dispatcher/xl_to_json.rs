//! xl -> json

use serde_json::Value as JsonValue;

use crate::sources::xl::{Cell, ExcelValue, XlConsumer};
use crate::{FabrixResult, D2};

/// source: json
pub struct Json;

pub trait Xl2Json {
    fn to_json(rows: D2) -> FabrixResult<JsonValue>;

    fn save(&mut self, data: JsonValue) -> FabrixResult<()>;
}

impl<T> XlConsumer<Json> for T
where
    T: Xl2Json,
{
    type UnitOut = JsonValue;
    type FinalOut = JsonValue;

    fn transform(cell: Cell) -> Self::UnitOut {
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
