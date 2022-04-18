//! xl -> json

use serde_json::{Number as JsonNumber, Value as JsonValue};

use crate::{xl, D2};

pub type XlJsonExecutor<R> = xl::XlExecutor<XlJson, XlJsonConvertor, R>;

pub struct XlJson;

pub struct XlJsonConvertor;

impl XlJsonConvertor {
    pub fn transform_data(data: D2<JsonValue>) -> JsonValue {
        JsonValue::Array(data.into_iter().map(JsonValue::Array).collect())
    }
}

impl xl::XlConsumer<XlJsonConvertor> for XlJson {
    type UnitOut = JsonValue;
    type FinalOut = JsonValue;

    fn transform(cell: xl::Cell) -> Self::UnitOut {
        match cell.value {
            xl::ExcelValue::Bool(v) => JsonValue::Bool(v),
            xl::ExcelValue::Number(v) => JsonNumber::from_f64(v)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null),
            xl::ExcelValue::String(v) => JsonValue::String(v.into_owned()),
            xl::ExcelValue::Date(v) => JsonValue::String(v.to_string()),
            xl::ExcelValue::Time(v) => JsonValue::String(v.to_string()),
            xl::ExcelValue::DateTime(v) => JsonValue::String(v.to_string()),
            xl::ExcelValue::None => JsonValue::Null,
            xl::ExcelValue::Error(v) => JsonValue::String(v),
        }
    }
}

#[cfg(test)]
mod xl_json_tests {
    use std::fs::File;

    use super::*;
    use crate::{sources::xl::XlSource, xl::Workbook};

    #[test]
    fn convert_test() {
        let source: Workbook<File> = XlSource::Path("../mock/test.xlsx").try_into().unwrap();

        let mut xle = XlJsonExecutor::new_with_source(source).unwrap();

        let foo = xle.consume_fn(
            Some(30),
            "data",
            |d| Ok(XlJsonConvertor::transform_data(d)),
            |d| {
                println!("{:?}\n\n", d);
                Ok(())
            },
        );

        println!("{:?}", foo);
    }
}
