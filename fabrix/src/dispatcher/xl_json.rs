//! xl -> json

use serde_json::{Number as JsonNumber, Value as JsonValue};

use crate::{ExcelValue, XlCell, XlConsumer, XlExecutor, D2};

pub type XlJsonExecutor<R> = XlExecutor<XlJson, XlJsonConvertor, R>;

#[derive(Default)]
pub struct XlJson {
    pub data: Vec<JsonValue>,
}

impl XlJson {
    pub fn new() -> Self {
        Self { data: vec![] }
    }

    pub fn append_data(&mut self, data: JsonValue) {
        self.data.push(data);
    }

    pub fn clear_data(&mut self) {
        self.data.clear();
    }

    pub fn transform_data(data: D2<JsonValue>) -> JsonValue {
        XlJsonConvertor::transform_data(data)
    }
}

pub struct XlJsonConvertor;

impl XlJsonConvertor {
    pub fn transform_data(data: D2<JsonValue>) -> JsonValue {
        JsonValue::Array(data.into_iter().map(JsonValue::Array).collect())
    }
}

impl XlConsumer<XlJsonConvertor> for XlJson {
    type UnitOut = JsonValue;
    type FinalOut = JsonValue;

    fn transform(cell: XlCell) -> Self::UnitOut {
        match cell.value {
            ExcelValue::Bool(v) => JsonValue::Bool(v),
            ExcelValue::Number(v) => JsonNumber::from_f64(v)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null),
            ExcelValue::String(v) => JsonValue::String(v.into_owned()),
            ExcelValue::Date(v) => JsonValue::String(v.to_string()),
            ExcelValue::Time(v) => JsonValue::String(v.to_string()),
            ExcelValue::DateTime(v) => JsonValue::String(v.to_string()),
            ExcelValue::None => JsonValue::Null,
            ExcelValue::Error(v) => JsonValue::String(v),
        }
    }
}

#[cfg(test)]
mod xl_json_tests {
    use std::fs::File;

    use super::*;
    use crate::{sources::xl::XlSource, XlWorkbook};

    const SOURCE_PATH: &str = "../mock/test.xlsx";
    const SHEET_NAME: &str = "data";

    #[test]
    fn convert_test() {
        let source: XlWorkbook<File> = XlSource::Path(SOURCE_PATH.to_owned()).try_into().unwrap();

        let mut xle = XlJsonExecutor::new_with_source(source);

        xle.consume_fn(
            Some(30),
            SHEET_NAME,
            |d| Ok(XlJson::transform_data(d)),
            |d| {
                println!("{:?}\n\n", d);
                Ok(())
            },
        )
        .expect("SUCCESS");
    }

    #[test]
    fn convert_consume_test() {
        let source: XlWorkbook<File> = XlSource::Path(SOURCE_PATH.to_owned()).try_into().unwrap();

        let mut helper = XlJson::new();

        let mut xle = XlJsonExecutor::new_with_source(source);

        xle.consume_fn_mut(
            Some(30),
            SHEET_NAME,
            |d| Ok(XlJson::transform_data(d)),
            |d| {
                helper.append_data(d);
                Ok(())
            },
        )
        .expect("SUCCESS");

        println!("{:?}", helper.data);
    }
}
