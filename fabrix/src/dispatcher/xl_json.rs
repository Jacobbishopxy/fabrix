//! xl -> json

use serde_json::{Number as JsonNumber, Value as JsonValue};

use crate::{xl, D2};

pub type XlJsonExecutor<R> = xl::XlExecutor<XlJson, XlJsonConvertor, R>;

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

    const SOURCE_PATH: &str = "../mock/test.xlsx";
    const SHEET_NAME: &str = "data";

    #[test]
    fn convert_test() {
        let source: Workbook<File> = XlSource::Path(SOURCE_PATH).try_into().unwrap();

        let mut xle = XlJsonExecutor::new_with_source(source).unwrap();

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
        let source: Workbook<File> = XlSource::Path(SOURCE_PATH).try_into().unwrap();

        let mut helper = XlJson::new();

        let mut xle = XlJsonExecutor::new_with_source(source).unwrap();

        xle.consume_fn_mut(
            Some(30),
            SHEET_NAME,
            |d| Ok(XlJson::transform_data(d)),
            |d| {
                // helper.append_data(d);
                let foo: JsonValue = d;
                helper.append_data(foo);
                Ok(())
            },
        )
        .expect("SUCCESS");

        println!("{:?}", helper.data);
    }
}
