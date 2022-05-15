//! xl -> json
//!
//!

use bson2::{Bson, DateTime};
use chrono::Timelike;

use crate::{ExcelValue, XlCell, XlConsumer, XlExecutor, D2};

pub type XlBsonExecutor<R> = XlExecutor<XlBson, XlBsonConvertor, R>;

pub struct XlBson;

pub struct XlBsonConvertor;

impl XlBsonConvertor {
    pub fn transform_data(data: D2<Bson>) -> Bson {
        Bson::Array(data.into_iter().map(Bson::Array).collect())
    }
}

impl XlConsumer<XlBsonConvertor> for XlBson {
    type UnitOut = Bson;
    type FinalOut = Bson;

    fn transform(cell: XlCell) -> Self::UnitOut {
        match cell.value {
            ExcelValue::Bool(v) => Bson::Boolean(v),
            ExcelValue::Number(v) => Bson::Double(v),
            ExcelValue::String(v) => Bson::String(v.into_owned()),
            ExcelValue::Date(v) => {
                let d = v.to_string();
                Bson::String(d)
            }
            ExcelValue::Time(v) => {
                let d = v.num_seconds_from_midnight();
                Bson::UInt32(d)
            }
            ExcelValue::DateTime(v) => {
                let dt = chrono::DateTime::<chrono::Utc>::from_utc(v, chrono::Utc);
                Bson::DateTime(DateTime::from_chrono(dt))
            }
            ExcelValue::None => Bson::Null,
            ExcelValue::Error(v) => Bson::String(v),
        }
    }
}

#[cfg(test)]
mod xl_bson_tests {
    use std::fs::File;

    use super::*;
    use crate::{XlSource, XlWorkbook};

    const XL_PATH: &str = "../mock/test.xlsx";

    #[test]
    fn convert_test() {
        let source: XlWorkbook<File> = XlSource::Path(XL_PATH.to_owned()).try_into().unwrap();

        let mut xle = XlBsonExecutor::new_with_source(source);

        let foo = xle.consume_fn(
            Some(30),
            "data",
            |d| Ok(XlBsonConvertor::transform_data(d)),
            |d| {
                println!("{:?}\n\n", d);
                Ok(())
            },
        );

        println!("{:?}", foo);
    }
}
