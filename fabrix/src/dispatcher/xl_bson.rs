//! xl -> json
//!
//!

use bson2::{Bson, DateTime};
use chrono::Timelike;

use crate::{xl, D2};

pub type XlBsonExecutor = xl::XlExecutor<XlBson, XlBsonConvertor>;

pub struct XlBson;

pub struct XlBsonConvertor;

impl XlBsonConvertor {
    pub fn transform_data(data: D2<Bson>) -> Bson {
        Bson::Array(data.into_iter().map(Bson::Array).collect())
    }
}

impl xl::XlConsumer<XlBsonConvertor> for XlBson {
    type UnitOut = Bson;
    type FinalOut = Bson;

    fn transform(cell: xl::Cell) -> Self::UnitOut {
        match cell.value {
            xl::ExcelValue::Bool(v) => Bson::Boolean(v),
            xl::ExcelValue::Number(v) => Bson::Double(v),
            xl::ExcelValue::String(v) => Bson::String(v.into_owned()),
            xl::ExcelValue::Date(v) => {
                let d = v.to_string();
                Bson::String(d)
            }
            xl::ExcelValue::Time(v) => {
                let d = v.num_seconds_from_midnight();
                Bson::UInt32(d)
            }
            xl::ExcelValue::DateTime(v) => {
                let dt = chrono::DateTime::<chrono::Utc>::from_utc(v, chrono::Utc);
                Bson::DateTime(DateTime::from_chrono(dt))
            }
            xl::ExcelValue::None => Bson::Null,
            xl::ExcelValue::Error(v) => Bson::String(v),
        }
    }
}

#[cfg(test)]
mod xl_bson_tests {
    use super::*;
    use crate::sources::xl::XlSource;

    #[test]
    fn convert_test() {
        let source = XlSource::Path("../mock/test.xlsx");

        let mut xle = XlBsonExecutor::new_with_source(source).unwrap();

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
