//!  xl -> db

use crate::xl::{Cell, ExcelValue, XlConsumer};
use crate::FabrixResult;
use crate::{sql::SqlExecutor, value, D2Value, DataFrame, Value};

// TODO: row-wised adaptor needs caching column names (assuming the 1st row is the header)
// we need an another struct other than `XlToDb`
// TODO: `consume_fn` however, needs to cache all the data (assuming the 1st column is the header)
/// xl -> database
pub struct XlToDb;

impl XlConsumer<XlToDb> for SqlExecutor {
    type UnitOut = Value;
    type FinalOut = DataFrame;

    fn transform(cell: Cell) -> Self::UnitOut {
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

impl XlToDb {
    pub fn convert_row_wised_with_index(data: D2Value) -> FabrixResult<DataFrame> {
        todo!()
    }

    pub fn convert_row_wised_no_index(data: D2Value) -> FabrixResult<DataFrame> {
        todo!()
    }

    pub fn convert_col_wised_with_index(data: D2Value) -> FabrixResult<DataFrame> {
        todo!()
    }

    pub fn convert_col_wised_no_index(data: D2Value) -> FabrixResult<DataFrame> {
        todo!()
    }
}

/// This test case shows a normal process of implement Xl2Db for custom biz logic.
#[cfg(test)]
mod test_xl_reader {

    use super::*;
    use crate::sql::SqlEngine;
    use crate::{sql, xl, D2Value, DataFrame, FabrixResult};

    const CONN3: &'static str = "sqlite://dev.sqlite";

    async fn async_consume_fn(fo: DataFrame) -> FabrixResult<()> {
        println!("{}\n\n", fo);
        Ok(())
    }

    #[tokio::test]
    async fn test_xl2db() {
        use crate::sources::xl::{XlExecutor, XlSource};

        // Xl read from a path
        let source = XlSource::Path("../mock/test.xlsx");

        // consumer instance
        let consumer = SqlExecutor::from_str(CONN3);
        // consumer.connect().await.unwrap();

        // XlExecutor instance
        let mut xle = XlExecutor::new_with_source(consumer, source).unwrap();
        // Connect to the database
        xle.consumer().connect().await.unwrap();

        // read sheet, and save converted data into memory
        let foo = xle
            .async_consume(
                Some(50),
                "data",
                XlToDb::convert_row_wised_no_index as xl::ConvertFn<D2Value, DataFrame>,
                (|fo| Box::pin(async_consume_fn(fo))) as xl::AsyncConsumeFn<DataFrame>,
            )
            .await;

        println!("{:?}", foo);

        // sql selection
        let select = sql::sql_adt::Select {
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
        let res = xle.consumer().select(&select).await.unwrap();

        println!("{:?}", res);
    }
}
