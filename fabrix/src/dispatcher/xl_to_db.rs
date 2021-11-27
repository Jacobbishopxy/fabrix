//!  xl -> db

use super::DispatcherDB;
use crate::xl::{Cell, ExcelValue, XlConsumer};
use crate::{sql::SqlExecutor, value, DataFrame, Value};

impl XlConsumer<DispatcherDB> for SqlExecutor {
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

/// This test case shows a normal process of implement Xl2Db for custom biz logic.
#[cfg(test)]
mod test_xl_reader {

    use super::*;
    use crate::{sql::sql_adt, sql::SqlEngine, xl, DataFrame, FabrixResult, D2};

    const CONN3: &'static str = "sqlite://dev.sqlite";

    fn convert_fn(data: D2) -> FabrixResult<DataFrame> {
        todo!()
    }

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
        let mut consumer = SqlExecutor::from_str(CONN3);
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
                &(convert_fn as xl::ConvertFn<D2, DataFrame>),
                &((|fo| Box::pin(async_consume_fn(fo))) as xl::AsyncConsumeFn<DataFrame>),
            )
            .await;

        println!("{:?}", foo);

        // TODO: redo sql_adt
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
        let res = xle.consumer().select(&select).await.unwrap();

        println!("{:?}", res);
    }
}
