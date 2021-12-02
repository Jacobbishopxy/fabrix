//!  xl -> db

use crate::xl::{Cell, ExcelValue, XlConsumer};
use crate::FabrixResult;
use crate::{sql::SqlExecutor, value, xl, D2Value, DataFrame, Value};

pub type XlDb = xl::XlExecutor<SqlExecutor, XlToDb>;

pub struct XlToDb {}

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

// TODO: &mut self for these methods
impl XlToDb {
    pub fn convert_row_wised_with_index(&mut self, data: D2Value) -> FabrixResult<DataFrame> {
        todo!()
    }

    pub fn convert_row_wised_no_index(&mut self, data: D2Value) -> FabrixResult<DataFrame> {
        todo!()
    }

    pub fn convert_col_wised_with_index(&mut self, data: D2Value) -> FabrixResult<DataFrame> {
        todo!()
    }

    pub fn convert_col_wised_no_index(&mut self, data: D2Value) -> FabrixResult<DataFrame> {
        todo!()
    }
}

/// This test case shows a normal process of implement Xl2Db for custom biz logic.
#[cfg(test)]
mod test_xl_reader {

    use std::sync::Arc;
    use tokio::sync::Mutex;

    use super::*;
    use crate::sources::xl::XlSource;
    use crate::sql::SqlEngine;
    use crate::{sql, DataFrame, FabrixResult};

    const CONN3: &'static str = "sqlite://dev.sqlite";

    struct PowerConsumer {
        consumer: SqlExecutor,
        count: usize,
    }

    impl PowerConsumer {
        async fn new(conn: &str) -> FabrixResult<Self> {
            let mut consumer = SqlExecutor::from_str(conn);
            consumer.connect().await?;
            Ok(Self { consumer, count: 0 })
        }

        async fn save(&mut self, table_name: &str, data: DataFrame) -> FabrixResult<()> {
            let exc = match self.count {
                0 => {
                    self.consumer
                        .save(table_name, data, &sql::sql_adt::SaveStrategy::FailIfExists)
                }
                _ => self
                    .consumer
                    .save(table_name, data, &sql::sql_adt::SaveStrategy::Append),
            }
            .await;

            match exc {
                Ok(_) => {
                    self.count += 1;
                    Ok(())
                }
                Err(e) => Err(e.into()),
            }
        }
    }

    #[tokio::test]
    async fn test_xl2db() {
        // Xl read from a path
        let source = XlSource::Path("../mock/test.xlsx");

        // consumer instance
        let mut consumer = SqlExecutor::from_str(CONN3);
        consumer.connect().await.unwrap();

        // XlExecutor instance
        let mut xle = XlDb::new_with_source(source).unwrap();
        let mut xl2db = XlToDb {};

        let mut pc = PowerConsumer::new(CONN3).await.unwrap();

        let iter = xle.iter_sheet(Some(50), "test_table").unwrap();

        for (i, row) in iter.enumerate() {
            let df = xl2db.convert_row_wised_no_index(row).unwrap();
            if let Ok(_) = pc.save("test_table", df).await {
                println!("{:?}: success", i);
            } else {
                println!("{:?}: failed", i);
                break;
            }
        }

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
        let res = consumer.select(&select).await.unwrap();

        println!("{:?}", res);
    }

    #[tokio::test]
    async fn test_xl2db_2() {
        let source = XlSource::Path("../mock/test.xlsx");

        let mut consumer = SqlExecutor::from_str(CONN3);
        consumer.connect().await.unwrap();

        let mut xle = XlDb::new_with_source(source).unwrap();

        let mut xl2db = XlToDb {};
        let pc = Arc::new(Mutex::new(PowerConsumer::new(CONN3).await.unwrap()));

        let foo = xle
            .async_consume_fn_mut(
                Some(40),
                "test_table",
                |d| xl2db.convert_col_wised_no_index(d),
                |d| {
                    Box::pin(async {
                        let am = Arc::clone(&pc);
                        let mut lk = am.lock().await;
                        lk.save("test_table", d).await
                    })
                },
            )
            .await;

        println!("{:?}", foo);
        println!("{:?}", pc.lock().await.count);
    }
}
