//!  xl -> db

use crate::xl::{Cell, ExcelValue, XlConsumer};
use crate::FabrixResult;
use crate::{sql::SqlExecutor, value, D2Value, DataFrame, Value};

// TODO: row-wised adaptor needs caching column names (assuming the 1st row is the header)
// we need an another struct other than `XlToDb`
// TODO: `consume_fn` however, needs to cache all the data (assuming the 1st column is the header)
/// xl -> database
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

    // test fn
    pub async fn consume(&mut self, data: DataFrame) -> FabrixResult<()> {
        todo!()
    }
}

/// This test case shows a normal process of implement Xl2Db for custom biz logic.
#[cfg(test)]
mod test_xl_reader {

    use super::*;
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
        use crate::sources::xl::{XlExecutor, XlSource};

        // Xl read from a path
        let source = XlSource::Path("../mock/test.xlsx");

        // consumer instance
        let consumer = SqlExecutor::from_str(CONN3);
        // consumer.connect().await.unwrap();

        // XlExecutor instance
        let mut xle = XlExecutor::new_with_source(consumer, source).unwrap();
        // Connect to the database
        xle.consumer_mut().connect().await.unwrap();

        let mut xl2db = XlToDb {};
        let mut pc = PowerConsumer::new(CONN3).await.unwrap();

        let iter = xle.iter_sheet(Some(50), "test_table").unwrap();

        for (i, row) in iter.enumerate() {
            let df = xl2db.convert_row_wised_no_index(row).unwrap();
            let res = pc.save("test_table", df).await;
            println!("{:?} : {:?}", i, res);
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
        let res = xle.consumer().select(&select).await.unwrap();

        println!("{:?}", res);
    }
}
