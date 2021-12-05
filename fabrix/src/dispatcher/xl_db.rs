//!  xl -> db
//!
//! - XlDbConvertor
//! - XlDbConsumer
//! - XlDb

use std::sync::Arc;

use tokio::sync::Mutex;

use crate::sql::{SqlEngine, SqlExecutor};
use crate::{sql, value, xl, D2Value, DataFrame, FabrixResult, Value};

pub type XlDbExecutor = xl::XlExecutor<SqlExecutor, XlDbConvertor>;

pub struct XlDbConvertor {
    pub fields: Option<Vec<String>>,
}

impl XlDbConvertor {
    pub fn new() -> Self {
        Self { fields: None }
    }

    pub fn clean_stats(&mut self) {
        self.fields = None;
    }

    fn set_row_wised_fields(&mut self, data: &mut D2Value) {
        // if no fields are defined, use the first row as the fields
        if let None = &self.fields {
            let mut fld = Vec::new();
            data.iter_mut().for_each(|row| {
                let v = row.swap_remove(0);
                fld.push(v.to_string());
            });
            // the 1st cell is the index name
            fld.remove(0);
            // set the fields
            self.fields = Some(fld);
        };
    }

    pub fn convert_row_wised_with_index(&mut self, mut data: D2Value) -> FabrixResult<DataFrame> {
        self.set_row_wised_fields(&mut data);

        let mut df = DataFrame::from_row_values(data, Some(0))?;
        df.set_column_names(self.fields.as_ref().unwrap())?;
        Ok(df)
    }

    pub fn convert_row_wised_no_index(&mut self, mut data: D2Value) -> FabrixResult<DataFrame> {
        self.set_row_wised_fields(&mut data);

        let mut df = DataFrame::from_row_values(data, None)?;
        df.set_column_names(self.fields.as_ref().unwrap())?;
        Ok(df)
    }

    pub fn convert_col_wised_with_index(&mut self, mut data: D2Value) -> FabrixResult<DataFrame> {
        todo!()
    }

    pub fn convert_col_wised_no_index(&mut self, mut data: D2Value) -> FabrixResult<DataFrame> {
        todo!()
    }
}

pub struct XlToDbConsumer {
    pub executor: SqlExecutor,
    pub consume_count: usize,
}

impl XlToDbConsumer {
    pub async fn new(conn: &str) -> FabrixResult<Self> {
        let mut executor = SqlExecutor::from_str(conn);
        executor.connect().await?;
        Ok(Self {
            executor,
            consume_count: 0,
        })
    }

    pub fn clean_stats(&mut self) {
        self.consume_count = 0;
    }

    pub async fn save(&mut self, table_name: &str, data: DataFrame) -> FabrixResult<()> {
        let exc = match self.consume_count {
            0 => {
                self.executor
                    .save(table_name, data, &sql::sql_adt::SaveStrategy::FailIfExists)
                    .await
            }
            _ => {
                self.executor
                    .save(table_name, data, &sql::sql_adt::SaveStrategy::Append)
                    .await
            }
        };

        match exc {
            Ok(_) => {
                self.consume_count += 1;
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }
}

pub struct XlDb {
    pub convertor: XlDbConvertor,
    pub consumer: Arc<Mutex<XlToDbConsumer>>,
}

impl XlDb {
    pub async fn new(conn: &str) -> FabrixResult<Self> {
        let convertor = XlDbConvertor::new();
        let consumer = Arc::new(Mutex::new(XlToDbConsumer::new(conn).await?));
        Ok(Self {
            convertor,
            consumer,
        })
    }

    pub async fn clean_stats(&mut self) {
        self.convertor.clean_stats();
        self.consumer.lock().await.clean_stats();
    }
}

impl xl::XlConsumer<XlDbConvertor> for SqlExecutor {
    type UnitOut = Value;
    type FinalOut = DataFrame;

    fn transform(cell: xl::Cell) -> Self::UnitOut {
        match cell.value {
            xl::ExcelValue::Bool(v) => value!(v),
            xl::ExcelValue::Number(v) => value!(v),
            xl::ExcelValue::String(v) => value!(v.into_owned()),
            xl::ExcelValue::Date(v) => value!(v),
            xl::ExcelValue::Time(v) => value!(v),
            xl::ExcelValue::DateTime(v) => value!(v),
            xl::ExcelValue::None => Value::Null,
            xl::ExcelValue::Error(v) => value!(v),
        }
    }
}

/// This test case shows a normal process of implement Xl2Db for custom biz logic.
#[cfg(test)]
mod test_xl_reader {

    use super::*;
    use crate::sources::xl::XlSource;
    use crate::sql::SqlEngine;

    const CONN3: &'static str = "sqlite://dev.sqlite";

    #[tokio::test]
    async fn test_xl2db() {
        // Xl read from a path
        let source = XlSource::Path("../mock/test.xlsx");

        // converter & consumer instance
        let mut convertor = XlDbConvertor::new();
        let mut consumer = XlToDbConsumer::new(CONN3).await.unwrap();

        // XlExecutor instance
        let mut xle = XlDbExecutor::new_with_source(source).unwrap();

        // xl sheet iterator
        let iter = xle.iter_sheet(Some(50), "test_table").unwrap();

        // iterate through the sheet, and save the data to db
        for (i, row) in iter.enumerate() {
            let df = convertor.convert_row_wised_no_index(row).unwrap();
            if let Ok(_) = consumer.save("test_table", df).await {
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
        let res = consumer.executor.select(&select).await.unwrap();

        println!("{:?}", res);
    }

    #[tokio::test]
    async fn test_xl2db_2() {
        let source = XlSource::Path("../mock/test.xlsx");

        let mut convertor = XlDbConvertor::new();
        let consumer = XlToDbConsumer::new(CONN3).await.unwrap();
        let am_consumer = Arc::new(Mutex::new(consumer));

        let mut xle = XlDbExecutor::new_with_source(source).unwrap();

        let foo = xle
            .async_consume_fn_mut(
                Some(40),
                "test_table",
                |d| convertor.convert_col_wised_no_index(d),
                |d| {
                    Box::pin(async {
                        let am = Arc::clone(&am_consumer);
                        let mut lk = am.lock().await;
                        lk.save("test_table", d).await.map(|_| ())
                    })
                },
            )
            .await;

        println!("{:?}", foo);
        println!("{:?}", am_consumer.lock().await.consume_count);
    }

    #[tokio::test]
    async fn test_xl2db_3() {
        let source = XlSource::Path("../mock/test.xlsx");

        let mut xl2db = XlDb::new(CONN3).await.unwrap();

        let mut xle = XlDbExecutor::new_with_source(source).unwrap();

        let foo = xle
            .async_consume_fn_mut(
                Some(40),
                "test_table",
                |d| xl2db.convertor.convert_col_wised_no_index(d),
                |d| {
                    Box::pin(async {
                        xl2db
                            .consumer
                            .lock()
                            .await
                            .save("test_table", d)
                            .await
                            .map(|_| ())
                    })
                },
            )
            .await;

        println!("{:?}", foo);
    }
}
