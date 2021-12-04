//!  xl -> db
//!
//!

use std::sync::Arc;

use tokio::sync::Mutex;

use crate::sql::{SqlEngine, SqlExecutor};
use crate::{sql, value, xl, D2Value, DataFrame, FabrixResult, Value};

pub type XlDb = xl::XlExecutor<SqlExecutor, XlToDb>;

pub struct XlToDb {
    pub fields: Option<Vec<String>>,
}

impl XlToDb {
    pub fn new() -> Self {
        Self { fields: None }
    }

    pub fn clean_stats(&mut self) {
        self.fields = None;
    }

    pub fn convert_row_wised_with_index(&mut self, mut data: D2Value) -> FabrixResult<DataFrame> {
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

        let mut df = DataFrame::from_row_values(data, Some(0))?;
        df.set_column_names(self.fields.as_ref().unwrap())?;
        Ok(df)
    }

    pub fn convert_row_wised_no_index(&mut self, mut data: D2Value) -> FabrixResult<DataFrame> {
        // if no fields are defined, use the first row as the fields
        if let None = &self.fields {
            let mut fld = Vec::new();
            data.iter_mut().for_each(|row| {
                let v = row.swap_remove(0);
                fld.push(v.to_string());
            });
            self.fields = Some(fld);
        };

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

impl xl::XlConsumer<XlToDb> for SqlExecutor {
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

pub struct XlToDbConsumer {
    pub executor: Arc<Mutex<SqlExecutor>>,
    pub consume_count: usize,
}

impl XlToDbConsumer {
    pub async fn new(conn: &str) -> FabrixResult<Self> {
        let mut executor = SqlExecutor::from_str(conn);
        executor.connect().await?;
        Ok(Self {
            executor: Arc::new(Mutex::new(executor)),
            consume_count: 0,
        })
    }

    pub fn executor(&self) -> Arc<Mutex<SqlExecutor>> {
        Arc::clone(&self.executor)
    }

    pub fn clean_stats(&mut self) {
        self.consume_count = 0;
    }

    // TODO: this method should be called by `&self.executor`
    pub async fn save(&mut self, table_name: &str, data: DataFrame) -> FabrixResult<()> {
        let exc = match self.consume_count {
            0 => {
                let exc = self.executor.try_lock()?;
                exc.save(table_name, data, &sql::sql_adt::SaveStrategy::FailIfExists)
                    .await
            }
            _ => {
                let exc = self.executor.try_lock()?;
                exc.save(table_name, data, &sql::sql_adt::SaveStrategy::Append)
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
        let mut xl2db = XlToDb::new();
        let mut consumer = XlToDbConsumer::new(CONN3).await.unwrap();

        // XlExecutor instance
        let mut xle = XlDb::new_with_source(source).unwrap();

        let iter = xle.iter_sheet(Some(50), "test_table").unwrap();

        for (i, row) in iter.enumerate() {
            let df = xl2db.convert_row_wised_no_index(row).unwrap();
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
        let res = consumer
            .executor()
            .lock()
            .await
            .select(&select)
            .await
            .unwrap();

        println!("{:?}", res);
    }

    #[tokio::test]
    async fn test_xl2db_2() {
        let source = XlSource::Path("../mock/test.xlsx");

        let mut xl2db = XlToDb::new();
        let consumer = XlToDbConsumer::new(CONN3).await.unwrap();

        let mut xle = XlDb::new_with_source(source).unwrap();

        let foo = xle
            .async_consume_fn_mut(
                Some(40),
                "test_table",
                |d| xl2db.convert_col_wised_no_index(d),
                |d| {
                    Box::pin(async {
                        let exc = consumer.executor();
                        let lk = exc.lock().await;
                        // TODO: cannot use predefined `save` method in `consumer`, since consumer itself is not a `Arc<Mutex<T>>`
                        match lk
                            .save("test_table", d, &sql::sql_adt::SaveStrategy::Append)
                            .await
                        {
                            Ok(_) => todo!(),
                            Err(_) => todo!(),
                        }
                    })
                },
            )
            .await;

        println!("{:?}", foo);
        println!("{:?}", consumer.consume_count);
    }
}
