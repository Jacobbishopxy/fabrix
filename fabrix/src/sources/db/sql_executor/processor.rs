//! Sql row processor

use itertools::Itertools;
use sqlx::{Column, Row as SRow};

use super::types::{
    value_type_try_into_marker, OptMarker, SqlRow, MYSQL_TMAP, PG_TMAP, SQLITE_TMAP,
};
use crate::{DbResult, Row, SqlBuilder, Value, ValueType, D1};

/// SqlRowProcessor is the core struct for processing different types of SqlRow
pub(crate) struct SqlRowProcessor {
    cache_markers: Option<Vec<OptMarker>>,
}

impl SqlRowProcessor {
    pub fn new() -> Self {
        SqlRowProcessor {
            cache_markers: None,
        }
    }

    pub fn new_with_cache(driver: &SqlBuilder, value_types: &[ValueType]) -> Self {
        SqlRowProcessor {
            cache_markers: Some(value_type_try_into_marker(driver, value_types)),
        }
    }

    /// since each row has the same type order, saving them in cache for future use
    fn caching(&mut self, sql_row: &SqlRow) {
        if let None = self.cache_markers {
            match sql_row {
                SqlRow::Mysql(row) => {
                    let ct = row
                        .columns()
                        .iter()
                        .map(|c| {
                            let t = c.type_info().to_string();
                            MYSQL_TMAP.get(&t[..])
                        })
                        .collect_vec();
                    self.cache_markers = Some(ct);
                }
                SqlRow::Pg(row) => {
                    let ct = row
                        .columns()
                        .iter()
                        .map(|c| {
                            let t = c.type_info().to_string();
                            PG_TMAP.get(&t[..])
                        })
                        .collect_vec();
                    self.cache_markers = Some(ct);
                }
                SqlRow::Sqlite(row) => {
                    let ct = row
                        .columns()
                        .iter()
                        .map(|c| {
                            let t = c.type_info().to_string();
                            SQLITE_TMAP.get(&t[..])
                        })
                        .collect_vec();
                    self.cache_markers = Some(ct);
                }
            }
        }
    }

    /// customize processing fn, without using cache
    pub fn process_by_fn<R>(&self, sql_row: R, f: &dyn Fn(SqlRow) -> DbResult<D1>) -> DbResult<D1>
    where
        R: Into<SqlRow>,
    {
        f(sql_row.into())
    }

    /// converting a sql row into a vector of `Value`
    pub fn process<T>(&mut self, sql_row: T) -> DbResult<Vec<Value>>
    where
        T: Into<SqlRow>,
    {
        let sql_row: SqlRow = sql_row.into();
        self.caching(&sql_row);
        let mut res = Vec::with_capacity(sql_row.len());

        for (idx, c) in self.cache_markers.as_ref().unwrap().iter().enumerate() {
            match c {
                Some(m) => {
                    res.push(m.extract_value(&sql_row, idx)?);
                }
                None => {
                    res.push(Value::Null);
                }
            }
        }

        Ok(res)
    }

    /// converting a sql row into `Row`
    /// WARNING: this method is assumed primary key is the first selected column in Sql query string
    pub fn process_to_row<T>(&mut self, sql_row: T) -> DbResult<Row>
    where
        T: Into<SqlRow>,
    {
        let sql_row: SqlRow = sql_row.into();
        self.caching(&sql_row);
        let mut res = Vec::with_capacity(sql_row.len() - 1);
        let mut itr = self.cache_markers.as_ref().unwrap().iter();
        let index = itr.next().unwrap().unwrap().extract_value(&sql_row, 0)?;

        for (idx, c) in itr.enumerate() {
            match c {
                Some(m) => {
                    // first idx is primary key, skip it
                    res.push(m.extract_value(&sql_row, idx + 1)?);
                }
                None => {
                    res.push(Value::Null);
                }
            }
        }

        Ok(Row::new(index, res))
    }
}

#[cfg(test)]
mod test_processor {
    use super::*;
    use crate::value;

    const CONN1: &'static str = "mysql://root:secret@localhost:3306/dev";
    // const CONN2: &'static str = "postgres://root:secret@localhost:5432/dev";
    // const CONN3: &'static str = "sqlite:/home/jacob/dev.sqlite";

    // processor with cache
    #[tokio::test]
    async fn test_row_process_cache() {
        let pool = sqlx::MySqlPool::connect(CONN1).await.unwrap();

        let que = "select names, dt from dev";

        let vt = vec![ValueType::String, ValueType::DateTime];

        let mut processor = SqlRowProcessor::new_with_cache(&SqlBuilder::Mysql, &vt);

        let res = sqlx::query(&que)
            .try_map(|row: sqlx::mysql::MySqlRow| {
                processor
                    .process(row)
                    .map_err(|e| e.turn_into_sqlx_decode_error())
            })
            .fetch_all(&pool)
            .await
            .unwrap();

        println!("{:?}", res);
    }

    // processor with new processing fn
    #[tokio::test]
    async fn test_row_process_fn() {
        let pool = sqlx::MySqlPool::connect(CONN1).await.unwrap();

        let que = "select names, dt from dev";

        let processor = SqlRowProcessor::new();

        // apply a new function instead of using default `process` method
        let box_f = |row| -> DbResult<D1> {
            let id = MYSQL_TMAP
                .get("VARCHAR")
                .unwrap()
                .extract_value(&row, 0)
                .unwrap();
            let name = MYSQL_TMAP
                .get("DATETIME")
                .unwrap()
                .extract_value(&row, 1)
                .unwrap();

            Ok(vec![value!(id), value!(name)])
        };

        let res = sqlx::query(&que)
            .try_map(|row: sqlx::mysql::MySqlRow| {
                processor
                    .process_by_fn(row, &box_f)
                    .map_err(|e| e.turn_into_sqlx_decode_error())
            })
            .fetch_all(&pool)
            .await
            .unwrap();

        println!("{:?}", res);
    }
}
