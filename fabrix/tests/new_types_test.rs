//! New types test case: &'static str -> db types
//!
//!

use std::{marker::PhantomData, str::FromStr};

use fabrix::{DbError, DbResult, Value, ValueType};
use sqlx::{mysql::MySqlRow, postgres::PgRow, sqlite::SqliteRow, Row};

#[warn(dead_code)]
enum SqlRow {
    Mysql(MySqlRow),
    Pg(PgRow),
    Sqlite(SqliteRow),
}

trait SqlTypeTagMarker {
    fn to_str(&self) -> &str;

    fn to_dtype(&self) -> ValueType;

    fn extract_value(&self, sql_row: &SqlRow, idx: usize) -> DbResult<Value>;

    fn extract_optional_value(&self, sql_row: &SqlRow, idx: usize) -> DbResult<Option<Value>>;
}

struct SqlTypeTag<T>(&'static str, PhantomData<T>)
where
    T: Into<Value>;

impl<T> SqlTypeTag<T>
where
    T: Into<Value>,
{
    fn new(name: &'static str) -> Self {
        SqlTypeTag(name, PhantomData)
    }
}

// sql type binds to `bool`
impl SqlTypeTagMarker for SqlTypeTag<bool> {
    fn to_str(&self) -> &str {
        self.0
    }

    fn to_dtype(&self) -> ValueType {
        ValueType::Bool
    }

    fn extract_value(&self, sql_row: &SqlRow, idx: usize) -> DbResult<Value> {
        match sql_row {
            SqlRow::Mysql(row) => {
                let v: Option<bool> = row.try_get(idx)?;
                match v {
                    Some(v) => Ok(Value::Bool(v)),
                    None => Ok(Value::Null),
                }
            }
            SqlRow::Pg(row) => {
                let v: Option<bool> = row.try_get(idx)?;
                match v {
                    Some(v) => Ok(Value::Bool(v)),
                    None => Ok(Value::Null),
                }
            }
            SqlRow::Sqlite(row) => {
                let v: Option<bool> = row.try_get(idx)?;
                match v {
                    Some(v) => Ok(Value::Bool(v)),
                    None => Ok(Value::Null),
                }
            }
        }
    }

    fn extract_optional_value(&self, sql_row: &SqlRow, idx: usize) -> DbResult<Option<Value>> {
        match sql_row {
            SqlRow::Mysql(row) => {
                let v: Option<bool> = row.try_get(idx)?;
                Ok(v.map(|v| v.into()))
            }
            SqlRow::Pg(row) => {
                let v: Option<bool> = row.try_get(idx)?;
                Ok(v.map(|v| v.into()))
            }
            SqlRow::Sqlite(row) => {
                let v: Option<bool> = row.try_get(idx)?;
                Ok(v.map(|v| v.into()))
            }
        }
    }
}

// TODO: irrational
impl FromStr for SqlTypeTag<bool> {
    type Err = DbError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "bool" => Ok(SqlTypeTag::new("bool")),
            _ => Err(DbError::new_common_error("invalid type")),
        }
    }
}

#[test]
fn test_from_str() {
    let s = "bool";

    let v: SqlTypeTag<bool> = s.parse().unwrap();

    assert_eq!(v.to_dtype(), ValueType::Bool);
}
