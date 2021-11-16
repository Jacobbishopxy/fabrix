//! Sql types

use std::{collections::HashMap, marker::PhantomData};

use itertools::Itertools;
use sqlx::{mysql::MySqlRow, postgres::PgRow, sqlite::SqliteRow, Row as SRow};

use super::{impl_sql_type_tag_marker, tmap_pair};
use crate::{Date, DateTime, Decimal, FabrixResult, SqlBuilder, Time, Uuid, Value, ValueType};

/// type alias
pub(crate) type OptMarker = Option<&'static Box<dyn SqlTypeTagMarker>>;

/// Type of Sql row
pub(crate) enum SqlRow {
    Mysql(MySqlRow),
    Pg(PgRow),
    Sqlite(SqliteRow),
}

impl SqlRow {
    pub(crate) fn len(&self) -> usize {
        match self {
            SqlRow::Mysql(r) => r.len(),
            SqlRow::Pg(r) => r.len(),
            SqlRow::Sqlite(r) => r.len(),
        }
    }
}

impl From<MySqlRow> for SqlRow {
    fn from(r: MySqlRow) -> Self {
        Self::Mysql(r)
    }
}

impl From<PgRow> for SqlRow {
    fn from(r: PgRow) -> Self {
        Self::Pg(r)
    }
}

impl From<SqliteRow> for SqlRow {
    fn from(r: SqliteRow) -> Self {
        Self::Sqlite(r)
    }
}

/// Sql type tag is used to tag static str to Rust primitive type and user customized type
#[derive(Debug)]
pub(crate) struct SqlTypeTag<T>(&'static str, PhantomData<T>)
where
    T: Into<Value>;

impl<T> SqlTypeTag<T>
where
    T: Into<Value>,
{
    pub(crate) fn new(st: &'static str) -> Self {
        SqlTypeTag(st, PhantomData)
    }
}

/// Behavior of SqlTypeTag, used to create trait objects and saving them to the global static HashMap
pub(crate) trait SqlTypeTagMarker: Send + Sync {
    /// to &str
    fn to_str(&self) -> &str;

    /// to datatype
    fn to_dtype(&self) -> ValueType;

    /// extract Value from sql row
    fn extract_value(&self, sql_row: &SqlRow, idx: usize) -> FabrixResult<Value>;
}

/// tmap value type
pub(crate) type SqlTypeTagKind = Box<dyn SqlTypeTagMarker>;

impl PartialEq<str> for SqlTypeTagKind {
    fn eq(&self, other: &str) -> bool {
        self.to_str() == other
    }
}

impl PartialEq<SqlTypeTagKind> for str {
    fn eq(&self, other: &SqlTypeTagKind) -> bool {
        self == other.to_str()
    }
}

const MISMATCHED_SQL_ROW: &'static str = "mismatched sql row";

impl_sql_type_tag_marker!(bool, Bool; [Mysql, Pg, Sqlite]);
impl_sql_type_tag_marker!(u8, U8; [Mysql], MISMATCHED_SQL_ROW);
impl_sql_type_tag_marker!(u16, U16; [Mysql], MISMATCHED_SQL_ROW);
impl_sql_type_tag_marker!(u32, U32; [Mysql], MISMATCHED_SQL_ROW);
impl_sql_type_tag_marker!(u64, U64; [Mysql], MISMATCHED_SQL_ROW);
impl_sql_type_tag_marker!(i8, I8; [Mysql, Pg], MISMATCHED_SQL_ROW);
impl_sql_type_tag_marker!(i16, I16; [Mysql, Pg], MISMATCHED_SQL_ROW);
impl_sql_type_tag_marker!(i32, I32; [Mysql, Pg, Sqlite]);
impl_sql_type_tag_marker!(i64, I64; [Mysql, Pg, Sqlite]);
impl_sql_type_tag_marker!(f32, F32; [Mysql, Pg], MISMATCHED_SQL_ROW);
impl_sql_type_tag_marker!(f64, F64; [Mysql, Pg, Sqlite]);
impl_sql_type_tag_marker!(String, String; [Mysql, Pg, Sqlite]);
impl_sql_type_tag_marker!(Date, chrono::NaiveDate, Date; [Mysql, Pg], MISMATCHED_SQL_ROW);
impl_sql_type_tag_marker!(Time, chrono::NaiveTime, Time; [Mysql, Pg], MISMATCHED_SQL_ROW);
impl_sql_type_tag_marker!(DateTime, chrono::NaiveDateTime, DateTime; [Mysql, Pg, Sqlite]);
impl_sql_type_tag_marker!(Decimal, rust_decimal::Decimal, Decimal; [Mysql, Pg], MISMATCHED_SQL_ROW);
impl_sql_type_tag_marker!(Uuid, uuid::Uuid, Uuid; [Pg], MISMATCHED_SQL_ROW);

lazy_static::lazy_static! {
    /// static Mysql column type mapping
    pub(crate) static ref MYSQL_TMAP: HashMap<&'static str, Box<dyn SqlTypeTagMarker>> = {
        HashMap::from([
            tmap_pair!("TINYINT(1)", bool),
            tmap_pair!("BOOLEAN", bool),
            tmap_pair!("TINYINT UNSIGNED", u8),
            tmap_pair!("SMALLINT UNSIGNED", u16),
            tmap_pair!("INT UNSIGNED", u32),
            tmap_pair!("BIGINT UNSIGNED", u64),
            tmap_pair!("TINYINT", i8),
            tmap_pair!("SMALLINT", i16),
            tmap_pair!("INT", i32),
            tmap_pair!("BIGINT", i64),
            tmap_pair!("FLOAT", f32),
            tmap_pair!("DOUBLE", f64),
            tmap_pair!("VARCHAR", String),
            tmap_pair!("CHAR", String),
            tmap_pair!("TEXT", String),
            tmap_pair!("TIMESTAMP", DateTime),
            tmap_pair!("DATETIME", DateTime),
            tmap_pair!("DATE", Date),
            tmap_pair!("TIME", Time),
            tmap_pair!("DECIMAL", Decimal),
        ])
    };

    /// static Pg column type mapping
    pub(crate) static ref PG_TMAP: HashMap<&'static str, Box<dyn SqlTypeTagMarker>> = {
        HashMap::from([
            tmap_pair!("BOOL", bool),
            tmap_pair!("CHAR", i8),
            tmap_pair!("TINYINT", i8),
            tmap_pair!("SMALLINT", i16),
            tmap_pair!("SMALLSERIAL", i16),
            tmap_pair!("INT2", i16),
            tmap_pair!("INT", i32),
            tmap_pair!("SERIAL", i32),
            tmap_pair!("INT4", i32),
            tmap_pair!("BIGINT", i64),
            tmap_pair!("BIGSERIAL", i64),
            tmap_pair!("INT8", i64),
            tmap_pair!("REAL", f32),
            tmap_pair!("FLOAT4", f32),
            tmap_pair!("DOUBLE PRECISION", f64),
            tmap_pair!("FLOAT8", f64),
            tmap_pair!("VARCHAR", String),
            tmap_pair!("CHAR(N)", String),
            tmap_pair!("TEXT", String),
            tmap_pair!("NAME", String),
            tmap_pair!("TIMESTAMPTZ", DateTime),
            tmap_pair!("TIMESTAMP", DateTime),
            tmap_pair!("DATE", Date),
            tmap_pair!("TIME", Time),
            tmap_pair!("NUMERIC", Decimal),
        ])
    };

    /// static Sqlite column type mapping
    pub(crate) static ref SQLITE_TMAP: HashMap<&'static str, Box<dyn SqlTypeTagMarker>> = {
        HashMap::from([
            tmap_pair!("BOOLEAN", bool),
            tmap_pair!("INTEGER", i32),
            tmap_pair!("BIGINT", i64),
            tmap_pair!("INT8", i64),
            tmap_pair!("REAL", f64),
            tmap_pair!("VARCHAR", String),
            tmap_pair!("CHAR(N)", String),
            tmap_pair!("TEXT", String),
            tmap_pair!("DATETIME", DateTime),
        ])
    };
}

/// string -> `ValueType`
pub(crate) fn string_try_into_value_type<S>(driver: &SqlBuilder, str: S) -> Option<ValueType>
where
    S: AsRef<str>,
{
    let str = str.as_ref();
    match driver {
        SqlBuilder::Mysql => MYSQL_TMAP.get(str).map(|t| t.to_dtype()),
        SqlBuilder::Postgres => PG_TMAP.get(str).map(|t| t.to_dtype()),
        SqlBuilder::Sqlite => SQLITE_TMAP.get(str).map(|t| t.to_dtype()),
    }
}

/// value_type -> mysql marker
fn value_type_try_into_mysql_marker(vt: &ValueType) -> Option<&'static Box<dyn SqlTypeTagMarker>> {
    match vt {
        ValueType::Bool => Some(MYSQL_TMAP.get("BOOLEAN").unwrap()),
        ValueType::U8 => Some(MYSQL_TMAP.get("TINYINT UNSIGNED").unwrap()),
        ValueType::U16 => Some(MYSQL_TMAP.get("SMALLINT UNSIGNED").unwrap()),
        ValueType::U32 => Some(MYSQL_TMAP.get("INT UNSIGNED").unwrap()),
        ValueType::U64 => Some(MYSQL_TMAP.get("BIGINT UNSIGNED").unwrap()),
        ValueType::I8 => Some(MYSQL_TMAP.get("TINYINT").unwrap()),
        ValueType::I16 => Some(MYSQL_TMAP.get("SMALLINT").unwrap()),
        ValueType::I32 => Some(MYSQL_TMAP.get("INT").unwrap()),
        ValueType::I64 => Some(MYSQL_TMAP.get("BIGINT").unwrap()),
        ValueType::F32 => Some(MYSQL_TMAP.get("FLOAT").unwrap()),
        ValueType::F64 => Some(MYSQL_TMAP.get("DOUBLE").unwrap()),
        ValueType::String => Some(MYSQL_TMAP.get("VARCHAR").unwrap()),
        ValueType::Date => Some(MYSQL_TMAP.get("DATE").unwrap()),
        ValueType::Time => Some(MYSQL_TMAP.get("TIME").unwrap()),
        ValueType::DateTime => Some(MYSQL_TMAP.get("DATETIME").unwrap()),
        ValueType::Decimal => Some(MYSQL_TMAP.get("DECIMAL").unwrap()),
        _ => None,
    }
}

/// value_type -> pg marker
fn value_type_try_into_pg_marker(vt: &ValueType) -> Option<&'static Box<dyn SqlTypeTagMarker>> {
    match vt {
        ValueType::Bool => Some(PG_TMAP.get("BOOL").unwrap()),
        ValueType::U8 => Some(PG_TMAP.get("TINYINT").unwrap()),
        ValueType::U16 => Some(PG_TMAP.get("SMALLINT").unwrap()),
        ValueType::U32 => Some(PG_TMAP.get("INT").unwrap()),
        ValueType::U64 => Some(PG_TMAP.get("BIGINT").unwrap()),
        ValueType::I8 => Some(PG_TMAP.get("TINYINT").unwrap()),
        ValueType::I16 => Some(PG_TMAP.get("SMALLINT").unwrap()),
        ValueType::I32 => Some(PG_TMAP.get("INT").unwrap()),
        ValueType::I64 => Some(PG_TMAP.get("BIGINT").unwrap()),
        ValueType::F32 => Some(PG_TMAP.get("REAL").unwrap()),
        ValueType::F64 => Some(PG_TMAP.get("DOUBLE PRECISION").unwrap()),
        ValueType::String => Some(PG_TMAP.get("VARCHAR").unwrap()),
        ValueType::Date => Some(PG_TMAP.get("DATE").unwrap()),
        ValueType::Time => Some(PG_TMAP.get("TIME").unwrap()),
        ValueType::DateTime => Some(PG_TMAP.get("TIMESTAMP").unwrap()),
        ValueType::Decimal => Some(PG_TMAP.get("NUMERIC").unwrap()),
        ValueType::Uuid => Some(PG_TMAP.get("UUID").unwrap()),
        _ => None,
    }
}

/// value_type -> sqlite marker
fn value_type_try_into_sqlite_marker(vt: &ValueType) -> Option<&'static Box<dyn SqlTypeTagMarker>> {
    match vt {
        ValueType::Bool => Some(SQLITE_TMAP.get("BOOLEAN").unwrap()),
        ValueType::U8 => Some(SQLITE_TMAP.get("INTEGER").unwrap()),
        ValueType::U16 => Some(SQLITE_TMAP.get("INTEGER").unwrap()),
        ValueType::U32 => Some(SQLITE_TMAP.get("INTEGER").unwrap()),
        ValueType::U64 => Some(SQLITE_TMAP.get("BIGINT").unwrap()),
        ValueType::I8 => Some(SQLITE_TMAP.get("INTEGER").unwrap()),
        ValueType::I16 => Some(SQLITE_TMAP.get("INTEGER").unwrap()),
        ValueType::I32 => Some(SQLITE_TMAP.get("INTEGER").unwrap()),
        ValueType::I64 => Some(SQLITE_TMAP.get("BIGINT").unwrap()),
        ValueType::F32 => Some(SQLITE_TMAP.get("REAL").unwrap()),
        ValueType::F64 => Some(SQLITE_TMAP.get("REAL").unwrap()),
        ValueType::String => Some(SQLITE_TMAP.get("VARCHAR").unwrap()),
        ValueType::DateTime => Some(SQLITE_TMAP.get("DATETIME").unwrap()),
        _ => None,
    }
}

/// value_type -> marker
pub(crate) fn value_type_try_into_marker(
    driver: &SqlBuilder,
    value_types: &[ValueType],
) -> Vec<OptMarker> {
    match driver {
        SqlBuilder::Mysql => value_types
            .iter()
            .map(|vt| value_type_try_into_mysql_marker(vt))
            .collect_vec(),
        SqlBuilder::Postgres => value_types
            .iter()
            .map(|vt| value_type_try_into_pg_marker(vt))
            .collect_vec(),
        SqlBuilder::Sqlite => value_types
            .iter()
            .map(|vt| value_type_try_into_sqlite_marker(vt))
            .collect_vec(),
    }
}

#[cfg(test)]
mod test_types {
    use super::*;

    #[test]
    fn test_cmp() {
        let mysql_bool = MYSQL_TMAP.get("TINYINT(1)").unwrap();

        println!("{:?}", "TINYINT(1)" == mysql_bool);
    }
}
