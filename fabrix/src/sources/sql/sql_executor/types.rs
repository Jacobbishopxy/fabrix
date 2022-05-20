//! Sql types
//!
//! Based on Sqlx types mapping:
//! - MySQL: https://docs.rs/sqlx/latest/sqlx/mysql/types/index.html
//! - Postgres: https://docs.rs/sqlx/latest/sqlx/postgres/types/index.html
//! - SQLite: https://docs.rs/sqlx/latest/sqlx/sqlite/types/index.html

use std::{collections::HashMap, marker::PhantomData};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use itertools::Itertools;
use sqlx::{mysql::MySqlRow, postgres::PgRow, sqlite::SqliteRow, Row as SRow};

use super::{impl_sql_type_tag_marker, static_sttm_get, tmap_pair};
use crate::{Bytes, Decimal, SqlBuilder, SqlResult, Uuid, Value, ValueType};

const MISMATCHED_SQL_ROW: &str = "mismatched sql row";

/// type alias
pub(crate) type OptMarker = Option<&'static dyn SqlTypeTagMarker>;

/// Type of Sql row
pub enum SqlRow {
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
    fn extract_value(&self, sql_row: &SqlRow, idx: usize) -> SqlResult<Value>;

    /// extract Option<Value> from sql row
    fn extract_optional_value(&self, sql_row: &SqlRow, idx: usize) -> SqlResult<Option<Value>>;
}

/// tmap value type
pub(crate) type Sttm = Box<dyn SqlTypeTagMarker>;

impl PartialEq<str> for Sttm {
    fn eq(&self, other: &str) -> bool {
        self.to_str() == other
    }
}

impl PartialEq<Sttm> for str {
    fn eq(&self, other: &Sttm) -> bool {
        self == other.to_str()
    }
}

// ================================================================================================
// impl SqlTypeTagMarker for SqlTypeTag<T>
// ================================================================================================

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
impl_sql_type_tag_marker!(NaiveDate, Date; [Mysql, Pg], MISMATCHED_SQL_ROW);
impl_sql_type_tag_marker!(NaiveTime, Time; [Mysql, Pg], MISMATCHED_SQL_ROW);
impl_sql_type_tag_marker!(NaiveDateTime, DateTime; [Mysql, Pg, Sqlite]);
impl_sql_type_tag_marker!(Decimal <= rust_decimal::Decimal, Decimal; [Mysql, Pg], MISMATCHED_SQL_ROW);
impl_sql_type_tag_marker!(Uuid <= uuid::Uuid, Uuid; [Pg], MISMATCHED_SQL_ROW);
impl_sql_type_tag_marker!(Bytes <= Vec<u8>, Bytes; [Mysql, Pg, Sqlite]);

// ================================================================================================
// static types mapping
// ================================================================================================

lazy_static::lazy_static! {
    /// Mysql Type Mapping: &'static str -> SqlTypeTag instance
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
            tmap_pair!("TIMESTAMP", NaiveDateTime),
            tmap_pair!("DATETIME", NaiveDateTime),
            tmap_pair!("DATE", NaiveDate),
            tmap_pair!("TIME", NaiveTime),
            tmap_pair!("DECIMAL", Decimal),
            tmap_pair!("VARBINARY", Bytes),
            tmap_pair!("BINARY", Bytes),
            tmap_pair!("BLOB", Bytes),
        ])
    };

    /// Postgres Type Mapping: &'static str -> SqlTypeTag instance
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
            tmap_pair!("TIMESTAMPTZ", NaiveDateTime),
            tmap_pair!("TIMESTAMP", NaiveDateTime),
            tmap_pair!("DATE", NaiveDate),
            tmap_pair!("TIME", NaiveTime),
            tmap_pair!("NUMERIC", Decimal),
            tmap_pair!("BYTEA", Bytes),
        ])
    };

    /// Sqlite Type Mapping: &'static str -> SqlTypeTag instance
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
            tmap_pair!("DATETIME", NaiveDateTime),
            tmap_pair!("BLOB", Bytes),
        ])
    };
}

// ================================================================================================
// types mapping functions
// ================================================================================================

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
fn value_type_try_into_mysql_marker(vt: &ValueType) -> Option<&'static dyn SqlTypeTagMarker> {
    match vt {
        ValueType::Bool => Some(static_sttm_get!(MYSQL_TMAP, "BOOLEAN")),
        ValueType::U8 => Some(static_sttm_get!(MYSQL_TMAP, "TINYINT UNSIGNED")),
        ValueType::U16 => Some(static_sttm_get!(MYSQL_TMAP, "SMALLINT UNSIGNED")),
        ValueType::U32 => Some(static_sttm_get!(MYSQL_TMAP, "INT UNSIGNED")),
        ValueType::U64 => Some(static_sttm_get!(MYSQL_TMAP, "BIGINT UNSIGNED")),
        ValueType::I8 => Some(static_sttm_get!(MYSQL_TMAP, "TINYINT")),
        ValueType::I16 => Some(static_sttm_get!(MYSQL_TMAP, "SMALLINT")),
        ValueType::I32 => Some(static_sttm_get!(MYSQL_TMAP, "INT")),
        ValueType::I64 => Some(static_sttm_get!(MYSQL_TMAP, "BIGINT")),
        ValueType::F32 => Some(static_sttm_get!(MYSQL_TMAP, "FLOAT")),
        ValueType::F64 => Some(static_sttm_get!(MYSQL_TMAP, "DOUBLE")),
        ValueType::String => Some(static_sttm_get!(MYSQL_TMAP, "VARCHAR")),
        ValueType::Date => Some(static_sttm_get!(MYSQL_TMAP, "DATE")),
        ValueType::Time => Some(static_sttm_get!(MYSQL_TMAP, "TIME")),
        ValueType::DateTime => Some(static_sttm_get!(MYSQL_TMAP, "DATETIME")),
        ValueType::Decimal => Some(static_sttm_get!(MYSQL_TMAP, "DECIMAL")),
        ValueType::Bytes => Some(static_sttm_get!(MYSQL_TMAP, "VARBINARY")),
        _ => None,
    }
}

/// value_type -> pg marker
fn value_type_try_into_pg_marker(vt: &ValueType) -> Option<&'static dyn SqlTypeTagMarker> {
    match vt {
        ValueType::Bool => Some(static_sttm_get!(PG_TMAP, "BOOL")),
        ValueType::U8 => Some(static_sttm_get!(PG_TMAP, "TINYINT")),
        ValueType::U16 => Some(static_sttm_get!(PG_TMAP, "SMALLINT")),
        ValueType::U32 => Some(static_sttm_get!(PG_TMAP, "INT")),
        ValueType::U64 => Some(static_sttm_get!(PG_TMAP, "BIGINT")),
        ValueType::I8 => Some(static_sttm_get!(PG_TMAP, "TINYINT")),
        ValueType::I16 => Some(static_sttm_get!(PG_TMAP, "SMALLINT")),
        ValueType::I32 => Some(static_sttm_get!(PG_TMAP, "INT")),
        ValueType::I64 => Some(static_sttm_get!(PG_TMAP, "BIGINT")),
        ValueType::F32 => Some(static_sttm_get!(PG_TMAP, "REAL")),
        ValueType::F64 => Some(static_sttm_get!(PG_TMAP, "DOUBLE PRECISION")),
        ValueType::String => Some(static_sttm_get!(PG_TMAP, "VARCHAR")),
        ValueType::Date => Some(static_sttm_get!(PG_TMAP, "DATE")),
        ValueType::Time => Some(static_sttm_get!(PG_TMAP, "TIME")),
        ValueType::DateTime => Some(static_sttm_get!(PG_TMAP, "TIMESTAMP")),
        ValueType::Decimal => Some(static_sttm_get!(PG_TMAP, "NUMERIC")),
        ValueType::Uuid => Some(static_sttm_get!(PG_TMAP, "UUID")),
        ValueType::Bytes => Some(static_sttm_get!(PG_TMAP, "BYTEA")),
        _ => None,
    }
}

/// value_type -> sqlite marker
fn value_type_try_into_sqlite_marker(vt: &ValueType) -> Option<&'static dyn SqlTypeTagMarker> {
    match vt {
        ValueType::Bool => Some(static_sttm_get!(SQLITE_TMAP, "BOOLEAN")),
        ValueType::U8 => Some(static_sttm_get!(SQLITE_TMAP, "INTEGER")),
        ValueType::U16 => Some(static_sttm_get!(SQLITE_TMAP, "INTEGER")),
        ValueType::U32 => Some(static_sttm_get!(SQLITE_TMAP, "INTEGER")),
        ValueType::U64 => Some(static_sttm_get!(SQLITE_TMAP, "BIGINT")),
        ValueType::I8 => Some(static_sttm_get!(SQLITE_TMAP, "INTEGER")),
        ValueType::I16 => Some(static_sttm_get!(SQLITE_TMAP, "INTEGER")),
        ValueType::I32 => Some(static_sttm_get!(SQLITE_TMAP, "INTEGER")),
        ValueType::I64 => Some(static_sttm_get!(SQLITE_TMAP, "BIGINT")),
        ValueType::F32 => Some(static_sttm_get!(SQLITE_TMAP, "REAL")),
        ValueType::F64 => Some(static_sttm_get!(SQLITE_TMAP, "REAL")),
        ValueType::String => Some(static_sttm_get!(SQLITE_TMAP, "VARCHAR")),
        ValueType::DateTime => Some(static_sttm_get!(SQLITE_TMAP, "DATETIME")),
        ValueType::Bytes => Some(static_sttm_get!(SQLITE_TMAP, "BLOB")),
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
            .map(value_type_try_into_mysql_marker)
            .collect_vec(),
        SqlBuilder::Postgres => value_types
            .iter()
            .map(value_type_try_into_pg_marker)
            .collect_vec(),
        SqlBuilder::Sqlite => value_types
            .iter()
            .map(value_type_try_into_sqlite_marker)
            .collect_vec(),
    }
}

#[cfg(test)]
mod test_types {
    use super::*;

    #[test]
    fn test_types_mysql() {
        let t = MYSQL_TMAP.get("TINYINT(1)").unwrap();
        assert_eq!(t.to_dtype(), ValueType::Bool);

        let t = MYSQL_TMAP.get("BOOLEAN").unwrap();
        assert_eq!(t.to_dtype(), ValueType::Bool);

        let t = MYSQL_TMAP.get("TINYINT UNSIGNED").unwrap();
        assert_eq!(t.to_dtype(), ValueType::U8);

        let t = MYSQL_TMAP.get("SMALLINT UNSIGNED").unwrap();
        assert_eq!(t.to_dtype(), ValueType::U16);

        let t = MYSQL_TMAP.get("INT UNSIGNED").unwrap();
        assert_eq!(t.to_dtype(), ValueType::U32);

        let t = MYSQL_TMAP.get("BIGINT UNSIGNED").unwrap();
        assert_eq!(t.to_dtype(), ValueType::U64);

        let t = MYSQL_TMAP.get("TINYINT").unwrap();
        assert_eq!(t.to_dtype(), ValueType::I8);

        let t = MYSQL_TMAP.get("SMALLINT").unwrap();
        assert_eq!(t.to_dtype(), ValueType::I16);

        let t = MYSQL_TMAP.get("INT").unwrap();
        assert_eq!(t.to_dtype(), ValueType::I32);

        let t = MYSQL_TMAP.get("BIGINT").unwrap();
        assert_eq!(t.to_dtype(), ValueType::I64);

        let t = MYSQL_TMAP.get("FLOAT").unwrap();
        assert_eq!(t.to_dtype(), ValueType::F32);

        let t = MYSQL_TMAP.get("DOUBLE").unwrap();
        assert_eq!(t.to_dtype(), ValueType::F64);

        let t = MYSQL_TMAP.get("VARCHAR").unwrap();
        assert_eq!(t.to_dtype(), ValueType::String);

        let t = MYSQL_TMAP.get("CHAR").unwrap();
        assert_eq!(t.to_dtype(), ValueType::String);

        let t = MYSQL_TMAP.get("TEXT").unwrap();
        assert_eq!(t.to_dtype(), ValueType::String);

        let t = MYSQL_TMAP.get("TIMESTAMP").unwrap();
        assert_eq!(t.to_dtype(), ValueType::DateTime);

        let t = MYSQL_TMAP.get("DATETIME").unwrap();
        assert_eq!(t.to_dtype(), ValueType::DateTime);

        let t = MYSQL_TMAP.get("DATE").unwrap();
        assert_eq!(t.to_dtype(), ValueType::Date);

        let t = MYSQL_TMAP.get("TIME").unwrap();
        assert_eq!(t.to_dtype(), ValueType::Time);

        let t = MYSQL_TMAP.get("DECIMAL").unwrap();
        assert_eq!(t.to_dtype(), ValueType::Decimal);

        let t = MYSQL_TMAP.get("VARBINARY").unwrap();
        assert_eq!(t.to_dtype(), ValueType::Bytes);

        let t = MYSQL_TMAP.get("BINARY").unwrap();
        assert_eq!(t.to_dtype(), ValueType::Bytes);

        let t = MYSQL_TMAP.get("BLOB").unwrap();
        assert_eq!(t.to_dtype(), ValueType::Bytes);
    }

    #[test]
    fn test_types_postgres() {
        let t = PG_TMAP.get("BOOL").unwrap();
        assert_eq!(t.to_dtype(), ValueType::Bool);

        let t = PG_TMAP.get("CHAR").unwrap();
        assert_eq!(t.to_dtype(), ValueType::I8);

        let t = PG_TMAP.get("TINYINT").unwrap();
        assert_eq!(t.to_dtype(), ValueType::I8);

        let t = PG_TMAP.get("SMALLINT").unwrap();
        assert_eq!(t.to_dtype(), ValueType::I16);

        let t = PG_TMAP.get("SMALLSERIAL").unwrap();
        assert_eq!(t.to_dtype(), ValueType::I16);

        let t = PG_TMAP.get("INT2").unwrap();
        assert_eq!(t.to_dtype(), ValueType::I16);

        let t = PG_TMAP.get("INT").unwrap();
        assert_eq!(t.to_dtype(), ValueType::I32);

        let t = PG_TMAP.get("SERIAL").unwrap();
        assert_eq!(t.to_dtype(), ValueType::I32);

        let t = PG_TMAP.get("INT4").unwrap();
        assert_eq!(t.to_dtype(), ValueType::I32);

        let t = PG_TMAP.get("BIGINT").unwrap();
        assert_eq!(t.to_dtype(), ValueType::I64);

        let t = PG_TMAP.get("BIGSERIAL").unwrap();
        assert_eq!(t.to_dtype(), ValueType::I64);

        let t = PG_TMAP.get("INT8").unwrap();
        assert_eq!(t.to_dtype(), ValueType::I64);

        let t = PG_TMAP.get("REAL").unwrap();
        assert_eq!(t.to_dtype(), ValueType::F32);

        let t = PG_TMAP.get("FLOAT4").unwrap();
        assert_eq!(t.to_dtype(), ValueType::F32);

        let t = PG_TMAP.get("DOUBLE PRECISION").unwrap();
        assert_eq!(t.to_dtype(), ValueType::F64);

        let t = PG_TMAP.get("FLOAT8").unwrap();
        assert_eq!(t.to_dtype(), ValueType::F64);

        let t = PG_TMAP.get("VARCHAR").unwrap();
        assert_eq!(t.to_dtype(), ValueType::String);

        let t = PG_TMAP.get("CHAR(N)").unwrap();
        assert_eq!(t.to_dtype(), ValueType::String);

        let t = PG_TMAP.get("TEXT").unwrap();
        assert_eq!(t.to_dtype(), ValueType::String);

        let t = PG_TMAP.get("NAME").unwrap();
        assert_eq!(t.to_dtype(), ValueType::String);

        let t = PG_TMAP.get("TIMESTAMPTZ").unwrap();
        assert_eq!(t.to_dtype(), ValueType::DateTime);

        let t = PG_TMAP.get("TIMESTAMP").unwrap();
        assert_eq!(t.to_dtype(), ValueType::DateTime);

        let t = PG_TMAP.get("DATE").unwrap();
        assert_eq!(t.to_dtype(), ValueType::Date);

        let t = PG_TMAP.get("TIME").unwrap();
        assert_eq!(t.to_dtype(), ValueType::Time);

        let t = PG_TMAP.get("NUMERIC").unwrap();
        assert_eq!(t.to_dtype(), ValueType::Decimal);

        let t = PG_TMAP.get("BYTEA").unwrap();
        assert_eq!(t.to_dtype(), ValueType::Bytes);
    }

    #[test]
    fn test_types_sqlite() {
        let t = SQLITE_TMAP.get("BOOLEAN").unwrap();
        assert_eq!(t.to_dtype(), ValueType::Bool);

        let t = SQLITE_TMAP.get("INTEGER").unwrap();
        assert_eq!(t.to_dtype(), ValueType::I32);

        let t = SQLITE_TMAP.get("BIGINT").unwrap();
        assert_eq!(t.to_dtype(), ValueType::I64);

        let t = SQLITE_TMAP.get("INT8").unwrap();
        assert_eq!(t.to_dtype(), ValueType::I64);

        let t = SQLITE_TMAP.get("REAL").unwrap();
        assert_eq!(t.to_dtype(), ValueType::F64);

        let t = SQLITE_TMAP.get("VARCHAR").unwrap();
        assert_eq!(t.to_dtype(), ValueType::String);

        let t = SQLITE_TMAP.get("CHAR(N)").unwrap();
        assert_eq!(t.to_dtype(), ValueType::String);

        let t = SQLITE_TMAP.get("TEXT").unwrap();
        assert_eq!(t.to_dtype(), ValueType::String);

        let t = SQLITE_TMAP.get("DATETIME").unwrap();
        assert_eq!(t.to_dtype(), ValueType::DateTime);

        let t = SQLITE_TMAP.get("BLOB").unwrap();
        assert_eq!(t.to_dtype(), ValueType::Bytes);
    }
}
