//! Fabrix sources: db error
//!
//! This module contains the error types for the database.

use std::error::Error as StdError;
use std::fmt::Display;

use thiserror::Error;

use crate::{CommonError, ValueType};

pub type DbResult<T> = Result<T, DbError>;

#[derive(Error, Debug)]
pub enum DbError {
    #[error("common error {0}")]
    Common(CommonError),

    #[error("parse {0} into {1} error ")]
    Parse(String, String),

    #[error(transparent)]
    Polars(#[from] polars::error::PolarsError),

    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),

    #[error(transparent)]
    SeqQuery(#[from] sea_query::error::Error),
}

type DataFrameDTypes = (ValueType, Vec<ValueType>);

impl DbError {
    pub fn new_common_error<T>(msg: T) -> DbError
    where
        T: Into<CommonError>,
    {
        DbError::Common(msg.into())
    }

    pub fn new_parse_error<T1, T2>(type1: T1, type2: T2) -> DbError
    where
        T1: Display,
        T2: Display,
    {
        DbError::Parse(type1.to_string(), type2.to_string())
    }

    pub fn new_parse_info_error<T>(r#type: T, info: &str) -> DbError
    where
        T: Display,
    {
        DbError::Parse(r#type.to_string(), info.to_string())
    }

    pub fn new_df_dtypes_mismatch_error(d1: DataFrameDTypes, d2: DataFrameDTypes) -> DbError {
        DbError::new_common_error(format!(
            "dataframe dtypes mismatch, d1: {:#?}, d2: {:#?}",
            d1, d2
        ))
    }

    pub fn new_empty_error() -> DbError {
        DbError::new_common_error("empty content")
    }

    pub fn turn_into_sqlx_decode_error(self) -> sqlx::Error {
        match self {
            DbError::Sqlx(se) => se,
            _ => sqlx::Error::Decode(Box::new(SqlDecodeError::new("sql row decode error"))),
        }
    }
}

#[derive(Debug)]
pub struct SqlDecodeError {
    pub err: String,
}

impl SqlDecodeError {
    pub fn new<T>(err: T) -> Self
    where
        T: AsRef<str>,
    {
        SqlDecodeError {
            err: err.as_ref().to_owned(),
        }
    }
}

impl Display for SqlDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.err)
    }
}

impl StdError for SqlDecodeError {}
