//! Fabrix sources: db error
//!
//! This module contains the error types for the database.

use std::error::Error as StdError;
use std::fmt::Display;

use thiserror::Error;

use crate::{CommonError, ValueType};

pub type SqlResult<T> = Result<T, SqlError>;

#[derive(Error, Debug)]
pub enum SqlError {
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

impl SqlError {
    pub fn new_common_error<T>(msg: T) -> SqlError
    where
        T: Into<CommonError>,
    {
        SqlError::Common(msg.into())
    }

    pub fn new_parse_error<T1, T2>(type1: T1, type2: T2) -> SqlError
    where
        T1: Display,
        T2: Display,
    {
        SqlError::Parse(type1.to_string(), type2.to_string())
    }

    pub fn new_parse_info_error<T>(r#type: T, info: &str) -> SqlError
    where
        T: Display,
    {
        SqlError::Parse(r#type.to_string(), info.to_string())
    }

    pub fn new_df_dtypes_mismatch_error(d1: DataFrameDTypes, d2: DataFrameDTypes) -> SqlError {
        SqlError::new_common_error(format!(
            "dataframe dtypes mismatch, d1: {:#?}, d2: {:#?}",
            d1, d2
        ))
    }

    pub fn new_empty_error() -> SqlError {
        SqlError::new_common_error("empty content")
    }

    pub fn turn_into_sqlx_decode_error(self) -> sqlx::Error {
        match self {
            SqlError::Sqlx(se) => se,
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
