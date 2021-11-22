//! Fabrix sources: db error
//!
//! This module contains the error types for the database.

use std::error::Error as StdError;
use std::fmt::Display;

use thiserror::Error;

use crate::{CommonError, CoreError};

pub type SqlResult<T> = Result<T, SqlError>;

#[derive(Error, Debug)]
pub enum SqlError {
    #[error("common error {0}")]
    Common(CommonError),

    #[error(transparent)]
    CORE(#[from] CoreError),

    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),

    #[error(transparent)]
    SeqQuery(#[from] sea_query::error::Error),
}

impl SqlError {
    pub fn new_common_error<T>(msg: T) -> SqlError
    where
        T: Into<CommonError>,
    {
        SqlError::Common(msg.into())
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
