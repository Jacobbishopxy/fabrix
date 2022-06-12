//! Fabrix sources: db error
//!
//! This module contains the error types for the database.

use std::error::Error as StdError;
use std::fmt::Display;

use fabrix_core::CoreError;
use nom::error::{ErrorKind, ParseError};
use thiserror::Error;

pub type SqlResult<T> = Result<T, SqlError>;

#[derive(Debug)]
pub enum CommonError {
    Str(&'static str),
    String(String),
}

impl AsRef<str> for CommonError {
    fn as_ref(&self) -> &str {
        match self {
            CommonError::Str(s) => s,
            CommonError::String(s) => s.as_str(),
        }
    }
}

impl std::fmt::Display for CommonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommonError::Str(v) => write!(f, "{:?}", v),
            CommonError::String(v) => write!(f, "{:?}", v),
        }
    }
}

impl From<&'static str> for CommonError {
    fn from(v: &'static str) -> Self {
        CommonError::Str(v)
    }
}

impl From<String> for CommonError {
    fn from(v: String) -> Self {
        CommonError::String(v)
    }
}

// ================================================================================================
// Nom error
// ================================================================================================

#[derive(Debug)]
pub struct NomError(String);

impl std::fmt::Display for NomError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<T: AsRef<str>> From<(T, ErrorKind)> for NomError {
    fn from(error: (T, ErrorKind)) -> Self {
        let (s, kind) = error;
        NomError(format!("Nom error code: {}, {:?}", s.as_ref(), kind))
    }
}

impl<T: AsRef<str>> ParseError<T> for NomError {
    fn from_error_kind(_: T, kind: ErrorKind) -> Self {
        let s = format!("Nom error code:  {:?}", kind);
        NomError(s)
    }

    fn append(_: T, kind: ErrorKind, other: Self) -> Self {
        NomError(format!("{:?}\nerror code: {:?}", other, kind))
    }
}

#[derive(Error, Debug)]
pub enum SqlError {
    #[error("common error {0}")]
    Common(CommonError),

    #[error(transparent)]
    CORE(#[from] CoreError),

    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),

    #[error(transparent)]
    ParseInt(#[from] std::num::ParseIntError),

    #[error(transparent)]
    SeqQuery(#[from] sea_query::error::Error),

    #[error(transparent)]
    Nom(#[from] nom::Err<NomError>),
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
