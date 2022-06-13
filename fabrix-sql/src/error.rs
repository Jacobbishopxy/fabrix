//! Fabrix sources: db error
//!
//! This module contains the error types for the database.

use std::error::Error as StdError;
use std::fmt::Display;

use fabrix_core::CoreError;
use nom::error::{ErrorKind, ParseError};
use thiserror::Error;

pub type SqlResult<T> = Result<T, SqlError>;

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
    #[error("invalid builder {0}")]
    InvalidBuilder(String),

    #[error("conversion error {0} to {1}")]
    Conversion(String, String),

    #[error("unsupported sea-query type")]
    UnsupportedSeaQueryType,

    #[error("invalid type {0}")]
    InvalidType(&'static str),

    #[error("invalid constraint")]
    InvalidConstraint,

    #[error("invalid index type {0}")]
    InvalidIndex(String),

    #[error("invalid connection string {0}")]
    InvalidConnStr(String),

    #[error("connection has already been established")]
    ConnectionAlreadyEstablished,

    #[error("connection has not been established")]
    ConnectionNotEstablished,

    #[error("unsupported database operation {0}")]
    UnsupportedDatabaseOperation(&'static str),

    #[error("source {0} not found")]
    SourceNotFound(&'static str),

    #[error("source {0} already exists")]
    SourceAlreadyExists(&'static str),

    #[error("content {0} is empty")]
    EmptyContent(String),

    #[error("mismatched sql row {0}")]
    MismatchedSqlRow(&'static str),

    #[error(transparent)]
    Core(#[from] CoreError),

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
