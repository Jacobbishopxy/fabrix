//! fabrix error type
//!
//! errors

use std::fmt::Display;

use thiserror::Error;

use crate::CoreError;
#[cfg(feature = "sql")]
use crate::SqlError;

/// Result type for fabrix
pub type FabrixResult<T> = Result<T, FabrixError>;

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

impl Display for CommonError {
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

#[derive(Error, Debug)]
pub enum FabrixError {
    // common errors
    #[error("common error {0}")]
    Common(CommonError),

    // IO errors
    #[error(transparent)]
    IO(#[from] std::io::Error),

    // Core errors
    #[error(transparent)]
    CORE(#[from] CoreError),

    // Polars errors
    #[error(transparent)]
    POLARS(#[from] polars::prelude::PolarsError),

    // Zip errors
    #[error(transparent)]
    Zip(#[from] zip::result::ZipError),

    // Sql errors
    #[cfg(feature = "sql")]
    #[error(transparent)]
    DB(#[from] SqlError),

    // Mongo errors
    // #[cfg(feature = "mongo")]
    // #[error(transparent)]
    // MONGO(#[from] MongoError),

    // Tokio lock error
    #[error(transparent)]
    TokioLock(#[from] tokio::sync::TryLockError),

    // unknown error
    #[error("unknown error")]
    Unknown,
}

impl From<&str> for FabrixError {
    fn from(v: &str) -> Self {
        FabrixError::Common(CommonError::String(v.to_owned()))
    }
}

impl From<String> for FabrixError {
    fn from(v: String) -> Self {
        FabrixError::Common(CommonError::String(v))
    }
}

impl FabrixError {
    pub fn new_common_error<T>(msg: T) -> Self
    where
        T: Into<CommonError>,
    {
        FabrixError::Common(msg.into())
    }
}
