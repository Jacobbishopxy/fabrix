//! fabrix error type
//!
//! errors

use nom::error::{ErrorKind, ParseError};
use thiserror::Error;

use crate::CoreError;
#[cfg(feature = "sql")]
use crate::SqlError;

/// Result type for fabrix
pub type FabrixResult<T> = Result<T, FabrixError>;

// ================================================================================================
// Common error
// ================================================================================================

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

// ================================================================================================
// Fabrix error
// ================================================================================================

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

    // Nom errors
    #[error(transparent)]
    Nom(#[from] nom::Err<NomError>),

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
