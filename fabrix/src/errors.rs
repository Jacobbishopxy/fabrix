//! fabrix error type
//!
//! errors

use std::fmt::Display;

use thiserror::Error;

#[cfg(feature = "sql")]
use crate::DbError;
#[cfg(feature = "file")]
use crate::FlError;

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
    #[error("common error {0}")]
    Common(CommonError),

    #[cfg(feature = "sql")]
    #[error(transparent)]
    DB(#[from] DbError),

    #[cfg(feature = "file")]
    #[error(transparent)]
    FL(#[from] FlError),

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
    pub fn from_common_error<T>(msg: T) -> Self
    where
        T: Into<CommonError>,
    {
        FabrixError::Common(msg.into())
    }
}
