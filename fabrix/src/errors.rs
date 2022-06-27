//! fabrix error type
//!
//! errors

use thiserror::Error;

/// Result type for fabrix
pub type FabrixResult<T> = Result<T, FabrixError>;

// ================================================================================================
// Uncategorized errors
// ================================================================================================
#[derive(Debug)]
pub enum UncategorizedError {
    Str(&'static str),
    String(String),
}

impl AsRef<str> for UncategorizedError {
    fn as_ref(&self) -> &str {
        match self {
            UncategorizedError::Str(s) => s,
            UncategorizedError::String(s) => s.as_str(),
        }
    }
}

impl std::fmt::Display for UncategorizedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UncategorizedError::Str(v) => write!(f, "{:?}", v),
            UncategorizedError::String(v) => write!(f, "{:?}", v),
        }
    }
}

impl From<&'static str> for UncategorizedError {
    fn from(v: &'static str) -> Self {
        UncategorizedError::Str(v)
    }
}

impl From<String> for UncategorizedError {
    fn from(v: String) -> Self {
        UncategorizedError::String(v)
    }
}

// ================================================================================================
// Fabrix error
// ================================================================================================

#[derive(Error, Debug)]
pub enum FabrixError {
    #[error("uncategorized err {0}")]
    Uncategorized(UncategorizedError),

    #[error("empty content {0}")]
    EmptyContent(&'static str),

    #[error("{0} is out of range {1}")]
    OutOfRange(usize, usize),

    #[error("{0}")]
    NotFound(String),

    #[error("{0} is not initialized")]
    NotInitialized(&'static str),

    #[error("{0} is not set")]
    NotSet(&'static str),

    #[error("unsupported type {0}")]
    UnsupportedType(&'static str),

    #[error("{0}")]
    InvalidArgument(String),

    // IO errors
    #[error(transparent)]
    IO(#[from] std::io::Error),

    // Core errors
    #[error(transparent)]
    Core(#[from] fabrix_core::CoreError),

    // Polars errors
    #[error(transparent)]
    Polars(#[from] polars::prelude::PolarsError),

    // Sql errors
    #[cfg(feature = "sql")]
    #[error(transparent)]
    Sql(#[from] fabrix_sql::SqlError),

    // Xl errors
    #[cfg(feature = "xl")]
    #[error(transparent)]
    Xl(#[from] fabrix_xl::XlError),

    // Mongo errors
    #[cfg(feature = "mongo")]
    #[error(transparent)]
    Mg(#[from] fabrix_mg::MgError),

    // Tokio lock error
    #[error(transparent)]
    TokioLock(#[from] tokio::sync::TryLockError),

    // unknown error
    #[error("unknown error")]
    Unknown,
}
