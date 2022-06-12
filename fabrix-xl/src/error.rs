//! Fabrix sources: xl error
//!
//!  

use fabrix_core::CoreError;
use thiserror::Error;

pub type XlResult<T> = Result<T, XlError>;

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

#[derive(Error, Debug)]
pub enum XlError {
    #[error("common error {0}")]
    Common(CommonError),

    #[error(transparent)]
    CORE(#[from] CoreError),

    // IO errors
    #[error(transparent)]
    IO(#[from] std::io::Error),

    // Zip errors
    #[error(transparent)]
    Zip(#[from] zip::result::ZipError),
}

impl XlError {
    pub fn new_common_error<T>(msg: T) -> XlError
    where
        T: Into<CommonError>,
    {
        XlError::Common(msg.into())
    }
}
