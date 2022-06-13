//! Fabrix sources: xl error
//!
//!  

use fabrix_core::CoreError;
use thiserror::Error;

pub type XlResult<T> = Result<T, XlError>;

#[derive(Error, Debug)]
pub enum XlError {
    #[error("unsupported source type")]
    UnsupportedSource,

    #[error("{0} not found")]
    SourceNotFound(&'static str),

    #[error("parsing error {0}")]
    Parsing(&'static str),

    #[error("{0}")]
    Unexpected(String),

    #[error(transparent)]
    Core(#[from] CoreError),

    // IO errors
    #[error(transparent)]
    IO(#[from] std::io::Error),

    #[error(transparent)]
    Xml(#[from] quick_xml::Error),

    // Zip errors
    #[error(transparent)]
    Zip(#[from] zip::result::ZipError),
}
