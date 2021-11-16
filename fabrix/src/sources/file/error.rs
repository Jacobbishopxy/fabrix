//! File error
//!
//! This module contains the error type for the file source.

use thiserror::Error;

use crate::CommonError;

pub type XlResult<T> = Result<T, XlError>;

#[derive(Error, Debug)]
pub enum XlError {
    #[error("common error {0}")]
    CommonError(CommonError),

    #[error(transparent)]
    StdIOError(#[from] std::io::Error),

    #[error(transparent)]
    ZipError(#[from] zip::result::ZipError),

    #[error("unknown error")]
    Unknown,
}

impl XlError {
    pub fn from_common_error<T>(msg: T) -> XlError
    where
        T: Into<CommonError>,
    {
        XlError::CommonError(msg.into())
    }
}
