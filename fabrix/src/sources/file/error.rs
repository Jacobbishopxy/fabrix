//! Fabrix sources: File error
//!
//! This module contains the error type for the file source.

use thiserror::Error;

use crate::CommonError;

pub type FlResult<T> = Result<T, FlError>;

#[derive(Error, Debug)]
pub enum FlError {
    #[error("common error {0}")]
    Common(CommonError),

    #[error(transparent)]
    StdIO(#[from] std::io::Error),

    #[error(transparent)]
    Zip(#[from] zip::result::ZipError),

    #[error("unknown error")]
    Unknown,
}

impl FlError {
    pub fn from_common_error<T>(msg: T) -> FlError
    where
        T: Into<CommonError>,
    {
        FlError::Common(msg.into())
    }
}
