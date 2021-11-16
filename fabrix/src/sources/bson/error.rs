//! Fabrix sources: Bson/Json error
//!
//! This module contains the error types for the Bson and Json sources.

use thiserror::Error;

use crate::CommonError;

pub type BsResult<T> = Result<T, BsError>;

#[derive(Error, Debug)]
pub enum BsError {
    #[error("common error {0}")]
    CommonError(CommonError),

    #[error("unknown error")]
    Unknown,
}

impl BsError {
    pub fn from_common_error<T>(msg: T) -> Self
    where
        T: Into<CommonError>,
    {
        BsError::CommonError(msg.into())
    }
}
