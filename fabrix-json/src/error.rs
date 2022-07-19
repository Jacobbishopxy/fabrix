//! Fabrix Json Error

use thiserror::Error;

pub type JsonResult<T> = Result<T, JsonError>;

#[derive(Error, Debug)]
pub enum JsonError {
    #[error("source not found")]
    SourceNotFound,

    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
}
