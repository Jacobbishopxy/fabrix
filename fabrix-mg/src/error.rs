//! FabrixMongoError
//!
//! error type for Fabrix MongoDB

use thiserror::Error;

pub type MgResult<T> = Result<T, MgError>;

#[derive(Error, Debug)]
pub enum MgError {
    #[error("object id not found")]
    OidNotFound,

    #[error("invalid argument: {0}")]
    InvalidArgument(&'static str),

    #[error("conversion error: from {0} to {1}")]
    Conversion(String, String),

    #[error(transparent)]
    Core(#[from] fabrix_core::CoreError),

    #[error(transparent)]
    FuturesIO(#[from] futures::io::Error),

    #[error(transparent)]
    Mongo(#[from] mongodb::error::Error),
}
