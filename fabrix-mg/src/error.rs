//! FabrixMongoError
//!
//! error type for Fabrix MongoDB

use thiserror::Error;

pub type MgResult<T> = Result<T, MgError>;

#[derive(Error, Debug)]
pub enum MgError {
    #[error("object id not found")]
    OidNotFound,

    #[error(transparent)]
    Core(#[from] fabrix_core::CoreError),

    #[error(transparent)]
    FuturesIO(#[from] futures::io::Error),

    #[error(transparent)]
    Mongo(#[from] mongodb::error::Error),
}
