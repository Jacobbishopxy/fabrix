//! FabrixMongoError
//!
//! error type for Fabrix MongoDB

use thiserror::Error;

pub type MgResult<T> = Result<T, MgError>;

#[derive(Error, Debug)]
pub enum MgError {
    #[error("object id not found")]
    OidNotFound,

    #[error("parse object id failed")]
    OidParseFailed,

    #[error("result not found")]
    ResultNotFound,

    #[error("cursor error")]
    CursorError,

    #[error("invalid argument: {0}")]
    InvalidArgument(&'static str),

    #[error("conversion error: from {0} to {1}")]
    Conversion(String, String),

    #[error("connection already established")]
    ConnectionAlreadyEstablished,

    #[error("connection not yet established")]
    ConnectionNotYetEstablished,

    #[error("database/collection not set")]
    DatabaseOrCollectionNotSet,

    #[error(transparent)]
    Core(#[from] fabrix_core::CoreError),

    #[error(transparent)]
    FuturesIO(#[from] futures::io::Error),

    #[error(transparent)]
    BsonOid(#[from] bson::oid::Error),

    #[error(transparent)]
    BsonSe(#[from] bson::ser::Error),

    #[error(transparent)]
    BsonDe(#[from] bson::de::Error),

    #[error(transparent)]
    Mongo(#[from] mongodb::error::Error),
}
