//! DynConnError
//!
//! error handling

use thiserror::Error;

pub type DynConnResult<T> = Result<T, DynConnError>;

#[derive(Error, Debug)]
pub enum DynConnError {
    #[cfg(feature = "sql")]
    #[error(transparent)]
    Sql(#[from] fabrix_sql::SqlError),

    #[cfg(feature = "mongo")]
    #[error(transparent)]
    Mg(#[from] fabrix_mg::MgError),

    #[error("locked")]
    Locked,

    #[error("absent")]
    Absent,
}
