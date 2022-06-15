//! DynConnError
//!
//! error handling

use thiserror::Error;

pub type DynConnResult<T> = Result<T, DynConnError>;

#[derive(Error, Debug)]
pub enum DynConnError {
    #[error(transparent)]
    Sql(#[from] fabrix_sql::SqlError),

    #[error("locked")]
    Locked,

    #[error("absent")]
    Absent,
}
