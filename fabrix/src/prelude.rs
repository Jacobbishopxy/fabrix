//! Fabrix prelude
//!
//! This is the prelude for the Fabrix crate.

pub use super::errors::*;
pub use crate::core::{
    DataFrame, Date, DateTime, Decimal, FieldInfo, Row, Series, Time, Uuid, Value, ValueType, D1,
    D2,
};

// sources: db
#[cfg(feature = "sql")]
pub use crate::sources::sql::error::{SqlError, SqlResult};
#[cfg(feature = "sql")]
pub use crate::sources::sql::sql_builder::adt;
#[cfg(feature = "sql")]
pub use crate::sources::sql::sql_executor::{SqlConnInfo, SqlEngine, SqlExecutor};

// sources: file
#[cfg(feature = "file")]
pub use crate::sources::file::error::{FlError, FlResult};

// sources: bson
#[cfg(feature = "bson")]
pub use crate::sources::bson::error::{BsError, BsResult};
