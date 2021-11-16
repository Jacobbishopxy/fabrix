//! Fabrix prelude

pub use crate::core::{
    DataFrame, Date, DateTime, Decimal, FieldInfo, Row, Series, Time, Uuid, Value, ValueType, D1,
    D2,
};

// sources: db
#[cfg(feature = "db")]
pub use crate::sources::db::error::{DbError, DbResult};
#[cfg(feature = "db")]
pub use crate::sources::db::sql_builder::adt;
#[cfg(feature = "db")]
pub use crate::sources::db::sql_executor::{ConnInfo, Executor as SqlExecutor};

// sources: file
#[cfg(feature = "file")]
pub use crate::sources::file::error::{FlError, FlResult};

// sources: bson
#[cfg(feature = "bson")]
pub use crate::sources::bson::error::{BsError, BsResult};
