//! Fabrix prelude
//!
//! This is the prelude for the Fabrix crate.

pub use super::errors::*;
pub use crate::core::{
    CoreError, CoreResult, DataFrame, Date, DateTime, Decimal, FieldInfo, Row, Series, Time, Uuid,
    Value, ValueType, D1, D2,
};

// sources: db
#[cfg(feature = "sql")]
pub use crate::sources::sql::{
    self,
    error::{SqlError, SqlResult},
};

// sources: file and its sub-modules
#[cfg(feature = "file")]
pub use crate::sources::file_error::{FlError, FlResult};
// sources: xl
#[cfg(feature = "xl")]
pub use crate::sources::xl;
// sources: bson
#[cfg(feature = "bson")]
pub use crate::sources::bson;

// dispatcher
#[cfg(feature = "sql")]
pub use crate::dispatcher::DispatcherDB;
