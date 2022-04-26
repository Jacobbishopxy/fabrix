//! Fabrix prelude
//!
//! This is the prelude for the Fabrix crate.

pub use super::errors::*;
pub use crate::core::{
    CoreError, CoreResult, D1Value, D2Value, DataFrame, Date, DateTime, Decimal, FieldInfo, Row,
    Schema, Series, Time, Uuid, Value, ValueType, ValueTypes, D1, D2,
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
#[cfg(feature = "json")]
pub use crate::sources::json;
// sources: bson
#[cfg(feature = "bson")]
pub use crate::sources::bson;

// dispatcher
#[cfg(all(feature = "xl", feature = "bson"))]
pub use crate::dispatcher::XlBson;
#[cfg(all(feature = "xl", feature = "json"))]
pub use crate::dispatcher::XlJson;
#[cfg(all(feature = "xl", feature = "sql"))]
pub use crate::dispatcher::{
    XlDbConvertor, XlDbExecutor, XlDbHelper, XlIndexSelection, XlToDbConsumer,
};
