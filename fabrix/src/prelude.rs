//! Fabrix prelude
//!
//! This is the prelude for the Fabrix crate.

pub use super::errors::*;

pub use fabrix_core::*;

#[cfg(feature = "sql")]
pub use fabrix_sql::*;

#[cfg(feature = "xl")]
pub use fabrix_xl::*;

#[cfg(feature = "dync")]
pub use fabrix_dyn_conn::*;

// sources: db
#[cfg(feature = "sql")]
pub use crate::sources::sql::*;

// sources: csv
#[cfg(feature = "csv")]
pub use crate::sources::csv::*;

// sources: parquet
#[cfg(feature = "parquet")]
pub use crate::sources::parquet::*;

// sources: xl
#[cfg(feature = "xl")]
pub use crate::sources::xl::*;

// sources: bson
#[cfg(feature = "json")]
pub use crate::sources::json::*;

// sources: bson
#[cfg(feature = "bson")]
pub use crate::sources::bson::*;

// dispatcher
#[cfg(all(feature = "xl", feature = "json"))]
pub use crate::dispatcher::XlJson;
#[cfg(all(feature = "xl", feature = "sql"))]
pub use crate::dispatcher::{
    XlDbConvertor, XlDbExecutor, XlDbHelper, XlIndexSelection, XlToDbConsumer,
};

pub use crate::dispatcher::{Dispatcher, FromSource, IntoSource, ReadOptions, WriteOptions};
