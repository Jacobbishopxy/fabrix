//! Fabrix Pipes

pub mod ds;
#[cfg(all(feature = "sql", feature = "xl"))]
pub mod xl_db;
#[cfg(all(feature = "xl", feature = "json"))]
pub mod xl_json;

pub use ds::*;
#[cfg(all(feature = "sql", feature = "xl"))]
pub use xl_db::{XlDbConvertor, XlDbExecutor, XlDbHelper, XlIndexSelection, XlToDbConsumer};
#[cfg(all(feature = "xl", feature = "json"))]
pub use xl_json::{XlJson, XlJsonConvertor, XlJsonExecutor};
