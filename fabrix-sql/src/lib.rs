//! Fabrix SQL

pub mod error;
pub mod sql_builder;
pub mod sql_executor;

pub use error::{SqlError, SqlResult};
pub use sql_builder::builder::SqlBuilder;
pub use sql_builder::interface::{DdlMutation, DdlQuery, DmlMutation, DmlQuery};
pub use sql_builder::sql_adt;
pub use sql_executor::*;
