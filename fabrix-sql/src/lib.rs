//! Fabrix SQL

pub mod builder;
pub mod error;
pub mod executor;

pub use builder::bd::SqlBuilder;
pub use builder::interface::{DdlMutation, DdlQuery, DmlMutation, DmlQuery};
pub use builder::sql_adt;
pub use error::{SqlError, SqlResult};
pub use executor::*;
