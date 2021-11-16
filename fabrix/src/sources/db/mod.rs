//! Db
//! Used for database IO

mod error;
pub mod sql_builder;
pub mod sql_executor;

pub use sql_builder::builder::SqlBuilder;
pub use sql_builder::interface::{DdlMutation, DdlQuery, DmlMutation, DmlQuery};
