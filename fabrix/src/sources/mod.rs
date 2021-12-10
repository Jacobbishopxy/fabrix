//! Fabrix sources

pub mod file_error;

#[cfg(feature = "bson")]
pub mod bson;
#[cfg(feature = "csv")]
pub mod csv;
#[cfg(feature = "json")]
pub mod json;
#[cfg(feature = "mongo")]
pub mod mongo;
#[cfg(feature = "parquet")]
pub mod parquet;
#[cfg(feature = "sql")]
pub mod sql;
#[cfg(feature = "xl")]
pub mod xl;
