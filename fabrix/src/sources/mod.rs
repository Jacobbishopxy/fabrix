//! Fabrix sources

pub mod error;

#[cfg(feature = "bson")]
pub mod bson;
#[cfg(feature = "csv")]
pub mod csv;
#[cfg(feature = "mgo")]
pub mod mgo;
#[cfg(feature = "parquet")]
pub mod parquet;
#[cfg(feature = "sql")]
pub mod sql;
#[cfg(feature = "xl")]
pub mod xl;
