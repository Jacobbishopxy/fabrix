//! Fabrix
//!
//! A connector, who links several resources together, whereby user can perform reading, transforming, operating
//! and writing data under a coordinated process.

pub mod dispatcher;
pub mod errors;
pub mod prelude;
pub mod sources;

pub use prelude::*;
