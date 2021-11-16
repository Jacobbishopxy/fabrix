//! Fabrix
//!
//! A connector, who links several resources together, whereby user can perform reading, transforming, operating
//! and writing data under a coordinated process.

#![feature(trait_upcasting)]
#![allow(incomplete_features)]

pub mod core;
pub mod errors;
pub mod macros;
pub mod prelude;
pub mod sources;

pub use errors::*;
pub use prelude::*;

pub(crate) use crate::sources::db::{DdlMutation, DdlQuery, DmlMutation, DmlQuery, SqlBuilder};
