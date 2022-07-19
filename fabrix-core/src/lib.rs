//! Fabrix core

#![feature(trait_upcasting)]
#![allow(incomplete_features)]

pub mod error;
pub mod fabrix;
pub mod fmt;
pub(crate) mod macros;
pub mod row;
pub mod rowframe;
pub mod schema;
pub mod series;
pub mod util;
pub mod value;

pub use error::*;
pub use fabrix::*;
pub use row::*;
pub use rowframe::*;
pub use schema::*;
pub use series::*;
pub use value::*;

pub use chrono::{self, *};
pub(crate) use macros::*;
pub use polars;
pub use util::IDX;
pub(crate) use util::*;
