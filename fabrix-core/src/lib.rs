//! Fabrix core

#![feature(trait_upcasting)]
#![feature(specialization)]
#![allow(incomplete_features)]

pub mod error;
pub mod fabrix;
pub mod fmt;
pub(crate) mod macros;
pub mod namedrow;
pub mod row;
pub mod schema;
pub mod series;
pub mod util;
pub mod value;

pub use error::*;
pub use fabrix::*;
pub use namedrow::*;
pub use row::*;
pub use schema::*;
pub use series::*;
pub use value::*;

pub use chrono::{self, *};
pub(crate) use macros::*;
pub use polars;
pub use rust_decimal::{self, Decimal as RustDecimal};
pub use util::IDX;
pub(crate) use util::*;
pub use uuid::{self, Uuid as RustUuid};
