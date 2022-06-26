//! Fabrix MongoDB

pub mod dr;
pub mod dy;
pub mod ec;
pub mod error;
pub mod indexes;

pub use bson::oid::ObjectId;
pub use dr::*;
pub use dy::*;
pub use ec::*;
pub use error::*;
pub use fabrix_mg_derive::CRUD;
pub use indexes::*;
