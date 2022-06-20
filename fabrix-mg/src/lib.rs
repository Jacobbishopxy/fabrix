//! Fabrix MongoDB

pub mod bd;
pub mod dr;
pub mod ec;
pub mod error;
pub mod indexes;

pub use bd::*;
pub use dr::*;
pub use ec::*;
pub use error::*;
pub use fabrix_mg_derive::CRUD;
pub use indexes::*;
