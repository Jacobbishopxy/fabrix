//! Fabrix core

pub mod dataframe;
pub(crate) mod macros;
pub mod row;
pub mod series;
pub mod util;
pub mod value;

pub use dataframe::*;
pub use row::*;
pub use series::*;
pub use value::*;

pub(crate) use macros::*;
pub use util::IDX;
pub(crate) use util::{cis_err, inf_err, oob_err, Stepper};

/// field info: column name, column type & has null
#[derive(Debug, Clone, PartialEq)]
pub struct FieldInfo {
    pub(crate) name: String,
    pub(crate) dtype: ValueType,
}

impl FieldInfo {
    pub fn new(name: String, dtype: ValueType) -> Self {
        FieldInfo { name, dtype }
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn dtype(&self) -> &ValueType {
        &self.dtype
    }
}
