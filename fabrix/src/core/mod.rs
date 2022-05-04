//! Fabrix core

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

pub(crate) use macros::*;
pub use util::IDX;
pub(crate) use util::*;

use polars::datatypes::Field as PolarsField;

/// field info: column name, column type & has null
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct FieldInfo {
    pub(crate) name: String,
    pub(crate) dtype: ValueType,
}

impl FieldInfo {
    pub fn new<T>(name: T, dtype: ValueType) -> Self
    where
        T: Into<String>,
    {
        let name = name.into();
        FieldInfo { name, dtype }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn dtype(&self) -> &ValueType {
        &self.dtype
    }
}

impl From<FieldInfo> for PolarsField {
    fn from(fi: FieldInfo) -> Self {
        PolarsField::new(&fi.name, fi.dtype.into())
    }
}
