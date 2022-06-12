//! Schema
//!
//! DataFrame Schema

use polars::datatypes::Field as PolarsField;
use polars::prelude::Schema as PolarsSchema;

use crate::ValueType;

/// field info: column name, column type & has null
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct FieldInfo {
    pub name: String,
    pub dtype: ValueType,
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

/// Schema
///
/// Schema is a subset of DataFrame FieldInfos collection.
#[derive(PartialEq, Eq, Clone, Default, Debug)]
pub struct Schema(PolarsSchema);

impl Schema {
    pub fn new() -> Self {
        Self(PolarsSchema::new())
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self(PolarsSchema::with_capacity(capacity))
    }

    pub fn from_field_infos<I: IntoIterator<Item = FieldInfo>>(field_infos: I) -> Self {
        Self(PolarsSchema::from(Vec::<FieldInfo>::from_iter(field_infos)))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn rename(&mut self, old_name: &str, new_name: &str) {
        self.0.rename(old_name, new_name.to_owned());
    }

    pub fn insert(&mut self, index: usize, field_info: FieldInfo) {
        let FieldInfo { name, dtype } = field_info;
        self.0.insert_index(index, name, dtype.into());
    }

    pub fn append(&mut self, field_info: FieldInfo) {
        let FieldInfo { name, dtype } = field_info;
        self.0.with_column(name, dtype.into());
    }

    pub fn extend(&mut self, other: Self) {
        self.0.merge(other.0);
    }

    pub fn get(&self, index: usize) -> Option<FieldInfo> {
        self.0.get_index(index).map(|(name, dtype)| FieldInfo {
            name: name.clone(),
            dtype: dtype.clone().into(),
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = FieldInfo> + '_ {
        self.0.iter().map(|(name, dtype)| FieldInfo {
            name: name.clone(),
            dtype: dtype.clone().into(),
        })
    }
}

impl AsRef<PolarsSchema> for Schema {
    fn as_ref(&self) -> &PolarsSchema {
        &self.0
    }
}

impl From<Schema> for PolarsSchema {
    fn from(schema: Schema) -> Self {
        schema.0
    }
}

impl From<PolarsSchema> for Schema {
    fn from(schema: PolarsSchema) -> Self {
        Self(schema)
    }
}
