//! Schema
//!
//! DataFrame Schema

use polars::prelude::Schema as PolarsSchema;

use crate::FieldInfo;

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
