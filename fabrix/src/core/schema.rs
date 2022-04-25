//! Schema
//!
//! DataFrame Schema

use std::collections::HashSet;

use polars::prelude::Schema as PolarsSchema;

use crate::FieldInfo;

/// Schema
///
/// Schema is a subset of DataFrame FieldInfos collection.
pub struct Schema {
    inner: HashSet<FieldInfo>,
}

impl Schema {
    pub fn new<I: IntoIterator<Item = FieldInfo>>(field_infos: I) -> Self {
        Schema {
            inner: HashSet::from_iter(field_infos),
        }
    }

    pub fn insert(&mut self, field_info: FieldInfo) {
        self.inner.insert(field_info);
    }
}

impl From<Schema> for PolarsSchema {
    fn from(schema: Schema) -> Self {
        PolarsSchema::from(schema.inner)
    }
}
