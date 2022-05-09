//! RowFrame
//!
//! row-wised dataframe.
//! This is an experimental struct, and is not yet ready for use.

use crate::{Row, Schema, Value};

#[allow(dead_code)]
#[derive(Debug)]
pub struct RowFrame {
    pub(crate) schema: Schema,
    pub(crate) index: Option<usize>,
    pub(crate) rows: Vec<Row>,
}
