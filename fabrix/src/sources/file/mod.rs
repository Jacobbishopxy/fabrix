//! File
//! Used for file IO
//!
//! Codes in `workbook.rs` & `worksheet.rs` are copied from [xlprotips/xl](https://github.com/xlprotips/xl/)

pub mod csv;
pub mod error;
pub mod parquet;
pub mod xl;

pub use error::*;
pub use xl::*;
