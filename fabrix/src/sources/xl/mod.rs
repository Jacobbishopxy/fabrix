//! Xl
//!
//! Xl is a module for reading (writing is temporary unsupported) Excel files.

pub mod reader;

pub use reader::{Reader as XlReader, XlReadOptions};

pub(crate) const UNSUPPORTED_TYPE: &str = "Unsupported XlSource type";
