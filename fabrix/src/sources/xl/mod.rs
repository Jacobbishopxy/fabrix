//! Xl
//!
//! Xl is a module for reading and writing Excel files.

pub mod executor;
mod util;
pub mod workbook;
pub(crate) mod worksheet;

pub use executor::{AsyncConsumeFn, ConvertFn, SyncConsumeFn, XlConsumer, XlExecutor, XlSource};
pub use workbook::Workbook;
pub(crate) use worksheet::SheetReader;
pub use worksheet::{Cell, ChunkCell, Row, RowIter, VecCell, Worksheet};

/// Date system, used to determine the date format from an Excel file.
///
/// Excel spreadsheets support two different date systems:
///
/// - the 1900 date system
/// - the 1904 date system
///
/// Under the 1900 system, the first date supported is January 1, 1900. Under the 1904 system, the
/// first date supported is January 1, 1904. Under either system, a date is represented as the
/// number of days that have elapsed since the first date. So you can't actually tell what date a
/// number represents unless you also know the date system the spreadsheet uses.
///
/// See <https://tinyurl.com/4syjy6cw> for more information.
#[derive(Debug)]
pub(crate) enum DateSystem {
    V1900,
    V1904,
}

/// `ExcelValue` is the enum that holds the equivalent "rust value" of a `Cell`s "raw_value."
#[derive(Debug, PartialEq)]
pub enum ExcelValue<'a> {
    Bool(bool),
    Number(f64),
    String(std::borrow::Cow<'a, str>),
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
    DateTime(chrono::NaiveDateTime),
    None,
    Error(String),
}

impl std::fmt::Display for ExcelValue<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ExcelValue::Bool(b) => write!(f, "{}", b),
            ExcelValue::Date(d) => write!(f, "{}", d),
            ExcelValue::DateTime(d) => write!(f, "{}", d),
            ExcelValue::Error(e) => write!(f, "#{}", e),
            ExcelValue::None => write!(f, ""),
            ExcelValue::Number(n) => write!(f, "{}", n),
            ExcelValue::String(s) => write!(f, "\"{}\"", s),
            ExcelValue::Time(t) => write!(f, "\"{}\"", t),
        }
    }
}
