//! Xl
//!
//! Xl is a module for reading (writing is temporary unsupported) Excel files.

use std::borrow::Cow;
use std::fs::File;
use std::io::Cursor;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

pub mod executor;
pub mod reader;
mod util;
pub mod workbook;
pub(crate) mod worksheet;

pub use executor::{AsyncConsumeFP, ConvertFP, SyncConsumeFP, XlConsumer, XlExecutor};
pub use reader::*;
pub use workbook::Workbook;
pub(crate) use worksheet::SheetReader;
pub use worksheet::{Cell, ChunkCell, Row, RowIter, VecCell, Worksheet};

pub(crate) const UNSUPPORTED_TYPE: &str = "Unsupported XlSource type";

/// Xl file source type
#[derive(Debug)]
pub enum XlSource {
    File(File),
    Path(String),
    Url(String),
    Bytes(Cursor<Vec<u8>>),
}

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

// TODO: more variants, Int and etc. They can get conversion information from `Cell`'s `raw_value` & `cell_type`
/// `ExcelValue` is the enum that holds the equivalent "rust value" of a `Cell`s "raw_value."
#[derive(Debug, PartialEq)]
pub enum ExcelValue<'a> {
    Bool(bool),
    Number(f64),
    String(Cow<'a, str>),
    Date(NaiveDate),
    Time(NaiveTime),
    DateTime(NaiveDateTime),
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
