//! Xl util
//!
//! This module contains utility functions for working with Xl.

use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};
use quick_xml::events::attributes::{Attribute, Attributes};

use super::DateSystem;
use crate::{XlError, XlResult};

/// Xml attribute -> String
pub(crate) fn attr_value(a: &Attribute) -> String {
    String::from_utf8(a.value.to_vec()).unwrap()
}

/// Try Xml attribute -> Option<String>
pub(crate) fn get(attrs: Attributes, which: &[u8]) -> Option<String> {
    for attr in attrs {
        let a = attr.unwrap();
        if a.key == which {
            return Some(attr_value(&a));
        }
    }
    None
}

const XL_MAX_COL: u16 = 16384;
const XL_MIN_COL: u16 = 1;

/// Return column letter for column number `n`
pub(crate) fn num2col(n: u16) -> Option<String> {
    if !(XL_MIN_COL..=XL_MAX_COL).contains(&n) {
        return None;
    }
    let mut s = String::new();
    let mut n = n;
    while n > 0 {
        let r: u8 = ((n - 1) % 26).try_into().unwrap();
        n = (n - 1) / 26;
        s.push((65 + r) as char)
    }
    Some(s.chars().rev().collect::<String>())
}

/// Return column number for column letter `letter`
pub(crate) fn col2num(letter: &str) -> Option<u16> {
    let letter = letter.to_uppercase();
    let mut num: u16 = 0;
    for c in letter.chars() {
        if !('A'..='Z').contains(&c) {
            return None;
        }
        num = num * 26 + ((c as u16) - ('A' as u16)) + 1;
    }
    if !(XL_MIN_COL..=XL_MAX_COL).contains(&num) {
        return None;
    }
    Some(num)
}

/// date/time conversion
pub(crate) enum DateConversion {
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    Time(NaiveTime),
    Number(i64),
}

/// Excel number -> date/time
pub(crate) fn excel_number_to_date(
    number: f64,
    date_system: &DateSystem,
) -> XlResult<DateConversion> {
    let base = match date_system {
        DateSystem::V1900 => {
            let mut base = NaiveDate::from_ymd(1899, 12, 31).and_hms(0, 0, 0);
            if (number - 60.0).abs() < 0.0001 {
                return Err(XlError::Parsing(
                    "Bad date in Excel file - 2/29/1900 not valid",
                ));
            } else if number > 60.0 {
                base -= Duration::days(1)
            }
            base
        }
        DateSystem::V1904 => NaiveDate::from_ymd(1904, 1, 1).and_hms(0, 0, 0),
    };
    let days = number.trunc() as i64;
    if days < -693594 {
        return Ok(DateConversion::Number(days));
    }
    let partial_days = number - (days as f64);
    let seconds = (partial_days * 86400000.0).round() as i64;
    let milliseconds = Duration::milliseconds(seconds % 1000);
    let seconds = Duration::seconds(seconds / 1000);
    let date = base + Duration::days(days) + seconds + milliseconds;
    if days == 0 {
        Ok(DateConversion::Time(date.time()))
    } else if date.time() == NaiveTime::from_hms(0, 0, 0) {
        Ok(DateConversion::Date(date.date()))
    } else {
        Ok(DateConversion::DateTime(date))
    }
}
