//! File Xl worksheet
//!
//! This module implements all the functionality specific to Excel worksheets.

use std::borrow::Cow;
use std::io::{BufReader, Read, Seek};

use quick_xml::events::Event;
use quick_xml::Reader;
use zip::read::ZipFile;

use super::{util, DateSystem, ExcelValue, XlWorkbook};
use crate::{XlError, XlResult};

/// a row of cells
#[derive(Debug)]
pub struct XlRow<'a> {
    pub data: Vec<XlCell<'a>>,
    pub num: usize,
}

impl<'a> XlRow<'a> {
    pub fn new(data: Vec<XlCell<'a>>, num: usize) -> XlRow<'a> {
        XlRow { data, num }
    }
}

/// minimum Excel unit: cell
#[derive(Debug)]
pub struct XlCell<'a> {
    /// The value you get by converting the raw_value (a string) into a Rust value
    pub value: ExcelValue<'a>,
    /// The formula (may be "empty") of the cell
    pub formula: String,
    /// What cell are we looking at? E.g., B3, A1, etc.
    pub reference: String,
    /// The cell style (e.g., the style you see in Excel by hitting Ctrl+1 and going to the "Number" tab).
    pub style: String,
    /// The type of cell as recorded by Excel (s = string using sharedStrings.xml, str = raw
    /// string, b = boolean, etc.). This may change from a `String` type to an `Enum` of some sorts
    /// in the future.
    pub cell_type: String,
    /// The raw string value recorded in the xml
    pub raw_value: String,
}

impl XlCell<'_> {
    /// return cell's row/column coordinates
    pub fn coordinates(&self) -> (u16, u32) {
        // let (col, row) = split_cell_reference(&self.reference);
        let (col, row) = {
            let r = &self.reference;
            let mut end = 0;
            for (i, c) in r.chars().enumerate() {
                if !c.is_ascii_alphabetic() {
                    end = i;
                    break;
                }
            }
            (&r[..end], &r[end..])
        };
        let col = util::col2num(col).unwrap();
        let row = row.parse().unwrap();
        (col, row)
    }
}

impl std::fmt::Display for XlCell<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
}

/// Excel worksheet reader
///
/// The `SheetReader` is used in a `RowIter` to navigate a worksheet. It contains a pointer to the
/// worksheet `ZipFile` in the xlsx file, the list of strings used in the workbook, the styles used
/// in the workbook, and the date system of the workbook. None of these fields are "public," but
/// must be provided through the `SheetReader::new` method. See that method for documentation of
/// each item.
pub(crate) struct SheetReader<'a> {
    reader: Reader<BufReader<ZipFile<'a>>>,
    strings: &'a [String],
    styles: &'a [String],
    date_system: &'a DateSystem,
}

impl<'a> SheetReader<'a> {
    /// Create a new `SheetReader`. The parameters are:
    ///
    /// - The `reader` should be a reader object pointing to the sheets xml within the zip file.
    /// - The `strings` argument should be reference to the vector of strings used in the xlsx. As
    ///   background, xlsx files do not store strings directly in each spreadsheet's xml file.
    ///   Instead, there is a special file that contains all the strings in the workbook that
    ///   basically boils down to a big list of strings. Whenever a string is needed in a
    ///   particular worksheet, the xml has the index of the string in that file. So we need this
    ///   information to print out any string values in a worksheet.
    /// - The `styles` are used to determine the data type (primarily for dates). While each cell
    ///   has a 'cell type,' dates are a little trickier to get right. So we use the style
    ///   information when we can.
    /// - Lastly, the `date_system` is used to determine what date we are looking at for cells that
    ///   contain date values. See the documentation for the `DateSystem` enum for more
    ///   information.
    pub(crate) fn new(
        reader: Reader<BufReader<ZipFile<'a>>>,
        strings: &'a [String],
        styles: &'a [String],
        date_system: &'a DateSystem,
    ) -> SheetReader<'a> {
        Self {
            reader,
            strings,
            styles,
            date_system,
        }
    }
}

/// get how many rows and columns has used in a worksheet
///
/// find the number of rows and columns used in a particular worksheet. takes the workbook xlsx
/// location as its first parameter, and the location of the worksheet in question (within the zip)
/// as the second parameter. Returns a tuple of (rows, columns) in the worksheet.
fn used_area(range: &str) -> (u32, u16) {
    let mut end: isize = -1;
    for (i, c) in range.chars().enumerate() {
        if c == ':' {
            end = i as isize;
            break;
        }
    }
    if end == -1 {
        (0, 0)
    } else {
        let end_range = &range[end as usize..];
        let mut end = 0;

        for (i, c) in end_range[1..].chars().enumerate() {
            if !c.is_ascii_alphabetic() {
                end = i + 1;
                break;
            }
        }

        let col = util::col2num(&end_range[1..end]).unwrap();
        let row: u32 = end_range[end..].parse().unwrap();
        (row, col)
    }
}

/// main struct in this file, storing all the data in a worksheet
///
/// The Worksheet is the primary object in this module since this is where most of the valuable
/// data is. See the methods below for how to use.
#[derive(Debug)]
pub struct XlWorksheet {
    pub name: String,
    pub position: u8,
    relationship_id: String,
    target: String,
    sheet_id: u8,
}

impl XlWorksheet {
    /// constructor
    ///
    /// Create a new worksheet. Note that this method will probably not be called directly.
    /// Instead, you'll normally get a worksheet from a `Workbook` object. E.g.,:
    ///
    ///     use xl::{Workbook, Worksheet};
    ///
    ///     let mut wb = Workbook::open("tests/data/Book1.xlsx").unwrap();
    ///     let sheets = wb.sheets();
    ///     let ws = sheets.get("Time");
    ///     assert!(ws.is_some());
    pub fn new(
        name: String,
        position: u8,
        relationship_id: String,
        target: String,
        sheet_id: u8,
    ) -> Self {
        XlWorksheet {
            name,
            position,
            relationship_id,
            target,
            sheet_id,
        }
    }

    /// Obtain a `RowIter` for this worksheet (that is in `workbook`). This is, arguably, the main
    /// part of the library. You use this method to iterate through all the values in this sheet.
    /// The simplest thing you can do is print the values out (which is what `xlcat` does), but you
    /// could do more if you wanted.
    ///
    /// # Example usage
    ///
    ///     use xl::{Workbook, Worksheet, ExcelValue};
    ///
    ///     let mut wb = Workbook::open("tests/data/Book1.xlsx").unwrap();
    ///     let sheets = wb.sheets();
    ///     let ws = sheets.get("Sheet1").unwrap();
    ///     let mut rows = ws.rows(&mut wb);
    ///     let row1 = rows.next().unwrap();
    ///     assert_eq!(row1[0].raw_value, "1");
    ///     assert_eq!(row1[1].value, ExcelValue::Number(2f64));
    pub fn rows<'a, READER: Read + Seek>(
        &self,
        workbook: &'a mut XlWorkbook<READER>,
    ) -> XlResult<RowIter<'a>> {
        let reader = workbook.sheet_reader(&self.target)?;
        Ok(RowIter {
            worksheet_reader: reader,
            want_row: 1,
            next_row: None,
            num_cols: 0,
            num_rows: 0,
            done_file: false,
            error: None,
        })
    }

    pub fn relationship_id(&self) -> &str {
        &self.relationship_id
    }

    pub fn sheet_id(&self) -> u8 {
        self.sheet_id
    }
}

pub struct RowIter<'a> {
    worksheet_reader: SheetReader<'a>,
    want_row: usize,
    next_row: Option<XlRow<'a>>,
    num_rows: u32,
    num_cols: u16,
    done_file: bool,
    pub error: Option<XlError>,
}

impl<'a> Iterator for RowIter<'a> {
    type Item = XlRow<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        // the xml in the xlsx file will not contain elements for empty rows. So
        // we need to "simulate" the empty rows since the user expects to see
        // them when they iterate over the worksheet.
        if let Some(XlRow { data: _, num }) = &self.next_row {
            // since we are currently buffering a row, we know we will either return it or a
            // "simulated" (i.e., emtpy) row. So we grab the current row and update the fact that
            // we will soon want a new row. We then figure out if we have the row we want or if we
            // need to keep spitting out empty rows.
            let current_row = self.want_row;
            self.want_row += 1;
            if *num == current_row {
                // we finally hit the row we were looking for, so we reset the buffer and return
                // the row that was sitting in it.
                let mut r = None;
                std::mem::swap(&mut r, &mut self.next_row);
                return r;
            } else {
                // otherwise, we must still be sitting behind the row we want. So we return an
                // empty row to simulate the row that exists in the spreadsheet.
                return empty_row(self.num_cols, current_row);
            }
        } else if self.done_file && self.want_row < self.num_rows as usize {
            self.want_row += 1;
            return empty_row(self.num_cols, self.want_row - 1);
        }

        let mut buf = Vec::new();
        let reader = &mut self.worksheet_reader.reader;
        let strings = self.worksheet_reader.strings;
        let styles = self.worksheet_reader.styles;
        let date_system = self.worksheet_reader.date_system;
        let next_row = {
            let mut row: Vec<XlCell> = Vec::with_capacity(self.num_cols as usize);
            let mut in_cell = false;
            let mut in_value = false;
            let mut c = new_cell();
            let mut this_row: usize = 0;
            loop {
                match reader.read_event(&mut buf) {
                    /* may be able to get a better estimate for the used area */
                    Ok(Event::Empty(ref e)) if e.name() == b"dimension" => {
                        if let Some(used_area_range) = util::get(e.attributes(), b"ref") {
                            if used_area_range != "A1" {
                                let (rows, cols) = used_area(&used_area_range);
                                self.num_cols = cols;
                                self.num_rows = rows;
                            }
                        }
                    }
                    /* -- end search for used area */
                    Ok(Event::Start(ref e)) if e.name() == b"row" => {
                        this_row = util::get(e.attributes(), b"r").unwrap().parse().unwrap();
                    }
                    Ok(Event::Start(ref e)) if e.name() == b"c" => {
                        in_cell = true;
                        e.attributes().for_each(|a| {
                            let a = a.unwrap();
                            if a.key == b"r" {
                                c.reference = util::attr_value(&a);
                            }
                            if a.key == b"t" {
                                c.cell_type = util::attr_value(&a);
                            }
                            if a.key == b"s" {
                                if let Ok(num) = util::attr_value(&a).parse::<usize>() {
                                    if let Some(style) = styles.get(num) {
                                        c.style = style.to_string();
                                    }
                                }
                            }
                        });
                    }
                    Ok(Event::Start(ref e)) if e.name() == b"v" => {
                        in_value = true;
                    }
                    // note: because v elements are children of c elements,
                    // need this check to go before the 'in_cell' check
                    Ok(Event::Text(ref e)) if in_value => {
                        c.raw_value = e.unescape_and_decode(reader).unwrap();
                        c.value = match &c.cell_type[..] {
                            "s" => {
                                if let Ok(pos) = c.raw_value.parse::<usize>() {
                                    let s = &strings[pos];
                                    ExcelValue::String(Cow::Borrowed(s))
                                } else {
                                    ExcelValue::String(Cow::Owned(c.raw_value.clone()))
                                }
                            }
                            "str" => ExcelValue::String(Cow::Owned(c.raw_value.clone())),
                            "b" => {
                                if c.raw_value == "0" {
                                    ExcelValue::Bool(false)
                                } else {
                                    ExcelValue::Bool(true)
                                }
                            }
                            "bl" => ExcelValue::None,
                            "e" => ExcelValue::Error(c.raw_value.to_string()),
                            _ if is_date(&c) => {
                                let num = c.raw_value.parse::<f64>().unwrap();
                                match util::excel_number_to_date(num, date_system) {
                                    Ok(dv) => match dv {
                                        util::DateConversion::Date(date) => ExcelValue::Date(date),
                                        util::DateConversion::DateTime(date) => {
                                            ExcelValue::DateTime(date)
                                        }
                                        util::DateConversion::Time(time) => ExcelValue::Time(time),
                                        util::DateConversion::Number(num) => {
                                            ExcelValue::Number(num as f64)
                                        }
                                    },
                                    Err(e) => {
                                        self.error = Some(e);
                                        break None;
                                    }
                                }
                            }
                            _ => ExcelValue::Number(c.raw_value.parse::<f64>().unwrap()),
                        }
                    }
                    Ok(Event::Text(ref e)) if in_cell => {
                        let txt = e.unescape_and_decode(reader).unwrap();
                        c.formula.push_str(&txt)
                    }
                    Ok(Event::End(ref e)) if e.name() == b"v" => {
                        in_value = false;
                    }
                    Ok(Event::End(ref e)) if e.name() == b"c" => {
                        if let Some(prev) = row.last() {
                            let (mut last_col, _) = prev.coordinates();
                            let (this_col, this_row) = c.coordinates();
                            while this_col > last_col + 1 {
                                let mut cell = new_cell();
                                cell.reference
                                    .push_str(&util::num2col(last_col + 1).unwrap());
                                cell.reference.push_str(&this_row.to_string());
                                row.push(cell);
                                last_col += 1;
                            }
                            row.push(c);
                        } else {
                            let (this_col, this_row) = c.coordinates();
                            for n in 1..this_col {
                                let mut cell = new_cell();
                                cell.reference.push_str(&util::num2col(n).unwrap());
                                cell.reference.push_str(&this_row.to_string());
                                row.push(cell);
                            }
                            row.push(c);
                        }
                        c = new_cell();
                        in_cell = false;
                    }
                    Ok(Event::End(ref e)) if e.name() == b"row" => {
                        self.num_cols = std::cmp::max(self.num_cols, row.len() as u16);
                        while row.len() < self.num_cols as usize {
                            let mut cell = new_cell();
                            cell.reference
                                .push_str(&util::num2col(row.len() as u16 + 1).unwrap());
                            cell.reference.push_str(&this_row.to_string());
                            row.push(cell);
                        }
                        let next_row = Some(XlRow::new(row, this_row));
                        if this_row == self.want_row {
                            break next_row;
                        } else {
                            self.next_row = next_row;
                            break empty_row(self.num_cols, self.want_row);
                        }
                    }
                    Ok(Event::Eof) => break None,
                    Err(e) => {
                        self.error = Some(XlError::new_common_error(format!(
                            "Error at position {}: {:?}",
                            reader.buffer_position(),
                            e
                        )));
                        break None;
                    }
                    _ => (),
                }
                buf.clear();
            }
        };
        self.want_row += 1;
        if next_row.is_none() && self.want_row - 1 < self.num_rows as usize {
            self.done_file = true;
            return empty_row(self.num_cols, self.want_row - 1);
        }
        next_row
    }
}

/// create a default cell
fn new_cell() -> XlCell<'static> {
    XlCell {
        value: ExcelValue::None,
        formula: "".to_string(),
        reference: "".to_string(),
        style: "".to_string(),
        cell_type: "".to_string(),
        raw_value: "".to_string(),
    }
}

/// create an optional static Row
fn empty_row(num_cols: u16, this_row: usize) -> Option<XlRow<'static>> {
    let mut row = vec![];
    for n in 0..num_cols {
        let mut c = new_cell();
        c.reference.push_str(&util::num2col(n + 1).unwrap());
        c.reference.push_str(&this_row.to_string());
        row.push(c);
    }
    Some(XlRow::new(row, this_row))
}

fn is_date(cell: &XlCell) -> bool {
    let is_d = cell.style == "d";
    let is_like_d_and_not_like_red = cell.style.contains('d') && !cell.style.contains("Red");
    let is_like_m = cell.style.contains('m');
    if is_d || is_like_d_and_not_like_red || is_like_m {
        true
    } else {
        cell.style.contains('y')
    }
}
