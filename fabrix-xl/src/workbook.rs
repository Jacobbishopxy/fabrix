//! File Xl Workbook
//!
//! This module provides the functionality necessary to interact with an Excel workbook (i.e., the
//! entire file).

use std::collections::HashMap;
use std::io::{BufReader, Read, Seek};

use lazy_static::lazy_static;
use quick_xml::{events::Event, Reader};
use zip::ZipArchive;

use super::{util, DateSystem, SheetReader, XlWorksheet};
use crate::{XlError, XlResult};

/// The main struct of this module.
///
/// The Workbook is the primary object you will use in this module. The public interface allows you
/// to see the path of the workbook as well as its date system.
///
/// # Example usage:
///
///     use xl::Workbook;
///     let mut wb = Workbook::open("tests/data/Book1.xlsx").unwrap();
///
#[derive(Debug)]
pub struct XlWorkbook<READER: Read + Seek> {
    xls: ZipArchive<READER>,
    encoding: String,
    date_system: DateSystem,
    strings: Vec<String>,
    styles: Vec<String>,
}

/// A `SheetMap` is an object containing all the sheets in a given workbook. The only way to obtain
/// a `SheetMap` is from an `xl::Worksheet` object.
///
/// # Example usage:
///
///     use xl::{Workbook, Worksheet};
///
///     let mut wb = Workbook::open("tests/data/Book1.xlsx").unwrap();
///     let sheets = wb.sheets();
#[derive(Debug)]
pub struct SheetMap {
    sheets_by_name: HashMap<String, u8>,
    sheets_by_num: Vec<Option<XlWorksheet>>,
}

impl SheetMap {
    /// After you obtain a `SheetMap`, `by_name` gives you a list of sheets in the `SheetMap`
    /// ordered by their position in the workbook.
    ///
    /// Example usage:
    ///
    ///     use xl::{Workbook, Worksheet};
    ///
    ///     let mut wb = Workbook::open("tests/data/Book1.xlsx").unwrap();
    ///     let sheets = wb.sheets();
    ///     let sheet_names = sheets.by_name();
    ///     assert_eq!(sheet_names[2], "Time");
    ///
    /// Note that the returned array is **ZERO** based rather than **ONE** based like `get`. The
    /// reason for this is that we want `get` to act like VBA, but here we are only looking for a
    /// list of names so the `Option` type seemed like overkill. (We have `get` act like VBA
    /// because I expect people who will use this library will be very used to that "style" and may
    /// expect the same thing in this library. If it becomes an issue, we can change it later).
    pub fn by_name(&self) -> Vec<&str> {
        self.sheets_by_num
            .iter()
            .filter(|&s| s.is_some())
            .map(|s| &s.as_ref().unwrap().name[..])
            .collect()
    }
}

/// Struct to let you refer to sheets by name or by position (1-based).
pub enum SheetNameOrNum<'a> {
    Name(&'a str),
    Pos(usize),
}

/// Trait to make it easy to use `get` when trying to get a sheet. You will probably not use this
/// struct directly./// Trait to make it easy to use `get` when trying to get a sheet.
pub trait SheetAccessTrait {
    fn go(&self) -> SheetNameOrNum;
}

impl SheetAccessTrait for &str {
    fn go(&self) -> SheetNameOrNum {
        SheetNameOrNum::Name(self)
    }
}

impl SheetAccessTrait for &String {
    fn go(&self) -> SheetNameOrNum {
        SheetNameOrNum::Name(self)
    }
}

impl SheetAccessTrait for usize {
    fn go(&self) -> SheetNameOrNum {
        SheetNameOrNum::Pos(*self)
    }
}

impl SheetMap {
    /// An easy way to obtain a reference to a `Worksheet` within this `Workbook`. Note that we
    /// return an `Option` because the sheet you want may not exist in the workbook. Also note that
    /// when you try to `get` a worksheet by number (i.e., by its position within the workbook),
    /// the tabs use **1-based indexing** rather than 0-based indexing (like the rest of Rust and
    /// most of the programming world). This was an intentional design choice to make things
    /// consistent with VBA. It's possible it may change in the future, but it seems intuitive
    /// enough if you are familiar with VBA and Excel programming, so it may not.
    ///
    /// # Example usage
    ///
    ///     use xl::{Workbook, Worksheet};
    ///
    ///     let mut wb = Workbook::open("tests/data/Book1.xlsx").unwrap();
    ///     let sheets = wb.sheets();
    ///
    ///     // by sheet name
    ///     let time_sheet = sheets.get("Time");
    ///     assert!(time_sheet.is_some());
    ///
    ///     // unknown sheet name
    ///     let unknown_sheet = sheets.get("not in this workbook");
    ///     assert!(unknown_sheet.is_none());
    ///
    ///     // by position
    ///     let unknown_sheet = sheets.get(1);
    ///     assert_eq!(unknown_sheet.unwrap().name, "Sheet1");
    pub fn get<T: SheetAccessTrait>(&self, sheet: T) -> Option<&XlWorksheet> {
        let sheet = sheet.go();
        match sheet {
            SheetNameOrNum::Name(n) => match self.sheets_by_name.get(n) {
                Some(p) => self.sheets_by_num.get(*p as usize)?.as_ref(),
                None => None,
            },
            SheetNameOrNum::Pos(n) => self.sheets_by_num.get(n)?.as_ref(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.sheets_by_num.is_empty()
    }

    /// The number of active sheets in the workbook.
    ///
    /// # Example usage
    ///
    ///     use xl::{Workbook, Worksheet};
    ///
    ///     let mut wb = Workbook::open("tests/data/Book1.xlsx").unwrap();
    ///     let sheets = wb.sheets();
    ///     assert_eq!(sheets.len(), 4);
    pub fn len(&self) -> u8 {
        (self.sheets_by_num.len() - 1) as u8
    }
}

impl<READER> XlWorkbook<READER>
where
    READER: Read + Seek,
{
    /// xlsx zips contain an xml file that has a mapping of "ids" to "targets." The ids are used
    /// to uniquely identify sheets within the file. The targets have information on where the
    /// sheets can be found within the zip. This function returns a hashmap of id -> target so that
    /// you can quickly determine the name of the sheet xml file within the zip.
    fn rels(&mut self) -> XlResult<HashMap<String, String>> {
        let mut map = HashMap::new();

        match self.xls.by_name("xl/_rels/workbook.xml.rels") {
            Ok(rels) => {
                // Looking for tree structure like:
                //   Relationships
                //     Relationship(id = "abc", target = "def")
                //     Relationship(id = "ghi", target = "lkm")
                //     etc.
                //  Each relationship contains an id that is used to reference
                //  the sheet and a target which tells us where we can find the
                //  sheet in the zip file.
                //
                //  Uncomment the following line to print out a copy of what
                //  the xml looks like (will probably not be too big).
                // let _ = std::io::copy(&mut rels, &mut std::io::stdout());

                let reader = BufReader::new(rels);
                let mut reader = Reader::from_reader(reader);
                reader.trim_text(true);

                let mut buf = Vec::new();
                loop {
                    match reader.read_event(&mut buf) {
                        Ok(Event::Empty(ref e)) if e.name() == b"Relationship" => {
                            let mut id = String::new();
                            let mut target = String::new();
                            e.attributes().for_each(|a| {
                                let a = a.unwrap();
                                if a.key == b"Id" {
                                    id = util::attr_value(&a);
                                }
                                if a.key == b"Target" {
                                    target = util::attr_value(&a);
                                }
                            });
                            map.insert(id, target);
                        }
                        // exits the loop when reaching end of file
                        Ok(Event::Eof) => break,
                        Err(e) => return Err(XlError::Xml(e)),
                        // There are several other `Event`s we do not consider here
                        _ => (),
                    }
                    buf.clear();
                }

                Ok(map)
            }
            Err(_) => Ok(map),
        }
    }

    /// Return `SheetMap` of all sheets in this workbook. See `SheetMap` class and associated
    /// methods for more detailed documentation.
    pub fn sheets(&mut self) -> XlResult<SheetMap> {
        let rels = self.rels()?;
        let num_sheets = rels
            .iter()
            .filter(|(_, v)| v.starts_with("worksheet"))
            .count();
        let mut sheets = SheetMap {
            sheets_by_name: HashMap::new(),
            sheets_by_num: Vec::with_capacity(num_sheets + 1),
        };
        // never a "0" sheet (consistent with VBA)
        sheets.sheets_by_num.push(None);

        match self.xls.by_name("xl/workbook.xml") {
            Ok(wb) => {
                // let _ = std::io::copy(&mut wb, &mut std::io::stdout());
                let reader = BufReader::new(wb);
                let mut reader = Reader::from_reader(reader);
                reader.trim_text(true);

                let mut buf = Vec::new();
                let mut current_sheet_num: u8 = 0;

                loop {
                    match reader.read_event(&mut buf) {
                        Ok(Event::Empty(ref e)) if e.name() == b"sheet" => {
                            current_sheet_num += 1;
                            let mut name = String::new();
                            let mut id = String::new();
                            let mut num = 0;
                            e.attributes().for_each(|a| {
                                let a = a.unwrap();
                                if a.key == b"r:id" {
                                    id = util::attr_value(&a);
                                }
                                if a.key == b"name" {
                                    name = util::attr_value(&a);
                                }
                                if a.key == b"sheetId" {
                                    if let Ok(r) = util::attr_value(&a).parse() {
                                        num = r;
                                    }
                                }
                            });
                            sheets
                                .sheets_by_name
                                .insert(name.clone(), current_sheet_num);
                            let target = {
                                let s = rels.get(&id).unwrap();
                                if let Some(stripped) = s.strip_prefix('/') {
                                    stripped.to_string()
                                } else {
                                    "xl/".to_owned() + s
                                }
                            };
                            let ws = XlWorksheet::new(name, current_sheet_num, id, target, num);
                            sheets.sheets_by_num.push(Some(ws));
                        }
                        Ok(Event::Eof) => break,
                        Err(e) => return Err(XlError::Xml(e)),
                        _ => (),
                    }
                    buf.clear();
                }
                Ok(sheets)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Open an existing workbook (xlsx file). Returns a `Result` in case there is an error opening
    /// the workbook.
    ///
    /// # Example usage:
    ///
    ///     use xl::Workbook;
    ///
    ///     let mut wb = Workbook::open("tests/data/Book1.xlsx");
    ///     assert!(wb.is_ok());
    ///
    ///     // non-existant file
    ///     let mut wb = Workbook::open("Non-existant xlsx");
    ///     assert!(wb.is_err());
    ///
    ///     // non-xlsx file
    ///     let mut wb = Workbook::open("src/main.rs");
    ///     assert!(wb.is_err());
    pub fn new(file: READER) -> XlResult<Self> {
        match ZipArchive::new(file) {
            Ok(mut xls) => {
                let strings = strings(&mut xls)?;
                let styles = find_styles(&mut xls)?;
                let date_system = get_date_system(&mut xls)?;
                Ok(XlWorkbook {
                    xls,
                    encoding: "utf8".to_owned(),
                    date_system,
                    strings,
                    styles,
                })
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Simple method to print out all the inner files of the xlsx zip.
    pub fn print_contents(&mut self) {
        for i in 0..self.xls.len() {
            let file = self.xls.by_index(i).unwrap();
            let outpath = match file.enclosed_name() {
                Some(path) => path.to_owned(),
                None => continue,
            };

            if file.name().ends_with('/') {
                println!("File {}: \"{}\"", i, outpath.display());
            } else {
                println!(
                    "File {}: \"{}\" ({} bytes)",
                    i,
                    outpath.display(),
                    file.size()
                );
            }
        }
    }

    /// Create a SheetReader for the given worksheet. A `SheetReader` is a struct in the
    /// `xl::Worksheet` class that can be used to iterate over rows, etc. See documentation in the
    /// `xl::Worksheet` module for more information.
    pub(crate) fn sheet_reader<'a>(&'a mut self, zip_target: &str) -> XlResult<SheetReader<'a>> {
        let target = match self.xls.by_name(zip_target) {
            Ok(ws) => ws,
            Err(_) => return Err(XlError::SourceNotFound("worksheet")),
        };
        // let _ = std::io::copy(&mut target, &mut std::io::stdout());

        let reader = BufReader::new(target);
        let mut reader = Reader::from_reader(reader);
        reader.trim_text(true);
        Ok(SheetReader::new(
            reader,
            &self.strings,
            &self.styles,
            &self.date_system,
        ))
    }

    pub fn encoding(&self) -> &str {
        &self.encoding
    }
}

fn strings<READER: Read + Seek>(zip_file: &mut ZipArchive<READER>) -> XlResult<Vec<String>> {
    let mut strings = Vec::new();
    match zip_file.by_name("xl/sharedStrings.xml") {
        Ok(strings_file) => {
            let reader = BufReader::new(strings_file);
            let mut reader = Reader::from_reader(reader);
            reader.trim_text(true);
            let mut buf = Vec::new();
            let mut this_string = String::new();
            let mut preserve_space = false;

            loop {
                match reader.read_event(&mut buf) {
                    Ok(Event::Start(ref e)) if e.name() == b"t" => {
                        if let Some(att) = util::get(e.attributes(), b"xml:space") {
                            if att == "preserve" {
                                preserve_space = true;
                            } else {
                                preserve_space = false;
                            }
                        } else {
                            preserve_space = false;
                        }
                    }
                    Ok(Event::Text(ref e)) => {
                        this_string.push_str(&e.unescape_and_decode(&reader).unwrap()[..])
                    }
                    Ok(Event::Empty(ref e)) if e.name() == b"t" => strings.push("".to_owned()),
                    Ok(Event::End(ref e)) if e.name() == b"t" => {
                        if preserve_space {
                            strings.push(this_string.to_owned());
                        } else {
                            strings.push(this_string.trim().to_owned());
                        }
                        this_string = String::new();
                    }
                    Ok(Event::Eof) => break,
                    Err(e) => return Err(XlError::Xml(e)),
                    _ => (),
                }
                buf.clear();
            }
            Ok(strings)
        }
        Err(_) => Ok(strings),
    }
}

/// find the number of rows and columns used in a particular worksheet. takes the workbook xlsx
/// location as its first parameter, and the location of the worksheet in question (within the zip)
/// as the second parameter. Returns a tuple of (rows, columns) in the worksheet.
fn find_styles<READER: Read + Seek>(xlsx: &mut ZipArchive<READER>) -> XlResult<Vec<String>> {
    let mut styles = Vec::new();
    let mut number_formats = standard_styles();
    let styles_xml = match xlsx.by_name("xl/styles.xml") {
        Ok(s) => s,
        Err(_) => return Ok(styles),
    };
    // let _ = std::io::copy(&mut styles_xml, &mut std::io::stdout());

    let reader = BufReader::new(styles_xml);
    let mut reader = Reader::from_reader(reader);
    reader.trim_text(true);
    let mut buf = Vec::new();
    let mut record_styles = false;
    loop {
        match reader.read_event(&mut buf) {
            Ok(Event::Empty(ref e)) if e.name() == b"numFmt" => {
                let id = util::get(e.attributes(), b"numFmtId").unwrap();
                let code = util::get(e.attributes(), b"formatCode").unwrap();
                number_formats.insert(id, code);
            }
            Ok(Event::Start(ref e)) if e.name() == b"cellXfs" => {
                // Section 2.1.589 Part 1 Section 18.3.1.4, c (Cell)
                // Item g. states that Office specifies that @s indexes into the cellXfs collection
                // in the style part. See https://tinyurl.com/yju9a6ox for more information.
                record_styles = true;
            }
            Ok(Event::End(ref e)) if e.name() == b"cellXfs" => record_styles = false,
            Ok(Event::Start(ref e)) | Ok(Event::Empty(ref e))
                if record_styles && e.name() == b"xf" =>
            {
                let id = util::get(e.attributes(), b"numFmtId").unwrap();
                if number_formats.contains_key(&id) {
                    styles.push(number_formats.get(&id).unwrap().to_string());
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(XlError::Xml(e)),
            _ => (),
        }
        buf.clear();
    }
    Ok(styles)
}

lazy_static! {
    static ref STANDARD_STYLES: HashMap<&'static str, &'static str> = vec![
        ("0", "General"),
        ("1", "0"),
        ("2", "0.00"),
        ("3", "#,##0"),
        ("4", "#,##0.00"),
        ("9", "0%"),
        ("10", "0.00%"),
        ("11", "0.00E+00"),
        ("12", "# ?/?"),
        ("13", "# ??/??"),
        ("14", "mm-dd-yy"),
        ("15", "d-mmm-yy"),
        ("16", "d-mmm"),
        ("17", "mmm-yy"),
        ("18", "h:mm AM/PM"),
        ("19", "h:mm:ss AM/PM"),
        ("20", "h:mm"),
        ("21", "h:mm:ss"),
        ("22", "m/d/yy h:mm"),
        ("37", "#,##0 ;(#,##0)"),
        ("38", "#,##0 ;[Red](#,##0)"),
        ("39", "#,##0.00;(#,##0.00)"),
        ("40", "#,##0.00;[Red](#,##0.00)"),
        ("45", "mm:ss"),
        ("46", "[h]:mm:ss"),
        ("47", "mmss.0"),
        ("48", "##0.0E+0"),
        ("49", "@"),
    ]
    .into_iter()
    .collect();
}

/// Return hashmap of standard styles (ISO/IEC 29500:2011 in Part 1, section 18.8.30)
fn standard_styles() -> HashMap<String, String> {
    let mut styles = HashMap::new();
    for (id, code) in STANDARD_STYLES.iter() {
        styles.insert(id.to_string(), code.to_string());
    }
    styles
}

fn get_date_system<READER: Read + Seek>(xlsx: &mut ZipArchive<READER>) -> XlResult<DateSystem> {
    match xlsx.by_name("xl/workbook.xml") {
        Ok(wb) => {
            let reader = BufReader::new(wb);
            let mut reader = Reader::from_reader(reader);
            reader.trim_text(true);
            let mut buf = Vec::new();
            loop {
                match reader.read_event(&mut buf) {
                    Ok(Event::Empty(ref e)) if e.name() == b"workbookPr" => {
                        if let Some(system) = util::get(e.attributes(), b"date1904") {
                            if system == "1" {
                                break Ok(DateSystem::V1904);
                            }
                        }
                        break Ok(DateSystem::V1900);
                    }
                    Ok(Event::Eof) => break Ok(DateSystem::V1900),
                    Err(e) => return Err(XlError::Xml(e)),
                    _ => (),
                }
                buf.clear();
            }
        }
        Err(_) => Err(XlError::SourceNotFound("xl/workbook.xml")),
    }
}
