//! CSV Reader
//!
//! Reading CSV files.

use std::fs::File;
use std::io::Cursor;

use async_trait::async_trait;
use polars::io::mmap::MmapBytesReader;
use polars::io::RowCount;
use polars::prelude::{CsvReader, SerReader};

use super::{CsvSource, UNSUPPORTED_TYPE};
use crate::{Fabrix, FabrixError, FabrixResult, FromSource, ReadOptions, Schema, ValueTypes};

// ================================================================================================
// CSV Reader
// ================================================================================================

/// CSV Reader
///
/// Read csv files from `std::fs::File` or `std::io::Cursor<T>`.
///
/// Since all the methods provided by `CsvReader` are `mut self`, there is also an
/// alternative way to build a `Reader` by using unsafe code:
/// ```rust,ignore
/// pub struct Reader<'a, R: MmapBytesReader + 'a> {
///     csv_reader: CsvReader<'a, R>,
/// }
///
/// impl<'a, R: MmapBytesReader> Reader<'a, R> {
///     pub fn has_header(&mut self) -> &mut Self {
///         let mut inner = unsafe { std::ptr::read(&self.csv_reader) };
///         inner = inner.has_header(true);
///         unsafe { std::ptr::write(&mut self.csv_reader, inner) };
///         self
///     }
/// }
/// ```
pub struct Reader<'a, R: MmapBytesReader + 'a> {
    csv_reader: Option<CsvReader<'a, R>>,
}

impl<'a, R: MmapBytesReader> Reader<'a, R> {
    pub fn new(reader: R) -> Self {
        Self {
            csv_reader: Some(CsvReader::new(reader)),
        }
    }

    pub fn has_reader(&self) -> bool {
        self.csv_reader.is_some()
    }

    pub fn new_reader(&mut self, reader: R) -> &mut Self {
        self.csv_reader = Some(CsvReader::new(reader));
        self
    }

    pub fn with_header(&mut self, has_header: bool) -> &mut Self {
        self.csv_reader = self.csv_reader.take().map(|r| r.has_header(has_header));
        self
    }

    pub fn with_skip_rows_after_header(&mut self, offset: usize) -> &mut Self {
        self.csv_reader = self
            .csv_reader
            .take()
            .map(|r| r.with_skip_rows_after_header(offset));
        self
    }

    pub fn with_n_rows(&mut self, num_rows: usize) -> &mut Self {
        self.csv_reader = self
            .csv_reader
            .take()
            .map(|r| r.with_n_rows(Some(num_rows)));
        self
    }

    pub fn with_row_count(&mut self, name: &str, offset: usize) -> &mut Self {
        let name = name.to_string();
        let offset = offset as u64;
        self.csv_reader = self
            .csv_reader
            .take()
            .map(|r| r.with_row_count(Some(RowCount { name, offset })));
        self
    }

    pub fn with_ignore_parser_errors(&mut self, ignore: bool) -> &mut Self {
        self.csv_reader = self
            .csv_reader
            .take()
            .map(|r| r.with_ignore_parser_errors(ignore));
        self
    }

    pub fn with_skip_rows(&mut self, skip_rows: usize) -> &mut Self {
        self.csv_reader = self.csv_reader.take().map(|r| r.with_skip_rows(skip_rows));
        self
    }

    pub fn with_rechunk(&mut self, rechunk: bool) -> &mut Self {
        self.csv_reader = self.csv_reader.take().map(|r| r.with_rechunk(rechunk));
        self
    }

    pub fn with_delimiter(&mut self, delimiter: u8) -> &mut Self {
        self.csv_reader = self.csv_reader.take().map(|r| r.with_delimiter(delimiter));
        self
    }

    pub fn with_comment_char(&mut self, comment_char: u8) -> &mut Self {
        self.csv_reader
            .take()
            .map(|r| r.with_comment_char(Some(comment_char)));
        self
    }

    // schema must be a subset of the total schema
    pub fn with_dtypes(&mut self, schema: &'a Schema) -> &mut Self {
        self.csv_reader = self
            .csv_reader
            .take()
            .map(|r| r.with_dtypes(Some(schema.as_ref())));
        self
    }

    pub fn with_dtypes_slice(&mut self, dtypes: &'a ValueTypes) -> &mut Self {
        self.csv_reader = self
            .csv_reader
            .take()
            .map(|r| r.with_dtypes_slice(Some(dtypes.as_ref())));
        self
    }

    pub fn with_projection(&mut self, projection: Vec<usize>) -> &mut Self {
        self.csv_reader = self
            .csv_reader
            .take()
            .map(|r| r.with_projection(Some(projection)));
        self
    }

    pub fn finish(&mut self, index: Option<usize>) -> FabrixResult<Fabrix> {
        let reader = self
            .csv_reader
            .take()
            .ok_or(FabrixError::NotInitialized("CsvReader"))?;

        let df = reader.finish()?;

        if let Some(index) = index {
            Ok(Fabrix::new(df, index)?)
        } else {
            Ok(Fabrix::new_no_index(df))
        }
    }
}

// ================================================================================================
// CsvReader TryFrom CsvSource
// ================================================================================================

impl<'a> TryFrom<CsvSource<'a>> for Reader<'a, File> {
    type Error = FabrixError;

    fn try_from(source: CsvSource<'a>) -> FabrixResult<Self> {
        match source {
            CsvSource::File(file) => Ok(Self::new(file)),
            CsvSource::Path(path) => Ok(Self::new(File::open(path)?)),
            _ => Err(FabrixError::UnsupportedType(UNSUPPORTED_TYPE)),
        }
    }
}

impl<'a> TryFrom<CsvSource<'a>> for Reader<'a, Cursor<Vec<u8>>> {
    type Error = FabrixError;

    fn try_from(source: CsvSource) -> FabrixResult<Self> {
        match source {
            CsvSource::BuffRead(bytes) => Ok(Reader::new(bytes)),
            _ => Err(FabrixError::UnsupportedType(UNSUPPORTED_TYPE)),
        }
    }
}

// ================================================================================================
// Csv read options & FromSource impl
// ================================================================================================

#[derive(Default)]
pub struct CsvReadOptions {
    pub has_header: Option<bool>,
    pub skip_rows_after_header: Option<usize>,
    pub num_rows: Option<usize>,
    pub row_count: Option<(String, usize)>,
    pub ignore_parser_errors: Option<bool>,
    pub skip_rows: Option<usize>,
    pub rechunk: Option<bool>,
    pub delimiter: Option<u8>,
    pub comment_char: Option<u8>,
    pub dtypes: Option<Schema>,
    pub dtypes_slice: Option<ValueTypes>,
    pub projection: Option<Vec<usize>>,
    pub index: Option<usize>,
}

impl ReadOptions for CsvReadOptions {
    fn source_type() -> &'static str {
        "csv"
    }
}

#[async_trait]
impl<'a, R> FromSource<'a, CsvReadOptions> for Reader<'a, R>
where
    R: MmapBytesReader + 'a,
{
    async fn async_read<'o>(&mut self, options: &'o CsvReadOptions) -> FabrixResult<Fabrix>
    where
        'o: 'a,
    {
        self.sync_read(options)
    }

    fn sync_read<'o>(&mut self, options: &'o CsvReadOptions) -> FabrixResult<Fabrix>
    where
        'o: 'a,
    {
        let CsvReadOptions {
            has_header,
            skip_rows_after_header,
            num_rows,
            row_count,
            ignore_parser_errors,
            skip_rows,
            rechunk,
            delimiter,
            comment_char,
            dtypes,
            dtypes_slice,
            projection,
            index,
        } = options;

        if let Some(has_header) = has_header {
            self.with_header(*has_header);
        }
        if let Some(skip_rows_after_header) = skip_rows_after_header {
            self.with_skip_rows_after_header(*skip_rows_after_header);
        }
        if let Some(num_rows) = num_rows {
            self.with_n_rows(*num_rows);
        }
        if let Some((name, offset)) = row_count {
            self.with_row_count(name, *offset);
        }
        if let Some(ignore_parser_errors) = ignore_parser_errors {
            self.with_ignore_parser_errors(*ignore_parser_errors);
        }
        if let Some(skip_rows) = skip_rows {
            self.with_skip_rows(*skip_rows);
        }
        if let Some(rechunk) = rechunk {
            self.with_rechunk(*rechunk);
        }
        if let Some(delimiter) = delimiter {
            self.with_delimiter(*delimiter);
        }
        if let Some(comment_char) = comment_char {
            self.with_comment_char(*comment_char);
        }
        if let Some(dtypes) = dtypes {
            self.with_dtypes(dtypes);
        }
        if let Some(dtypes_slice) = dtypes_slice {
            self.with_dtypes_slice(dtypes_slice);
        }
        if let Some(projection) = projection {
            self.with_projection(projection.clone());
        }

        self.finish(*index)
    }
}

#[cfg(test)]
mod test_csv_reader {
    use std::io::Cursor;

    use super::*;
    use crate::{FieldInfo, ValueType};

    const CSV_FILE_PATH: &str = "../mock/test.csv";

    #[test]
    fn file_read() {
        let fi = vec![
            FieldInfo::new("id", ValueType::U32),
            FieldInfo::new("issued_times", ValueType::U8),
        ];
        let foo = Schema::from_field_infos(fi);

        let mut reader: Reader<File> = CsvSource::Path(CSV_FILE_PATH).try_into().unwrap();

        assert!(reader.has_reader());

        let foo = reader.with_dtypes(&foo).finish(None);

        assert!(foo.is_ok());

        println!("foo:\n{:?}", foo.unwrap());

        assert!(!reader.has_reader());
    }

    #[test]
    fn buff_read() {
        let mock_data = r#"
        id,first_name,last_name,email,gender,ip_address,company,birth,issued_date,issued_times
        1,Bartlett,Blythin,bblythin0@ebay.co.uk,Polygender,,Flashset,7/16/1985,,
        2,Aguie,Searchwell,asearchwell1@domainmarket.com,Agender,68.35.53.37,Vidoo,10/20/1989,,
        3,Bogey,Gajewski,bgajewski2@dailymail.co.uk,Polygender,121.199.95.125,Fiveclub,1/16/1995,4/5/2006,5
        4,William,Nelius,wnelius3@sina.com.cn,Bigender,172.34.97.121,Topiczoom,3/2/1991,12/27/2004,1
        5,Corney,Bresman,cbresman4@exblog.jp,Genderqueer,8.204.27.11,Skyba,9/26/1986,,
        6,Corine,Dowry,cdowry5@shutterfly.com,Agender,199.166.218.12,Rhycero,10/21/1984,,
        7,Gilda,Itzchaky,gitzchaky6@marriott.com,Genderqueer,87.69.205.125,Meetz,1/27/1983,,
        8,Morry,Liepina,mliepina7@youku.com,Agender,13.183.127.154,Brainlounge,12/7/1999,,
        9,Tresa,Jiracek,tjiracek8@devhub.com,Non-binary,42.168.32.144,Linkbuzz,9/19/1980,12/7/2003,2
        "#;

        let crs = Cursor::new(mock_data);

        let mut reader = Reader::new(crs);

        let foo = reader.finish(None);

        println!("{:?}", foo);
    }
}
