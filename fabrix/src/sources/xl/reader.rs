//! Xl reader
//!
//! Reading Xl files

use std::{
    fs::File,
    io::{Cursor, Read, Seek},
};

use async_trait::async_trait;
use fabrix_core::{value, Fabrix, Value, D2};
use fabrix_xl::{ExcelValue, XlCell, XlConsumer, XlExecutor, XlSource, XlWorkbook};

use super::UNSUPPORTED_TYPE;
use crate::{FabrixError, FabrixResult, FromSource, ReadOptions};

// ================================================================================================
// Xl into Fabrix convertor implementation
// ================================================================================================

type XlFabrixExecutor<R> = XlExecutor<XlFabrix, (), R>;

#[derive(Default)]
struct XlFabrix {
    data: Option<Fabrix>,
}

impl XlFabrix {
    fn new() -> Self {
        Self { data: None }
    }

    fn transform_data(
        data: D2<Value>,
        is_column_wise: bool,
        has_header: bool,
    ) -> FabrixResult<Fabrix> {
        if is_column_wise {
            Ok(Fabrix::from_column_values(data, None, has_header)?)
        } else {
            Ok(Fabrix::from_row_values(data, None, has_header)?)
        }
    }

    fn store(&mut self, data: Fabrix) {
        self.data = Some(data);
    }
}

impl XlConsumer<()> for XlFabrix {
    type UnitOut = Value;
    type FinalOut = Fabrix;

    fn transform(cell: XlCell) -> Self::UnitOut {
        match cell.value {
            ExcelValue::Bool(v) => value!(v),
            ExcelValue::Number(v) => value!(v),
            ExcelValue::String(v) => value!(v.to_string()),
            ExcelValue::Date(v) => value!(v),
            ExcelValue::Time(v) => value!(v),
            ExcelValue::DateTime(v) => value!(v),
            ExcelValue::None => Value::Null,
            ExcelValue::Error(v) => Value::String(format!("error: {v}")),
        }
    }
}

// ================================================================================================
// Xl Reader
// ================================================================================================

/// Xl Reader
pub struct Reader<R: Read + Seek> {
    xl_reader: Option<XlFabrixExecutor<R>>,
    sheet_name: Option<String>,
    has_header: Option<bool>,
    is_column_wise: Option<bool>,
}

impl<R: Read + Seek> Reader<R> {
    pub fn new(reader: R) -> FabrixResult<Self> {
        Ok(Self {
            xl_reader: Some(XlExecutor::new_with_source(XlWorkbook::new(reader)?)),
            sheet_name: None,
            has_header: None,
            is_column_wise: None,
        })
    }

    pub fn has_reader(&self) -> bool {
        self.xl_reader.is_some()
    }

    pub fn with_sheet_name(&mut self, sheet_name: &str) -> &mut Self {
        self.sheet_name = Some(sheet_name.to_string());
        self
    }

    pub fn with_header(&mut self, has_header: bool) -> &mut Self {
        self.has_header = Some(has_header);
        self
    }

    pub fn with_column_wise(&mut self, is_column_wise: bool) -> &mut Self {
        self.is_column_wise = Some(is_column_wise);
        self
    }

    pub fn finish(&mut self, index: Option<usize>) -> FabrixResult<Fabrix> {
        let mut xl_reader = self
            .xl_reader
            .take()
            .ok_or(FabrixError::NotInitialized("XlReader"))?;

        let mut helper = XlFabrix::new();

        let sheet_name = self
            .sheet_name
            .take()
            .ok_or(FabrixError::NotSet("sheet name"))?;

        let has_header = self.has_header.take().unwrap_or(true);

        let is_column_wise = self.is_column_wise.take().unwrap_or(false);

        xl_reader.consume_fn_mut(
            None,
            &sheet_name,
            |d| {
                XlFabrix::transform_data(d, is_column_wise, has_header)
                    .map_err(|e| fabrix_xl::XlError::Unexpected(format!("fabrix error {:?}", e)))
            },
            |d| {
                helper.store(d);
                Ok(())
            },
        )?;

        let mut res = helper.data.take().unwrap();
        if let Some(index) = index {
            res.set_index_tag(index)?;
        }

        Ok(res)
    }
}

// ================================================================================================
// XlReader TryFrom XlSource
// ================================================================================================

impl TryFrom<XlSource> for Reader<File> {
    type Error = FabrixError;

    fn try_from(source: XlSource) -> Result<Self, Self::Error> {
        match source {
            XlSource::File(file) => Self::new(file),
            XlSource::Path(path) => Self::new(File::open(path)?),
            _ => Err(FabrixError::UnsupportedType(UNSUPPORTED_TYPE)),
        }
    }
}

impl TryFrom<XlSource> for Reader<Cursor<Vec<u8>>> {
    type Error = FabrixError;

    fn try_from(source: XlSource) -> Result<Self, Self::Error> {
        match source {
            XlSource::Buff(bytes) => Self::new(bytes),
            _ => Err(FabrixError::UnsupportedType(UNSUPPORTED_TYPE)),
        }
    }
}

// ================================================================================================
// Xl read options & FromSource impl
// ================================================================================================

#[derive(Default)]
pub struct XlReadOptions {
    pub sheet_name: Option<String>,
    pub has_header: Option<bool>,
    pub is_column_wise: Option<bool>,
    pub index: Option<usize>,
}

impl ReadOptions for XlReadOptions {
    fn source_type() -> &'static str {
        "xl"
    }
}

#[async_trait]
impl<'a, R> FromSource<'a, XlReadOptions> for Reader<R>
where
    R: Seek + Read + Send,
{
    async fn async_read<'o>(&mut self, options: &'o XlReadOptions) -> FabrixResult<Fabrix>
    where
        'o: 'a,
    {
        self.sync_read(options)
    }

    fn sync_read<'o>(&mut self, options: &'o XlReadOptions) -> FabrixResult<Fabrix>
    where
        'o: 'a,
    {
        let XlReadOptions {
            sheet_name,
            has_header,
            is_column_wise,
            index,
        } = options;

        if let Some(sheet_name) = sheet_name {
            self.with_sheet_name(sheet_name);
        }
        if let Some(has_header) = has_header {
            self.with_header(*has_header);
        }
        if let Some(is_column_wise) = is_column_wise {
            self.with_column_wise(*is_column_wise);
        }

        self.finish(*index)
    }
}

#[cfg(test)]
mod test_xl_reader {
    use super::*;

    const XL_FILE_PATH: &str = "../mock/test.xlsx";

    #[test]
    fn row_wise_file_read() {
        let mut reader: Reader<File> = XlSource::Path(XL_FILE_PATH.to_string()).try_into().unwrap();

        assert!(reader.has_reader());

        let foo = reader
            .with_header(true)
            .with_sheet_name("data")
            .finish(None);

        assert!(foo.is_ok());

        println!("foo:\n {:?}", foo.unwrap());

        assert!(!reader.has_reader());
    }

    #[test]
    fn col_wise_file_read() {
        let mut reader: Reader<File> = XlSource::Path(XL_FILE_PATH.to_string()).try_into().unwrap();

        assert!(reader.has_reader());

        let foo = reader
            .with_header(true)
            .with_column_wise(true)
            .with_sheet_name("data_t")
            .finish(Some(0));

        assert!(foo.is_ok());

        println!("foo:\n {:?}", foo.unwrap());

        assert!(!reader.has_reader());
    }
}
