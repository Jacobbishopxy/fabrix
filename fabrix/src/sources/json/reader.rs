//! Json Reader
//!
//! Reading JSON into a Fabrix struct.

use std::fs::File;
use std::io::{BufReader, Cursor};

use async_trait::async_trait;
use polars::io::mmap::MmapBytesReader;
use polars::prelude::{JsonFormat, JsonReader, SerReader};

use crate::{Fabrix, FabrixError, FabrixResult, FromSource, JsonSource, ReadOptions, Schema};

use super::UNSUPPORTED_TYPE;

// ================================================================================================
// JSON Reader
// ================================================================================================

pub struct Reader<R: MmapBytesReader> {
    json_reader: Option<JsonReader<R>>,
}

impl<R: MmapBytesReader> Reader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            json_reader: Some(JsonReader::new(reader)),
        }
    }

    pub fn has_reader(&self) -> bool {
        self.json_reader.is_some()
    }

    pub fn new_reader(&mut self, reader: R) -> &mut Self {
        self.json_reader = Some(JsonReader::new(reader));
        self
    }

    pub fn with_schema(&mut self, schema: &Schema) -> &mut Self {
        self.json_reader = self
            .json_reader
            .take()
            .map(|r| r.with_schema(schema.as_ref()));
        self
    }

    pub fn with_infer_schema_len(&mut self, max_records: usize) -> &mut Self {
        self.json_reader = self
            .json_reader
            .take()
            .map(|r| r.infer_schema_len(Some(max_records)));
        self
    }

    pub fn with_batch_size(&mut self, batch_size: usize) -> &mut Self {
        self.json_reader = self
            .json_reader
            .take()
            .map(|r| r.with_batch_size(batch_size));
        self
    }

    pub fn with_projection(&mut self, projection: Vec<String>) -> &mut Self {
        self.json_reader = self
            .json_reader
            .take()
            .map(|r| r.with_projection(Some(projection)));
        self
    }

    pub fn with_json_format(&mut self, is_json: bool) -> &mut Self {
        let json_format = if is_json {
            JsonFormat::Json
        } else {
            JsonFormat::JsonLines
        };

        self.json_reader = self
            .json_reader
            .take()
            .map(|r| r.with_json_format(json_format));
        self
    }

    pub fn with_rechunk(&mut self, rechunk: bool) -> &mut Self {
        self.json_reader = self.json_reader.take().map(|r| r.set_rechunk(rechunk));
        self
    }

    pub fn finish(&mut self, index: Option<usize>) -> FabrixResult<Fabrix> {
        let reader = self
            .json_reader
            .take()
            .ok_or(FabrixError::NotInitialized("JsonReader"))?;

        let df = reader.finish()?;

        if let Some(index) = index {
            Ok(Fabrix::new(df, index)?)
        } else {
            Ok(Fabrix::new_no_index(df))
        }
    }
}

// ================================================================================================
// JsonReader TryFrom JsonSource
// ================================================================================================

impl<'a> TryFrom<JsonSource<'a>> for Reader<BufReader<File>> {
    type Error = FabrixError;

    fn try_from(source: JsonSource) -> Result<Self, Self::Error> {
        match source {
            JsonSource::File(file) => Ok(Reader::new(BufReader::new(file))),
            JsonSource::Path(path) => Ok(Reader::new(BufReader::new(File::open(path)?))),
            _ => Err(FabrixError::UnsupportedType(UNSUPPORTED_TYPE)),
        }
    }
}

impl<'a> TryFrom<JsonSource<'a>> for Reader<Cursor<Vec<u8>>> {
    type Error = FabrixError;

    fn try_from(source: JsonSource<'a>) -> Result<Self, Self::Error> {
        match source {
            JsonSource::BuffRead(bytes) => Ok(Reader::new(bytes)),
            _ => Err(FabrixError::UnsupportedType(UNSUPPORTED_TYPE)),
        }
    }
}

// ================================================================================================
// Json read options & FromSource impl
// ================================================================================================

#[derive(Default)]
pub struct JsonReadOptions {
    pub infer_schema_len: Option<usize>,
    pub batch_size: Option<usize>,
    pub projection: Option<Vec<String>>,
    pub format_is_json: Option<bool>,
    pub rechunk: Option<bool>,
    pub index: Option<usize>,
}

impl ReadOptions for JsonReadOptions {
    fn source_type() -> &'static str {
        "json"
    }
}

#[async_trait]
impl<'a, R> FromSource<'a, JsonReadOptions> for Reader<R>
where
    R: MmapBytesReader + Send,
{
    async fn async_read<'o>(&mut self, options: &'o JsonReadOptions) -> FabrixResult<Fabrix>
    where
        'o: 'a,
    {
        self.sync_read(options)
    }

    fn sync_read<'o>(&mut self, options: &'o JsonReadOptions) -> FabrixResult<Fabrix>
    where
        'o: 'a,
    {
        let JsonReadOptions {
            infer_schema_len,
            batch_size,
            projection,
            format_is_json,
            rechunk,
            index,
        } = options;

        if let Some(infer_schema_len) = infer_schema_len {
            self.with_infer_schema_len(*infer_schema_len);
        }
        if let Some(batch_size) = batch_size {
            self.with_batch_size(*batch_size);
        }
        if let Some(projection) = projection {
            self.with_projection(projection.clone());
        }
        if let Some(format_is_json) = format_is_json {
            self.with_json_format(*format_is_json);
        }
        if let Some(rechunk) = rechunk {
            self.with_rechunk(*rechunk);
        }

        self.finish(*index)
    }
}

#[cfg(test)]
mod test_json_reader {
    use super::*;

    const JSON_FILE_PATH: &str = "../mock/test.json";

    #[test]
    fn file_read() {
        let file = File::open(JSON_FILE_PATH).unwrap();
        let br = BufReader::new(file);

        let mut reader = Reader::new(br);
        let foo = reader.finish(None);

        assert!(foo.is_ok());

        println!("foo:\n{:?}", foo.unwrap());

        assert!(!reader.has_reader());
    }

    #[test]
    fn buff_read() {
        let mock_data = r#"
        [
            {
                "name": "foo",
                "age": 20
            },
            {
                "name": "bar",
                "age": 30
            }
        ]"#;

        let buff = Cursor::new(mock_data);

        let mut reader = Reader::new(buff);
        let foo = reader.finish(None);

        assert!(foo.is_ok());

        println!("foo:\n{:?}", foo.unwrap());

        assert!(!reader.has_reader());
    }
}
