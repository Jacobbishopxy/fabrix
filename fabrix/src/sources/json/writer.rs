//! JSON Writer
//!
//! Writing a Fabrix struct in to Json

use std::fs::File;
use std::io::{Cursor, Write};

use async_trait::async_trait;
use polars::prelude::{JsonFormat, JsonWriter, SerWriter};

use crate::{Fabrix, FabrixError, FabrixResult, IntoSource, JsonSource, WriteOptions};

use super::UNSUPPORTED_TYPE;

// ================================================================================================
// JSON Writer
// ================================================================================================

pub struct Writer<W: Write> {
    json_writer: Option<JsonWriter<W>>,
}

impl<W: Write> Writer<W> {
    pub fn new(writer: W) -> Self {
        Self {
            json_writer: Some(JsonWriter::new(writer)),
        }
    }

    pub fn has_writer(&self) -> bool {
        self.json_writer.is_some()
    }

    pub fn new_writer(&mut self, writer: W) -> &mut Self {
        self.json_writer = Some(JsonWriter::new(writer));
        self
    }

    pub fn with_json_format(&mut self, is_json: bool) -> &mut Self {
        let json_format = if is_json {
            JsonFormat::Json
        } else {
            JsonFormat::JsonLines
        };

        self.json_writer = self
            .json_writer
            .take()
            .map(|r| r.with_json_format(json_format));
        self
    }

    pub fn finish(&mut self, mut fabrix: Fabrix) -> FabrixResult<()> {
        let mut writer = self
            .json_writer
            .take()
            .ok_or_else(|| FabrixError::new_common_error("JsonWriter is not initialized"))?;

        writer.finish(&mut fabrix.data)?;
        Ok(())
    }
}

// ================================================================================================
// JsonWriter TryFrom JsonSource
// ================================================================================================

impl<'a> TryFrom<JsonSource<'a>> for Writer<File> {
    type Error = FabrixError;

    fn try_from(source: JsonSource<'a>) -> FabrixResult<Self> {
        match source {
            JsonSource::File(file) => Ok(Writer::new(file)),
            JsonSource::Path(path) => Ok(Writer::new(File::create(path)?)),
            _ => Err(FabrixError::new_common_error(UNSUPPORTED_TYPE)),
        }
    }
}

impl<'a> TryFrom<JsonSource<'a>> for Writer<&'a mut Cursor<Vec<u8>>> {
    type Error = FabrixError;

    fn try_from(source: JsonSource<'a>) -> FabrixResult<Self> {
        match source {
            JsonSource::BuffWrite(bytes) => Ok(Writer::new(bytes)),
            _ => Err(FabrixError::new_common_error(UNSUPPORTED_TYPE)),
        }
    }
}

// ================================================================================================
// JSON write options & IntoSource impl
// ================================================================================================

#[derive(Default)]
pub struct JsonWriteOptions {
    pub is_json: Option<bool>,
}

impl WriteOptions for JsonWriteOptions {
    fn source_type() -> &'static str {
        "json"
    }
}

#[async_trait]
impl<'a, W> IntoSource<'a, JsonWriteOptions> for Writer<W>
where
    W: Write + Send,
{
    async fn async_write<'o>(
        &mut self,
        fabrix: Fabrix,
        options: &'o JsonWriteOptions,
    ) -> FabrixResult<()>
    where
        'o: 'a,
    {
        self.sync_write(fabrix, options)
    }

    fn sync_write<'o>(&mut self, fabrix: Fabrix, options: &'o JsonWriteOptions) -> FabrixResult<()>
    where
        'o: 'a,
    {
        if let Some(is_json) = options.is_json {
            self.with_json_format(is_json);
        }

        self.finish(fabrix)
    }
}

#[cfg(test)]
mod test_json_writer {
    use std::fs::File;

    use serde_json::Value;

    use super::*;
    use crate::{date, datetime, fx};

    const JSON_FILE_PATH: &str = "../cache/write.json";

    #[test]
    fn file_write() {
        let mut writer = Writer::new(File::create(JSON_FILE_PATH).unwrap());
        assert!(writer.has_writer());

        let fx = fx![
            "id";
            "id" => [1, 2, 3],
            "name" => ["a", "b", "c"],
            "date" => [date!(2020,1,1), date!(2020,1,2), date!(2020,1,3)],
            "datetime" => [datetime!(2020,1,1,12,0,0), datetime!(2020,1,1,12,0,1), datetime!(2020,1,1,12,0,2)],
        ]
        .unwrap();

        let foo = writer.with_json_format(true).finish(fx);

        assert!(foo.is_ok());
        assert!(!writer.has_writer());
    }

    #[test]
    fn buff_write() {
        let mut crs = Cursor::new(Vec::new());

        let mut writer = Writer::new(crs.by_ref());
        assert!(writer.has_writer());

        let fx = fx![
            "id";
            "id" => [1, 2, 3],
            "name" => ["a", "b", "c"],
            "date" => [date!(2020,1,1), date!(2020,1,2), date!(2020,1,3)],
            "datetime" => [datetime!(2020,1,1,12,0,0), datetime!(2020,1,1,12,0,1), datetime!(2020,1,1,12,0,2)],
        ]
        .unwrap();

        let foo = writer.with_json_format(true).finish(fx);

        assert!(foo.is_ok());
        assert!(!writer.has_writer());

        println!("{:?}", crs.get_ref());

        // turn bytes into Json string
        let bar: Result<Value, serde_json::Error> = serde_json::from_slice(crs.get_ref());
        assert!(bar.is_ok());
        println!("{:?}", bar.unwrap().to_string());
    }
}
