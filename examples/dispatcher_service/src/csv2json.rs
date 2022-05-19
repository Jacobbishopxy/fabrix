//! Csv file to Json
//!
//! Dispatcher<CsvReader, JsonWriter>

use std::collections::HashMap;
use std::io::{Cursor, Write};

use actix_multipart::Multipart;
use actix_web::{HttpResponse, Result};
use fabrix::{CsvReader, JsonWriter};
use futures::{StreamExt, TryStreamExt};
use serde_json::Value;

use crate::{AppError, MULTIPART_KEY_FILE};

// TODO: error handling

pub async fn csv_to_json(mut payload: Multipart) -> Result<HttpResponse> {
    let mut result = Vec::<HashMap<String, Value>>::new();

    while let Ok(Some(mut field)) = payload.try_next().await {
        let cd = field.content_disposition();

        if let Some(MULTIPART_KEY_FILE) = cd.get_name() {
            let filename = cd.get_filename().ok_or_else(|| AppError::Uncategorized {
                e: "Filename not found".to_string(),
            })?;
            let name = sanitize_filename::sanitize(filename);

            // turn buffer into fabrix struct
            let mut buff = Cursor::new(Vec::new());

            // write all bytes from multipart to buffer
            while let Some(Ok(chunk)) = field.next().await {
                buff.get_mut().write_all(&chunk)?;
            }

            let mut reader = CsvReader::new(buff);
            let fx = reader.finish(None).unwrap();

            // turn fabrix struct into json
            let mut json = Cursor::new(Vec::new());
            let mut writer = JsonWriter::new(json.by_ref());
            writer.with_json_format(true).finish(fx).unwrap();

            let json_str: Value = serde_json::from_slice(json.get_ref()).unwrap();
            let mut res = HashMap::new();
            res.insert(name, json_str);
            result.push(res);
        }
    }

    Ok(HttpResponse::Ok().json(result))
}
