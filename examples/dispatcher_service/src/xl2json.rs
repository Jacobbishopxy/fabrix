//! Xl file to Json
//!
//! XlReader + JsonWriter

use std::{collections::HashMap, io::Cursor};

use actix_multipart::Multipart;
use actix_web::{HttpResponse, Result};
use fabrix::{JsonWriter, XlReader};
use futures::TryStreamExt;
use serde_json::Value;

use crate::{AppError, FILE_TYPE_XL, MULTIPART_KEY_FILE};

pub async fn xl_to_json(mut payload: Multipart) -> Result<HttpResponse> {
    let mut result = Vec::<HashMap<String, Value>>::new();

    while let Ok(Some(mut field)) = payload.try_next().await {
        // skip non-xl files
        if *field.content_type() != FILE_TYPE_XL {
            continue;
        }

        let cd = field.content_disposition();

        if let Some(MULTIPART_KEY_FILE) = cd.get_name() {
            let filename = cd
                .get_filename()
                .ok_or_else(|| AppError::Uncategorized("Filename not found".to_string()))?;
            let name = sanitize_filename::sanitize(filename);

            // turn buffer into fabrix struct
            let mut buff = Cursor::new(Vec::new());

            // write all bytes from multipart to buffer
            while let Some(Ok(chunk)) = field.next().await {
                buff.get_mut().write_all(&chunk)?;
            }

            // turn buffer into fabrix
            let mut reader = XlReader::new(buff)?;
            let fx = reader.finish(None).map_err(AppError::Fabrix)?;

            // turn fabrix into json
            let mut json = Cursor::new(Vec::new());
            let mut writer = JsonWriter::new(json.by_ref());
            writer
                .with_json_format(true)
                .finish(fx)
                .map_err(AppError::Fabrix)?;

            let json_str: Value =
                serde_json::from_slice(json.get_ref()).map_err(AppError::Serde)?;
            let mut res = HashMap::new();
            res.insert(name, json_str);
            result.push(res);
        }
    }

    Ok(HttpResponse::Ok().json(result))
}
