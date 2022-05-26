//! Xl file to Database (Sqlite)
//!
//! XlReader + SqlWriter

use std::io::{Cursor, Write};

use actix_multipart::Multipart;
use actix_web::{HttpResponse, Result};
use fabrix::{sql_adt, DatabaseSqlite, SqlWriter, XlReader};
use futures::{StreamExt, TryStreamExt};

use crate::{get_current_time, AppError, UploadedFile, DB_CONN, FILE_TYPE_XL, MULTIPART_KEY_FILE};

pub async fn xl_to_db(mut payload: Multipart) -> Result<HttpResponse> {
    let mut result = Vec::<UploadedFile>::new();

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
            let table_name = name.replace(".xlsx", "");

            // turn buffer into fabrix struct
            let mut buff = Cursor::new(Vec::new());

            // write all bytes from multipart to buffer
            while let Some(Ok(chunk)) = field.next().await {
                buff.get_mut().write_all(&chunk)?;
            }

            // turn buffer into fabrix
            let mut reader = XlReader::new(buff).map_err(AppError::Fabrix)?;
            let fx = reader.finish(None).map_err(AppError::Fabrix)?;

            let mut writer = SqlWriter::<DatabaseSqlite>::new_from_str(DB_CONN)
                .await
                .map_err(AppError::Fabrix)?;

            // save fabrix into database
            writer
                .with_save_strategy(sql_adt::SaveStrategy::Replace)
                .finish(&table_name, fx)
                .await
                .map_err(AppError::Fabrix)?;

            let res = UploadedFile::new(name, get_current_time());
            result.push(res);
        }
    }

    Ok(HttpResponse::Ok().json(result))
}
