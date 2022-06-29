//! Parquet to Mongo
//!
//! ParquetReader + MongoWriter

use std::io::{Cursor, Write};

use actix_multipart::Multipart;
use actix_web::{HttpResponse, Result};
use fabrix::{MongoWriter, ParquetReader};
use futures::{StreamExt, TryStreamExt};

use crate::{
    get_current_time, responses::UploadedFile, AppError, MG_COLLECTION, MG_CONN, MG_DATABASE,
    MULTIPART_KEY_FILE,
};

pub async fn parquet_to_mongo(mut payload: Multipart) -> Result<HttpResponse> {
    let mut result = Vec::<UploadedFile>::new();

    while let Ok(Some(mut field)) = payload.try_next().await {
        let cd = field.content_disposition();

        let fe = cd.get_filename().map(|n| n.split('.').collect::<Vec<_>>());

        if let Some(ss) = fe {
            if !matches!(ss.last(), Some(&"parquet")) {
                continue;
            }

            if let Some(MULTIPART_KEY_FILE) = cd.get_name() {
                let name = sanitize_filename::sanitize(cd.get_filename().unwrap());

                let mut buff = Cursor::new(Vec::new());

                while let Some(Ok(chunk)) = field.next().await {
                    buff.get_mut().write_all(&chunk)?;
                }

                let mut reader = ParquetReader::new(buff);
                let fx = reader.finish(None).map_err(AppError::Fabrix)?;

                let mut writer = MongoWriter::new(MG_CONN, MG_DATABASE, MG_COLLECTION)
                    .await
                    .map_err(AppError::Fabrix)?;
                writer.finish(fx).await.map_err(AppError::Fabrix)?;

                let res = UploadedFile::new(name, get_current_time());
                result.push(res);
            }
        }
    }

    Ok(HttpResponse::Ok().json(result))
}
