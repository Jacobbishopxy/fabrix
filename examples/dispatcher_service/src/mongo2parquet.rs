//! MongoDB to Parquet
//!
//! MongoReader + ParquetWriter

use std::io::Cursor;

use actix_web::{http::header::ContentType, web, HttpResponse, Result};
use fabrix::{MongoReader, ParquetWriter};

use crate::{AppError, QueryObjectId, MG_COLLECTION, MG_CONN, MG_DATABASE};

pub async fn mongo_to_parquet(oid: web::Query<QueryObjectId>) -> Result<HttpResponse> {
    let mut reader = MongoReader::new(MG_CONN, MG_DATABASE, MG_COLLECTION)
        .await
        .map_err(AppError::Fabrix)?;

    reader.with_id(&oid.0.object_id);

    let fx = reader.finish().await.map_err(AppError::Fabrix)?;

    let cd = format!("attachment; filename={:?}.parquet", oid.0);

    let mut bytes = Vec::new();
    let mut writer = ParquetWriter::new(Cursor::new(&mut bytes));
    writer.finish(fx).map_err(AppError::Fabrix)?;

    Ok(HttpResponse::Ok()
        .append_header(("content-disposition", cd.as_str()))
        .append_header(ContentType::octet_stream())
        .body(bytes))
}
