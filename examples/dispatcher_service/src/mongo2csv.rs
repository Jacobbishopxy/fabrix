//! MongoDB to Csv
//!
//! MongoReader + CsvWriter

use std::io::Cursor;

use actix_web::{http::header::ContentType, web, HttpResponse, Result};
use fabrix::{CsvWriter, MongoReader};

use crate::{AppError, QueryObjectId, MG_COLLECTION, MG_CONN, MG_DATABASE};

pub async fn mongo_to_csv(oid: web::Query<QueryObjectId>) -> Result<HttpResponse> {
    let mut reader = MongoReader::new(MG_CONN, MG_DATABASE, MG_COLLECTION)
        .await
        .map_err(AppError::Fabrix)?;

    reader.with_id(&oid.0.object_id);

    let fx = reader.finish().await.map_err(AppError::Fabrix)?;

    let cd = format!("attachment; filename={:?}.csv", oid.0);

    let mut bytes = Vec::new();
    let mut writer = CsvWriter::new(Cursor::new(&mut bytes));
    writer.finish(fx).map_err(AppError::Fabrix)?;

    Ok(HttpResponse::Ok()
        .append_header(("content-disposition", cd.as_str()))
        .append_header(ContentType::octet_stream())
        .body(bytes))
}
