//! Database to CSV file
//!
//! SqlReader + CsvWriter

use std::io::Cursor;

use actix_web::{http::header::ContentType, web, HttpResponse, Result};
use fabrix::{sql_adt, CsvWriter, DatabaseSqlite, FromSource, SqlReadOptions, SqlReader};

use crate::{AppError, DB_CONN};

pub async fn db_to_csv(select: web::Json<sql_adt::Select>) -> Result<HttpResponse> {
    let mut reader = SqlReader::<DatabaseSqlite>::new_from_str(DB_CONN)
        .await
        .map_err(AppError::Fabrix)?;
    let table_name = select.0.get_table();
    let select = SqlReadOptions::from_sql_select(&select.0);
    let fx = reader.async_read(&select).await.map_err(AppError::Fabrix)?;

    let mut bytes = Vec::new();
    let mut writer = CsvWriter::new(Cursor::new(&mut bytes));
    writer.finish(fx).map_err(AppError::Fabrix)?;

    let cd = format!("attachment; filename={table_name}.csv");

    Ok(HttpResponse::Ok()
        .append_header(("content-disposition", cd.as_str()))
        .append_header(ContentType::octet_stream())
        .body(bytes))
}
