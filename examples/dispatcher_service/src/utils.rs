//! Database utils

use std::str::FromStr;

use actix_web::{web, HttpResponse, Result};
use fabrix::{DatabaseSqlite, SqlExecutor, SqlHelper, SqlMeta};

use crate::{AppError, QueryTableName, DB_CONN};

pub async fn show_tables() -> Result<HttpResponse> {
    let mut executor = SqlExecutor::<DatabaseSqlite>::from_str(DB_CONN).map_err(AppError::Sql)?;
    executor.connect().await.map_err(AppError::Sql)?;
    let tables = executor.get_tables_name().await.map_err(AppError::Sql)?;
    Ok(HttpResponse::Ok().json(tables))
}

pub async fn show_table_schema(que: web::Query<QueryTableName>) -> Result<HttpResponse> {
    let mut executor = SqlExecutor::<DatabaseSqlite>::from_str(DB_CONN).map_err(AppError::Sql)?;
    executor.connect().await.map_err(AppError::Sql)?;
    let schema = executor
        .get_table_schema(&que.table_name)
        .await
        .map_err(AppError::Sql)?;
    Ok(HttpResponse::Ok().json(schema))
}
