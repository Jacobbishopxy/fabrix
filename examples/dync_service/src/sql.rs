//! DynConn for Sql

use actix_web::{error::ErrorInternalServerError, web, HttpResponse, Responder, Result};
use fabrix::{DynConnForSql, DynConnInfo};

use crate::util::*;

// TODO:
// 1. add initial data (from yaml) by using serde_yaml
// 1. other operations: dml, ddl

pub async fn index(data: web::Data<DcSql>) -> Result<impl Responder> {
    let res = data
        .list_all()
        .into_iter()
        .map(DynConnIndexInfo::from)
        .collect::<Vec<_>>();

    Ok(web::Json(res))
}

pub async fn add(
    data: web::Data<DcSql>,
    conn_info: web::Json<DynConnIndexInfo>,
) -> Result<impl Responder> {
    let q = conn_info.into_inner();
    let ec = q
        .generate_sql_executor()
        .map_err(|_| ErrorInternalServerError("failed to generate sql executor"))?;

    match data.insert(q.uid().to_string(), ec) {
        Some(_) => Ok(HttpResponse::Ok().body("replace previous ec")),
        None => Ok(HttpResponse::Ok().body("new ec registered")),
    }
}

pub async fn remove(data: web::Data<DcSql>, key: web::Query<QueryKey>) -> Result<impl Responder> {
    match data.remove(&key.into_inner().key) {
        Some(_) => Ok(HttpResponse::Ok().body("ec removed")),
        None => Ok(HttpResponse::Ok().body("key does not exist")),
    }
}

pub async fn connect(data: web::Data<DcSql>, key: web::Query<QueryKey>) -> Result<impl Responder> {
    match data.connect(&key.into_inner().key).await {
        Ok(_) => Ok(HttpResponse::Ok().body("connection established")),
        Err(e) => Ok(HttpResponse::InternalServerError().body(e.to_string())),
    }
}

pub async fn disconnect(
    data: web::Data<DcSql>,
    key: web::Query<QueryKey>,
) -> Result<impl Responder> {
    match data.disconnect(&key.into_inner().key).await {
        Ok(_) => Ok(HttpResponse::Ok().body("disconnect succeed")),
        Err(e) => Ok(HttpResponse::InternalServerError().body(e.to_string())),
    }
}

pub async fn check(data: web::Data<DcSql>, key: web::Query<QueryKey>) -> Result<impl Responder> {
    match data.is_connected(&key.into_inner().key) {
        Ok(v) => Ok(HttpResponse::Ok().body(v.to_string())),
        Err(e) => Ok(HttpResponse::InternalServerError().body(e.to_string())),
    }
}
