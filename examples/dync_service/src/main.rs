//! Dyn conn service
//!
//! test case

use std::str::FromStr;

use actix_web::{
    error::ErrorInternalServerError, web, App, HttpResponse, HttpServer, Responder, Result,
};
use fabrix::{
    DatabaseMysql, DatabasePg, DatabaseSqlite, DynConn, DynConnForSql, DynConnInfo, DynConnSqlInfo,
    FabrixResult, SqlBuilder, SqlEngine, SqlExecutor,
};
use serde::{Deserialize, Serialize};

type BoxSE = Box<dyn SqlEngine>;
type DC = DynConn<String, BoxSE>;

#[derive(Serialize, Deserialize)]
struct DynConnIndexInfo {
    uid: String,
    driver: SqlBuilder,
    conn: String,
}

impl DynConnIndexInfo {
    fn new(uid: String, driver: SqlBuilder, conn: String) -> Self {
        Self { uid, driver, conn }
    }

    fn generate_sql_executor(&self) -> FabrixResult<BoxSE> {
        let engine: Box<dyn SqlEngine> = match self.driver {
            SqlBuilder::Mysql => {
                Box::new(SqlExecutor::<DatabaseMysql>::from_str(self.conn.as_str())?)
            }
            SqlBuilder::Postgres => {
                Box::new(SqlExecutor::<DatabasePg>::from_str(self.conn.as_str())?)
            }
            SqlBuilder::Sqlite => {
                Box::new(SqlExecutor::<DatabaseSqlite>::from_str(self.conn.as_str())?)
            }
        };

        Ok(engine)
    }
}

impl From<(String, DynConnSqlInfo)> for DynConnIndexInfo {
    fn from((uid, info): (String, DynConnSqlInfo)) -> Self {
        DynConnIndexInfo::new(uid, info.driver, info.conn_str)
    }
}

#[derive(Deserialize)]
struct QueryKey {
    key: String,
}

async fn index(data: web::Data<DC>) -> Result<impl Responder> {
    let res = data
        .list_all()
        .into_iter()
        .map(DynConnIndexInfo::from)
        .collect::<Vec<_>>();

    Ok(web::Json(res))
}

async fn add(
    data: web::Data<DC>,
    conn_info: web::Json<DynConnIndexInfo>,
) -> Result<impl Responder> {
    let q = conn_info.into_inner();
    let ec = q
        .generate_sql_executor()
        .map_err(|_| ErrorInternalServerError("failed to generate sql executor"))?;

    match data.insert(q.uid, ec) {
        Some(_) => Ok(HttpResponse::Ok().finish()),
        None => Ok(HttpResponse::InternalServerError().finish()),
    }
}

async fn remove(data: web::Data<DC>, key: web::Json<QueryKey>) -> Result<impl Responder> {
    match data.remove(&key.into_inner().key) {
        Some(_) => Ok(HttpResponse::Ok().finish()),
        None => Ok(HttpResponse::InternalServerError().finish()),
    }
}

async fn connect(data: web::Data<DC>, key: web::Query<QueryKey>) -> Result<impl Responder> {
    match data.connect(&key.into_inner().key).await {
        Ok(_) => Ok(HttpResponse::Ok().finish()),
        Err(_) => Ok(HttpResponse::InternalServerError().finish()),
    }
}

async fn disconnect(data: web::Data<DC>, key: web::Query<QueryKey>) -> Result<impl Responder> {
    match data.disconnect(&key.into_inner().key).await {
        Ok(_) => Ok(HttpResponse::Ok().finish()),
        Err(_) => Ok(HttpResponse::InternalServerError().finish()),
    }
}

async fn check(data: web::Data<DC>, key: web::Query<QueryKey>) -> Result<impl Responder> {
    match data.is_connected(&key.into_inner().key) {
        Ok(_) => Ok(HttpResponse::Ok().finish()),
        Err(_) => Ok(HttpResponse::InternalServerError().finish()),
    }
}

// TODO:
// 1. add initial data (from yaml) by using serde_yaml
// 1. other operations: dml, ddl

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let dc: DC = DynConn::new();
    let web_data = web::Data::new(dc);

    HttpServer::new(move || {
        App::new()
            .app_data(web_data.clone())
            .route("/", web::get().to(index))
            .route("/add", web::post().to(add))
            .route("/remove", web::delete().to(remove))
            .route("/connect", web::get().to(connect))
            .route("/disconnect", web::delete().to(disconnect))
            .route("/check", web::get().to(check))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
