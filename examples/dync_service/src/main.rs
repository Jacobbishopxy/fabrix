//! Dyn conn service
//!
//! test case

mod mongo;
mod sql;
mod util;

use actix_web::{web, App, HttpServer};
use fabrix::DynConn;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let dc_sql: util::DcSql = DynConn::new();
    let data_sql = web::Data::new(dc_sql);

    let dc_mongo: util::DcMongo = DynConn::new();
    let data_mongo = web::Data::new(dc_mongo);

    HttpServer::new(move || {
        App::new()
            .service(
                web::scope("/sql")
                    .app_data(data_sql.clone())
                    .route("/", web::get().to(sql::index))
                    .route("/add", web::post().to(sql::add))
                    .route("/remove", web::delete().to(sql::remove))
                    .route("/connect", web::get().to(sql::connect))
                    .route("/disconnect", web::delete().to(sql::disconnect))
                    .route("/check", web::get().to(sql::check)),
            )
            .service(
                web::scope("/mongo")
                    .app_data(data_mongo.clone())
                    .route("/", web::get().to(mongo::index))
                    .route("/add", web::post().to(mongo::add))
                    .route("/remove", web::delete().to(mongo::remove))
                    .route("/connect", web::get().to(mongo::connect))
                    .route("/check", web::get().to(mongo::check)),
            )
    })
    .bind(("127.0.0.1", 8060))?
    .run()
    .await
}
