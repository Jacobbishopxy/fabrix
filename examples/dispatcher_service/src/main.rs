//! Dispatcher service
//!
//! A simple service that accepts multiple sources of data and dispatches them to different places.

use actix_web::{middleware::Logger, web, App, HttpServer};
use dispatcher_service::{
    csv_to_db, csv_to_json, db_to_csv, db_to_parquet, show_table_schema, show_tables, xl_to_db,
    xl_to_json,
};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let config_str = include_str!("../log4rs.yml");
    let config = serde_yaml::from_str(config_str).unwrap();
    log4rs::init_raw_config(config).unwrap();

    let bind = ("127.0.0.1", 8080);
    log::info!("Starting server on: http://{}:{}", bind.0, bind.1);

    HttpServer::new(|| {
        let logger = Logger::default();

        App::new()
            .app_data(web::FormConfig::default().limit(10240))
            .wrap(logger)
            .service(
                web::scope("/api")
                    .service(
                        web::scope("/csv")
                            .route("/to_json", web::post().to(csv_to_json))
                            .route("/to_db", web::post().to(csv_to_db)),
                    )
                    .service(
                        web::scope("/xl")
                            .route("/to_json", web::post().to(xl_to_json))
                            .route("/to_db", web::post().to(xl_to_db)),
                    )
                    .service(
                        web::scope("/db")
                            .route("/show_tables", web::get().to(show_tables))
                            .route("/show_table_schema", web::get().to(show_table_schema))
                            .route("/to_csv", web::post().to(db_to_csv))
                            .route("/to_parquet", web::post().to(db_to_parquet)),
                    ),
            )
    })
    .bind(bind)?
    .run()
    .await
}
