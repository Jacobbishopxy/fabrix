//! Xl_process_service
//!
//! Receive a xlsx file by http multipart/form-data, read it and send it back as JSON.
//!
//! Reference: https://github.com/actix/examples/blob/master/forms/multipart/src/main.rs

use std::fmt::Display;
use std::io::{Cursor, Write};

use actix_multipart::Multipart;
use actix_web::{middleware, web, App, Error, HttpResponse, HttpServer};
use futures_util::TryStreamExt;
// use uuid::Uuid;

use fabrix::{dispatcher::xl_json, xl, FabrixError};

#[derive(Debug)]
struct WebError {
    err: FabrixError,
}

impl Display for WebError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.err)
    }
}

impl actix_web::error::ResponseError for WebError {}

async fn save_file(mut payload: Multipart) -> Result<HttpResponse, Error> {
    // iterate over multipart stream
    while let Some(mut field) = payload.try_next().await? {
        // A multipart/form-data stream has to contain `content_disposition`
        // let content_disposition = field.content_disposition();

        // let filename = content_disposition
        //     .get_filename()
        //     .map_or_else(|| Uuid::new_v4().to_string(), sanitize_filename::sanitize);
        // dbg!(&filename);

        // let filepath = format!("./tmp/{}", filename);
        // dbg!(&filepath);

        // File::create is blocking operation, use threadpool
        // let mut f = web::block(|| File::create(filepath)).await??;

        let mut buff = Cursor::new(vec![]);

        // Field in turn is stream of *Bytes* object
        while let Some(chunk) = field.try_next().await? {
            dbg!(&chunk);

            buff.write_all(&chunk)?;

            // filesystem operations are blocking, we have to use threadpool
            // f = web::block(move || f.write_all(&chunk).map(|_| f)).await??;
        }

        let source = xl::Workbook::new(buff).unwrap();

        let mut xle =
            xl_json::XlJsonExecutor::new_with_source(source).map_err(|e| WebError { err: e })?;

        let res = xle.consume_fn(
            Some(30),
            "data",
            |d| Ok(xl_json::XlJsonConvertor::transform_data(d)),
            |d| {
                println!("{:?}\n\n", d);
                Ok(())
            },
        );

        println!("{:?}", res);

        // read from cached file
    }

    Ok(HttpResponse::Ok().into())
}

async fn index() -> HttpResponse {
    let html = r#"
        <html>
        <head><title>Multipart Form</title></head>
        <body>
            <form target="/" method="post" enctype="multipart/form-data">
                <input type="file" multiple name="file">
                <button type="submit">Submit</button>
            </form>
        </body>
        </html>
    "#;

    HttpResponse::Ok().body(html)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "info");
    std::fs::create_dir_all("./tmp")?;

    HttpServer::new(|| {
        App::new().wrap(middleware::Logger::default()).service(
            web::resource("/")
                .route(web::get().to(index))
                .route(web::post().to(save_file)),
        )
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
