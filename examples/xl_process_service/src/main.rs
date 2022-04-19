//! Xl_process_service
//!
//! Receive a xlsx file by http multipart/form-data, read it and send it back as JSON.
//!
//! Reference: https://github.com/actix/examples/blob/master/forms/multipart/src/main.rs

use std::fmt::Display;
use std::fs::File;
use std::io::{Cursor, Write};

use actix_multipart::Multipart;
use actix_web::{middleware, web, App, Error, HttpResponse, HttpServer};
use futures_util::TryStreamExt;
use serde::Deserialize;
use uuid::Uuid;

use fabrix::{dispatcher::xl_json, xl, FabrixError};

const TMP_DIR: &str = "./tmp";
const XL_FILE_TYPE: &str = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";

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
        let content_disposition = field.content_disposition();

        let filename = content_disposition
            .get_filename()
            .map_or_else(|| Uuid::new_v4().to_string(), sanitize_filename::sanitize);
        dbg!(&filename);

        let filepath = format!("{TMP_DIR}/{filename}");
        dbg!(&filepath);

        // File::create is blocking operation, use threadpool
        let mut f = web::block(|| File::create(filepath)).await??;

        // Field in turn is stream of *Bytes* object
        while let Some(chunk) = field.try_next().await? {
            // filesystem operations are blocking, we have to use threadpool
            f = web::block(move || f.write_all(&chunk).map(|_| f)).await??;
        }

        // read from cached file
    }

    Ok(HttpResponse::Ok().into())
}

#[derive(Deserialize)]
struct XlToJsonQuery {
    sheet_name: String,
}

async fn xl_to_json(
    info: web::Query<XlToJsonQuery>,
    mut payload: Multipart,
) -> Result<HttpResponse, Error> {
    let mut data = Vec::new();

    // iterate over multipart stream
    while let Some(mut field) = payload.try_next().await? {
        let file_type = field.content_type().to_string();
        dbg!(&file_type);

        if file_type != XL_FILE_TYPE {
            let err = FabrixError::new_common_error(format!("Invalid file type: {file_type}"));
            return Err(WebError { err }.into());
        }

        let mut buff = Cursor::new(vec![]);

        // Field in turn is stream of *Bytes* object
        while let Some(chunk) = field.try_next().await? {
            // dbg!(&chunk);
            buff.write_all(&chunk)?;
        }

        let source = xl::Workbook::new(buff).unwrap();

        let mut helper = xl_json::XlJson::new();

        let mut xle =
            xl_json::XlJsonExecutor::new_with_source(source).map_err(|e| WebError { err: e })?;

        xle.consume_fn_mut(
            Some(30),
            &info.sheet_name,
            |d| Ok(xl_json::XlJsonConvertor::transform_data(d)),
            |d| {
                helper.append_data(d);
                Ok(())
            },
        )
        .map_err(|e| WebError { err: e })?;

        data.push(helper.data);
    }

    Ok(HttpResponse::Ok().body(serde_json::json!(data).to_string()))
}

async fn index() -> HttpResponse {
    let html = r#"
        <html>
        <head><title>Multipart Form</title></head>
        <body>
            <p>save file</p>
            <form action="/save" method="post" enctype="multipart/form-data">
                <input type="file" multiple name="file">
                <button type="submit">Submit</button>
            </form>
            <br/>
            <p>xl file extract</p>
            <form action="/xl" method="post" enctype="multipart/form-data">
                <lable for="sheet_name">Sheet name: </lable>
                <input type="text" name="sheet_name">
                <br/>
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
        App::new()
            .wrap(middleware::Logger::default())
            .service(web::resource("/").route(web::get().to(index)))
            .service(web::resource("/save").route(web::post().to(save_file)))
            .service(web::resource("/xl").route(web::post().to(xl_to_json)))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
