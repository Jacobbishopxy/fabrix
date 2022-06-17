//! Xl_process_service
//!
//! Receive a xlsx file by http multipart/form-data, read it and send it back as JSON.
//!
//! Reference: https://github.com/actix/examples/blob/master/forms/multipart/src/main.rs

use std::fmt::Display;
use std::fs::File;
use std::io::{Cursor, Write};
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::str::FromStr;

use actix_multipart::Multipart;
use actix_web::{middleware, web, App, Error, HttpResponse, HttpServer};
use clap::Parser;
use futures_util::TryStreamExt;
use serde::Deserialize;
use uuid::Uuid;

use fabrix::{dispatcher::xl_json, FabrixError, XlWorkbook};

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

// save file to disk
async fn save_file(mut payload: Multipart) -> Result<HttpResponse, Error> {
    // iterate over multipart stream
    while let Some(mut field) = payload.try_next().await? {
        // skip field that is not a file
        if field.name() != "file" {
            continue;
        }

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
    }

    Ok(HttpResponse::Ok().into())
}

#[derive(Deserialize)]
struct XlToJsonQuery {
    sheet_name: String,
}

// convert xlsx to json
async fn xl_to_json(
    info: web::Query<XlToJsonQuery>,
    mut payload: Multipart,
) -> Result<HttpResponse, Error> {
    let mut data = Vec::new();

    // iterate over multipart stream
    while let Some(mut field) = payload.try_next().await? {
        // skip field that is not a file
        if field.name() != "file" {
            continue;
        }

        let file_type = field.content_type().to_string();
        dbg!(&file_type);

        if file_type != XL_FILE_TYPE {
            let err = FabrixError::InvalidArgument(format!("Invalid file type: {file_type}"));
            return Err(WebError { err }.into());
        }

        let mut buff = Cursor::new(vec![]);

        // Field in turn is stream of *Bytes* object
        while let Some(chunk) = field.try_next().await? {
            // dbg!(&chunk);
            buff.write_all(&chunk)?;
        }

        let source = XlWorkbook::new(buff).unwrap();

        let mut helper = xl_json::XlJson::new();

        let mut xle = xl_json::XlJsonExecutor::new_with_source(source);

        xle.consume_fn_mut(
            Some(30),
            &info.sheet_name,
            |d| Ok(xl_json::XlJsonConvertor::transform_data(d)),
            |d| {
                helper.append_data(d);
                Ok(())
            },
        )
        .map_err(|e| WebError {
            err: FabrixError::Xl(e),
        })?;

        data.push(helper.data);
    }

    Ok(HttpResponse::Ok().body(serde_json::json!(data).to_string()))
}

async fn index() -> HttpResponse {
    let html = include_str!("index.html");

    HttpResponse::Ok().body(html)
}

#[derive(Parser, Debug)]
#[clap(
    name = "server",
    about = "A simple server for file processing illustration"
)]
struct Opt {
    #[clap(short = 'a', long = "addr", default_value = "::1")]
    addr: String,
    #[clap(short = 'p', long = "port", default_value = "8080")]
    port: u16,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let opt = Opt::parse();
    std::env::set_var("RUST_LOG", "info");
    std::fs::create_dir_all(TMP_DIR)?;

    let sock_addr = SocketAddr::from((
        IpAddr::from_str(opt.addr.as_str()).unwrap_or(IpAddr::V6(Ipv6Addr::LOCALHOST)),
        opt.port,
    ));

    println!("listening on http://{sock_addr}");

    HttpServer::new(|| {
        App::new()
            .wrap(middleware::Logger::default())
            .service(web::resource("/").route(web::get().to(index)))
            .service(web::resource("/save").route(web::post().to(save_file)))
            .service(web::resource("/xl").route(web::post().to(xl_to_json)))
    })
    .bind(sock_addr)?
    .run()
    .await
}
