//! Error handling

use actix_web::http::{header::ContentType, StatusCode};
use actix_web::{HttpResponse, ResponseError};
use derive_more::{Display, Error};
use serde::Serialize;

#[derive(Serialize, Debug, Display, Error)]
pub enum AppError {
    #[display(fmt = "Invalid file type: {}", e)]
    InvalidFileType { e: String },

    #[display(fmt = "Uncategorized error: {}", e)]
    Uncategorized { e: String },
}

impl ResponseError for AppError {
    fn status_code(&self) -> StatusCode {
        match self {
            AppError::InvalidFileType { .. } => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            AppError::Uncategorized { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code())
            .insert_header(ContentType::html())
            .body(self.to_string())
    }
}
