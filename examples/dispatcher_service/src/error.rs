//! Error handling

use actix_web::http::{header::ContentType, StatusCode};
use actix_web::{HttpResponse, ResponseError};
use fabrix::FabrixError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Invalid file type: {0}")]
    InvalidFileType(String),

    #[error(transparent)]
    Fabrix(#[from] FabrixError),

    #[error(transparent)]
    Serde(#[from] serde_json::Error),

    #[error("Uncategorized error: {0}")]
    Uncategorized(String),
}

impl ResponseError for AppError {
    fn status_code(&self) -> StatusCode {
        match self {
            AppError::InvalidFileType(_) => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            AppError::Fabrix(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::Serde(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::Uncategorized(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code())
            .insert_header(ContentType::html())
            .body(self.to_string())
    }
}
