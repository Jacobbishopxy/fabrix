//! Core Error
//!
//!

use std::fmt::Display;

use thiserror::Error;

pub type CoreResult<T> = Result<T, CoreError>;

#[derive(Error, Debug)]
pub enum CoreError {
    #[error("parse {0} into {1} error ")]
    Parse(String, String),

    #[error("length {0} out of len {1} boundary")]
    OutOfBoundary(usize, usize),

    #[error("length {0} does not match length {1}")]
    LengthMismatch(usize, usize),

    #[error("invalid length")]
    InvalidLength,

    #[error("index not found")]
    IndexNotFound,

    #[error("index mismatch")]
    IndexMismatch,

    #[error("index tag is empty")]
    EmptyIndexTag,

    #[error("type mismatch {0}")]
    TypeMismatch(&'static str),

    #[error("value {0} not found")]
    ValueNotFound(String),

    #[error("name {0} not found")]
    NameNotFound(String),

    #[error("{0}")]
    EmptyContent(&'static str),

    #[error(transparent)]
    Polars(#[from] polars::error::PolarsError),

    #[error("unknown error")]
    Unknown,
}

impl CoreError {
    pub fn new_parse_error<T1, T2>(type1: T1, type2: T2) -> CoreError
    where
        T1: Display,
        T2: Display,
    {
        CoreError::Parse(type1.to_string(), type2.to_string())
    }

    pub fn new_parse_info_error<T>(r#type: T, info: &str) -> CoreError
    where
        T: Display,
    {
        CoreError::Parse(r#type.to_string(), info.to_string())
    }
}
