//! Core Error
//!
//!

use std::fmt::Display;

use thiserror::Error;

use crate::{CommonError, ValueType};

pub type CoreResult<T> = Result<T, CoreError>;

type DataFrameDTypes = (ValueType, Vec<ValueType>);

#[derive(Error, Debug)]
pub enum CoreError {
    #[error("common error {0}")]
    CommonError(CommonError),

    #[error("parse {0} into {1} error ")]
    Parse(String, String),

    #[error(transparent)]
    Polars(#[from] polars::error::PolarsError),

    #[error("unknown error")]
    Unknown,
}

impl CoreError {
    pub fn new_common_error<T>(msg: T) -> Self
    where
        T: Into<CommonError>,
    {
        CoreError::CommonError(msg.into())
    }

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

    pub fn new_df_dtypes_mismatch_error(d1: DataFrameDTypes, d2: DataFrameDTypes) -> CoreError {
        CoreError::new_common_error(format!(
            "dataframe dtypes mismatch, d1: {:#?}, d2: {:#?}",
            d1, d2
        ))
    }

    pub fn new_empty_error() -> CoreError {
        CoreError::new_common_error("empty content")
    }
}
