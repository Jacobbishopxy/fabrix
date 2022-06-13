//! Fabrix util
//!
//! utilities

use crate::{CoreError, Value};

/// a general naming for a default FDataFrame index
pub const IDX: &str = "index";
pub(crate) const DECIMAL: &str = "Decimal";
pub(crate) const UUID: &str = "Uuid";
pub(crate) const BYTES: &str = "Bytes";
pub(crate) const DAYS19700101: i32 = 719_163;
pub(crate) const NANO10E9: i64 = 1_000_000_000;

/// Used for counting iteration and determining when to stop yielding
pub struct Stepper {
    pub(crate) len: usize,
    pub(crate) step: usize,
}

impl Stepper {
    pub fn new(len: usize) -> Self {
        Stepper { len, step: 0 }
    }

    pub fn exhausted(&self) -> bool {
        self.len == self.step
    }

    pub fn forward(&mut self) {
        self.step += 1;
    }
}

/// out of boundary error
pub(crate) fn oob_err(length: usize, len: usize) -> CoreError {
    CoreError::OutOfBoundary(length, len)
}

/// index not found error
pub(crate) fn inf_err() -> CoreError {
    CoreError::IndexNotFound
}

/// index mismatch error
pub(crate) fn ims_err() -> CoreError {
    CoreError::IndexMismatch
}

/// type mismatch error
pub(crate) fn tms_err(name: &'static str) -> CoreError {
    CoreError::TypeMismatch(name)
}

/// value not found error
pub(crate) fn vnf_err(value: &Value) -> CoreError {
    CoreError::ValueNotFound(value.to_string())
}

/// content empty error
pub(crate) fn cis_err(name: &'static str) -> CoreError {
    CoreError::EmptyContent(name)
}

/// name not found error
pub(crate) fn nnf_err(name: &str) -> CoreError {
    CoreError::NameNotFound(name.to_owned())
}

/// length does not match error
pub(crate) fn lnm_err(len1: usize, len2: usize) -> CoreError {
    CoreError::LengthMismatch(len1, len2)
}

/// invalid data length error
pub(crate) fn idl_err() -> CoreError {
    CoreError::InvalidLength
}
