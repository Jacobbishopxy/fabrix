//! Fabrix util
//!
//! utilities

use crate::{FabrixError, Value};

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
        if self.len == self.step {
            true
        } else {
            false
        }
    }

    pub fn forward(&mut self) {
        self.step += 1;
    }
}

/// a general naming for a default FDataFrame index
pub const IDX: &'static str = "index";

/// out of boundary error
pub(crate) fn oob_err(length: usize, len: usize) -> FabrixError {
    FabrixError::new_common_error(format!("length {:?} out of len {:?} boundary", length, len))
}

/// index not found error
pub(crate) fn inf_err<'a>(index: &Value) -> FabrixError {
    FabrixError::new_common_error(format!("index {:?} not found", index))
}

/// content empty error
pub(crate) fn cis_err(name: &str) -> FabrixError {
    FabrixError::new_common_error(format!("{:?} is empty", name))
}
