//! All the responses returned by the dispatcher service.

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct UploadedFile {
    filename: String,
    time: u64,
    size: u64,
}
