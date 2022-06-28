//! All the responses returned by the dispatcher service.

use serde::Serialize;

#[derive(Serialize, Debug)]
pub(crate) struct UploadedFile {
    filename: String,
    time: u64,
}

impl UploadedFile {
    pub(crate) fn new(filename: String, time: u64) -> Self {
        Self { filename, time }
    }
}
