//! All the requests required by the dispatcher service.

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct QueryTableName {
    pub table_name: String,
}

#[derive(Debug, Deserialize)]
pub struct QueryObjectId {
    pub object_id: String,
}
