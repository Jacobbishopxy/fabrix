//! Sql Executor
//!
//! The most important parts of the sql executor are `loader.rs` & `executor.rs`

pub mod executor;
pub mod loader;
pub mod macros;
pub mod processor;
pub mod types;

pub use executor::{SqlEngine, SqlExecutor, SqlHelper};
pub(crate) use loader::{FabrixDatabaseLoader, LoaderPool};
pub(crate) use macros::*;
pub(crate) use processor::SqlRowProcessor;

use crate::SqlBuilder;

/// Connection information
pub struct SqlConnInfo {
    pub driver: SqlBuilder,
    pub username: String,
    pub password: String,
    pub host: String,
    pub port: i32,
    pub database: String,
}

impl SqlConnInfo {
    pub fn new(
        driver: SqlBuilder,
        username: &str,
        password: &str,
        host: &str,
        port: i32,
        database: &str,
    ) -> SqlConnInfo {
        SqlConnInfo {
            driver,
            username: username.to_owned(),
            password: password.to_owned(),
            host: host.to_owned(),
            port,
            database: database.to_owned(),
        }
    }
}

impl std::fmt::Display for SqlConnInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}://{}:{}@{}:{}/{}",
            self.driver, self.username, self.password, self.host, self.port, self.database,
        )
    }
}
