//! Sql Executor
//!
//! The most important parts of the sql executor are `loader.rs` & `executor.rs`

use std::str::FromStr;

use nom::bytes::complete::{tag, take_until1};
use nom::character::complete::{alpha1, alphanumeric1, digit1};
use nom::sequence::separated_pair;

pub mod executor;
pub mod loader;
pub mod macros;
pub mod processor;
pub mod types;

pub use executor::{SqlEngine, SqlExecutor, SqlHelper};
pub(crate) use loader::{FabrixDatabaseLoader, LoaderPool};
pub(crate) use macros::*;
pub(crate) use processor::SqlRowProcessor;

use crate::{SqlBuilder, SqlError, SqlResult};

/// Connection information
pub struct SqlConnInfo {
    pub driver: SqlBuilder,
    pub username: String,
    pub password: String,
    pub host: String,
    pub port: u32,
    pub database: String,
}

impl SqlConnInfo {
    pub fn new(
        driver: SqlBuilder,
        username: &str,
        password: &str,
        host: &str,
        port: u32,
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

// Nom parser result type
type ConnStrPattern<'a> = (
    &'a str,
    (&'a str, ((&'a str, &'a str), ((&'a str, &'a str), &'a str))),
);

impl<'a> TryFrom<ConnStrPattern<'a>> for SqlConnInfo {
    type Error = SqlError;

    fn try_from(source: ConnStrPattern<'a>) -> Result<Self, Self::Error> {
        let (_, (driver, ((username, password), ((host, port), database)))) = source;

        Ok(Self::new(
            SqlBuilder::from_str(driver)?,
            username,
            password,
            host,
            port.parse::<u32>()?,
            database,
        ))
    }
}

pub(crate) fn get_conn_info(value: &str) -> SqlResult<SqlConnInfo> {
    let f_host_and_port = separated_pair(take_until1(":"), tag(":"), digit1);
    let f_address_and_database = separated_pair(f_host_and_port, tag("/"), alphanumeric1);
    let f_username_and_password = separated_pair(alphanumeric1, tag(":"), alphanumeric1);
    let f_user_and_rest = separated_pair(f_username_and_password, tag("@"), f_address_and_database);
    let mut f_driver_and_rest = separated_pair(alpha1, tag("://"), f_user_and_rest);

    let res = f_driver_and_rest(value)?;

    SqlConnInfo::try_from(res)
}

impl TryFrom<&str> for SqlConnInfo {
    type Error = SqlError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        get_conn_info(value)
    }
}

#[allow(dead_code)]
#[cfg(test)]
mod test_sql_conn_info {
    // use super::*;

    const CONN1: &str = "mysql://root:secret@localhost:3306/dev";
    const CONN2: &str = "postgres://root:secret@127.0.0.1:5432/dev";
    // TODO: `get_conn_info` doesn't work with `sqlite://`
    const CONN3: &str = "sqlite://dev.sqlite";
}
