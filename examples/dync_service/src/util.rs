//! Utilities

use std::str::FromStr;

use fabrix::{
    DatabaseMysql, DatabasePg, DatabaseSqlite, DynConn, DynConnMongoInfo, DynConnSqlInfo,
    FabrixError, FabrixResult, MongoExecutor, SqlBuilder, SqlEngine, SqlExecutor,
};
use serde::{Deserialize, Serialize};

pub type BoxSE = Box<dyn SqlEngine>;
pub type DcSql = DynConn<String, BoxSE>;
pub type DcMongo = DynConn<String, MongoExecutor>;

#[derive(Deserialize)]
pub struct QueryKey {
    pub key: String,
}

#[derive(Serialize, Deserialize)]
pub enum Driver {
    Mysql,
    Postgres,
    Sqlite,
    Mongo,
}

impl TryFrom<Driver> for SqlBuilder {
    type Error = FabrixError;

    fn try_from(value: Driver) -> Result<Self, Self::Error> {
        match value {
            Driver::Mysql => Ok(SqlBuilder::Mysql),
            Driver::Postgres => Ok(SqlBuilder::Postgres),
            Driver::Sqlite => Ok(SqlBuilder::Sqlite),
            Driver::Mongo => Err(FabrixError::new_uncategorized("Invalid driver: Mongo")),
        }
    }
}

impl From<SqlBuilder> for Driver {
    fn from(value: SqlBuilder) -> Self {
        match value {
            SqlBuilder::Mysql => Driver::Mysql,
            SqlBuilder::Postgres => Driver::Postgres,
            SqlBuilder::Sqlite => Driver::Sqlite,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct DynConnIndexInfo {
    uid: String,
    driver: Driver,
    conn: String,
}

impl DynConnIndexInfo {
    pub fn new(uid: String, driver: Driver, conn: String) -> Self {
        Self { uid, driver, conn }
    }

    pub fn uid(&self) -> &str {
        &self.uid
    }

    pub fn generate_sql_executor(&self) -> FabrixResult<BoxSE> {
        let engine: Box<dyn SqlEngine> = match self.driver {
            Driver::Mysql => Box::new(SqlExecutor::<DatabaseMysql>::from_str(self.conn.as_str())?),
            Driver::Postgres => Box::new(SqlExecutor::<DatabasePg>::from_str(self.conn.as_str())?),
            Driver::Sqlite => {
                Box::new(SqlExecutor::<DatabaseSqlite>::from_str(self.conn.as_str())?)
            }
            _ => return Err(FabrixError::new_uncategorized("Invalid driver: Mongo")),
        };

        Ok(engine)
    }

    pub fn generate_mongo_executor(&self) -> FabrixResult<MongoExecutor> {
        Ok(MongoExecutor::new(&self.conn))
    }
}

impl From<(String, DynConnSqlInfo)> for DynConnIndexInfo {
    fn from((uid, info): (String, DynConnSqlInfo)) -> Self {
        DynConnIndexInfo::new(uid, info.driver.into(), info.conn_str)
    }
}

impl From<(String, DynConnMongoInfo)> for DynConnIndexInfo {
    fn from((uid, info): (String, DynConnMongoInfo)) -> Self {
        DynConnIndexInfo::new(uid, Driver::Mongo, info.conn_str)
    }
}
