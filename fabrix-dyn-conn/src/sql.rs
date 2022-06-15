//! DynConnForSql
//!
//! sql

use std::{fmt::Display, hash::Hash, ops::Deref};

use async_trait::async_trait;
use fabrix_sql::{sql_adt, SqlBuilder, SqlEngine};

use crate::{DynConn, DynConnError, DynConnResult};

#[async_trait]
pub trait DynConnForSql<K>
where
    K: Display + Eq + Hash + Send + Sync,
{
    // ================================================================================================
    // SqlInfo
    // ================================================================================================

    fn get_driver(&self, key: &K) -> DynConnResult<SqlBuilder>;

    fn get_conn_str(&self, key: &K) -> DynConnResult<String>;

    // ================================================================================================
    // SqlHelper
    // ================================================================================================

    async fn get_table_exists(&self, key: &K, table: &str) -> DynConnResult<bool>;

    async fn get_table_schema(
        &self,
        key: &K,
        table: &str,
    ) -> DynConnResult<Vec<sql_adt::TableSchema>>;

    // TODO: the rest of the methods

    // ================================================================================================
    // SqlEngine
    // ================================================================================================

    fn check_connection(&self, key: &K) -> DynConnResult<bool>;

    // TODO: the rest of the methods
}

macro_rules! gv {
    ($self:expr, $key:expr) => {{
        let v = $self.store.try_get($key);

        if v.is_locked() {
            return Err(DynConnError::Locked);
        }

        v.try_unwrap().ok_or(DynConnError::Absent)?.value()
    }};
}

#[async_trait]
impl<K, V> DynConnForSql<K> for DynConn<K, V>
where
    K: Display + Eq + Hash + Send + Sync,
    V: Deref<Target = dyn SqlEngine> + Send + Sync,
{
    fn get_driver(&self, key: &K) -> DynConnResult<SqlBuilder> {
        Ok(gv!(self, key).get_driver().clone())
    }

    fn get_conn_str(&self, key: &K) -> DynConnResult<String> {
        Ok(gv!(self, key).get_conn_str().to_string())
    }

    async fn get_table_exists(&self, key: &K, table: &str) -> DynConnResult<bool> {
        Ok(gv!(self, key).get_table_exists(table).await)
    }

    async fn get_table_schema(
        &self,
        key: &K,
        table: &str,
    ) -> DynConnResult<Vec<sql_adt::TableSchema>> {
        Ok(gv!(self, key).get_table_schema(table).await?)
    }

    fn check_connection(&self, key: &K) -> DynConnResult<bool> {
        Ok(gv!(self, key).is_connected())
    }
}

#[cfg(test)]
mod dyn_conn_for_sql_tests {
    use std::{str::FromStr, sync::Arc};

    use super::*;

    use fabrix_sql::{DatabasePg, DatabaseSqlite, SqlExecutor};
    use uuid::Uuid;

    const CONN2: &str = "postgres://root:secret@localhost:5432/dev";
    const CONN3: &str = "sqlite://dev.sqlite";

    #[tokio::test]
    async fn dyn_conn_for_sql_creation() {
        let dc = DynConn::<Uuid, Box<dyn SqlEngine>>::new();

        let mut db1 = SqlExecutor::<DatabasePg>::from_str(CONN2).unwrap();
        db1.connect().await.expect("postgres connection failed");
        let mut db2 = SqlExecutor::<DatabaseSqlite>::from_str(CONN3).unwrap();
        db2.connect().await.expect("sqlite connection failed");

        let k1 = Uuid::new_v4();
        dc.store.insert(k1, Box::new(db1));
        let k2 = Uuid::new_v4();
        dc.store.insert(k1, Box::new(db2));

        let arc_dc = Arc::new(dc);

        let foo = arc_dc.clone();
        let bar = arc_dc.clone();

        let task1 = async move { foo.get_table_exists(&k1, "dev").await };

        let task2 = async move { bar.get_table_schema(&k2, "dev").await };

        let (res1, res2) = tokio::join!(task1, task2);

        println!("{:?}", res1);
        println!("{:?}", res2);
    }
}
