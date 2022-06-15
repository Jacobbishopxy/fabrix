//! DynConnForSql
//!
//! sql

use std::{
    fmt::Display,
    hash::Hash,
    ops::{Deref, DerefMut},
};

use async_trait::async_trait;
use fabrix_core::{D1Value, Fabrix, Series};
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

    async fn connect(&self, key: &K) -> DynConnResult<()>;

    async fn disconnect(&self, key: &K) -> DynConnResult<()>;

    fn is_connected(&self, key: &K) -> DynConnResult<bool>;

    // ================================================================================================
    // SqlHelper
    // ================================================================================================

    async fn get_table_exists(&self, key: &K, table: &str) -> DynConnResult<bool>;

    async fn get_table_schema(
        &self,
        key: &K,
        table: &str,
    ) -> DynConnResult<Vec<sql_adt::TableSchema>>;

    async fn get_table_constraint(
        &self,
        key: &K,
        table: &str,
    ) -> DynConnResult<Vec<sql_adt::TableConstraint>>;

    async fn get_column_constraint(
        &self,
        key: &K,
        table: &str,
    ) -> DynConnResult<Vec<sql_adt::ColumnConstraint>>;

    async fn get_column_index(
        &self,
        key: &K,
        table_name: &str,
    ) -> DynConnResult<Vec<sql_adt::ColumnIndex>>;

    async fn get_tables_name(&self, key: &K) -> DynConnResult<Vec<String>>;

    async fn get_primary_key(&self, key: &K, table: &str) -> DynConnResult<String>;

    async fn get_existing_ids(
        &self,
        key: &K,
        table_name: &str,
        ids: &Series,
    ) -> DynConnResult<D1Value>;

    async fn drop_table(&self, key: &K, table: &str) -> DynConnResult<()>;

    async fn rename_table(&self, key: &K, from: &str, to: &str) -> DynConnResult<()>;

    async fn truncate_table(&self, key: &K, table: &str) -> DynConnResult<()>;

    async fn create_index(
        &self,
        key: &K,
        table: &str,
        column: &str,
        index: Option<&str>,
    ) -> DynConnResult<()>;

    async fn drop_index(&self, key: &K, table: &str, index: &str) -> DynConnResult<()>;

    // ================================================================================================
    // SqlEngine
    // ================================================================================================

    async fn insert(&self, key: &K, table: &str, data: Fabrix) -> DynConnResult<u64>;

    async fn update(&self, key: &K, table: &str, data: Fabrix) -> DynConnResult<u64>;

    async fn save(
        &self,
        key: &K,
        table: &str,
        data: Fabrix,
        strategy: &sql_adt::SaveStrategy,
    ) -> DynConnResult<usize>;

    async fn delete(&self, key: &K, delete: &sql_adt::Delete) -> DynConnResult<u64>;

    async fn select(&self, key: &K, select: &sql_adt::Select) -> DynConnResult<Fabrix>;
}

/// getting a reference of SqlExecutor by a given key
macro_rules! gv {
    ($self:expr, $key:expr) => {{
        let v = $self.store.try_get($key);

        if v.is_locked() {
            return Err(DynConnError::Locked);
        }

        v.try_unwrap().ok_or(DynConnError::Absent)?.value()
    }};
}

/// getting a mutable ref SqlExecutor by a given key
macro_rules! gmv {
    ($self:expr, $key:expr) => {{
        let v = $self.store.try_get_mut($key);

        if v.is_locked() {
            return Err(DynConnError::Locked);
        }

        v.try_unwrap().ok_or(DynConnError::Absent)?.value_mut()
    }};
}

#[async_trait]
impl<K, V> DynConnForSql<K> for DynConn<K, V>
where
    K: Display + Eq + Hash + Send + Sync,
    V: Deref<Target = dyn SqlEngine>,
    V: DerefMut<Target = dyn SqlEngine>,
    V: Send + Sync,
{
    fn get_driver(&self, key: &K) -> DynConnResult<SqlBuilder> {
        Ok(gv!(self, key).get_driver().clone())
    }

    fn get_conn_str(&self, key: &K) -> DynConnResult<String> {
        Ok(gv!(self, key).get_conn_str().to_string())
    }

    async fn connect(&self, key: &K) -> DynConnResult<()> {
        Ok(gmv!(self, key).connect().await?)
    }

    async fn disconnect(&self, key: &K) -> DynConnResult<()> {
        Ok(gmv!(self, key).disconnect().await?)
    }

    fn is_connected(&self, key: &K) -> DynConnResult<bool> {
        Ok(gv!(self, key).is_connected())
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

    async fn get_table_constraint(
        &self,
        key: &K,
        table: &str,
    ) -> DynConnResult<Vec<sql_adt::TableConstraint>> {
        Ok(gv!(self, key).get_table_constraint(table).await?)
    }

    async fn get_column_constraint(
        &self,
        key: &K,
        table: &str,
    ) -> DynConnResult<Vec<sql_adt::ColumnConstraint>> {
        Ok(gv!(self, key).get_column_constraint(table).await?)
    }

    async fn get_column_index(
        &self,
        key: &K,
        table_name: &str,
    ) -> DynConnResult<Vec<sql_adt::ColumnIndex>> {
        Ok(gv!(self, key).get_column_index(table_name).await?)
    }

    async fn get_tables_name(&self, key: &K) -> DynConnResult<Vec<String>> {
        Ok(gv!(self, key).get_tables_name().await?)
    }

    async fn get_primary_key(&self, key: &K, table: &str) -> DynConnResult<String> {
        Ok(gv!(self, key).get_primary_key(table).await?)
    }

    async fn get_existing_ids(
        &self,
        key: &K,
        table_name: &str,
        ids: &Series,
    ) -> DynConnResult<D1Value> {
        Ok(gv!(self, key).get_existing_ids(table_name, ids).await?)
    }

    async fn drop_table(&self, key: &K, table: &str) -> DynConnResult<()> {
        Ok(gv!(self, key).drop_table(table).await?)
    }

    async fn rename_table(&self, key: &K, from: &str, to: &str) -> DynConnResult<()> {
        Ok(gv!(self, key).rename_table(from, to).await?)
    }

    async fn truncate_table(&self, key: &K, table: &str) -> DynConnResult<()> {
        Ok(gv!(self, key).truncate_table(table).await?)
    }

    async fn create_index(
        &self,
        key: &K,
        table: &str,
        column: &str,
        index: Option<&str>,
    ) -> DynConnResult<()> {
        Ok(gv!(self, key).create_index(table, column, index).await?)
    }

    async fn drop_index(&self, key: &K, table: &str, index: &str) -> DynConnResult<()> {
        Ok(gv!(self, key).drop_index(table, index).await?)
    }

    async fn insert(&self, key: &K, table: &str, data: Fabrix) -> DynConnResult<u64> {
        Ok(gv!(self, key).insert(table, data).await?)
    }

    async fn update(&self, key: &K, table: &str, data: Fabrix) -> DynConnResult<u64> {
        Ok(gv!(self, key).update(table, data).await?)
    }

    async fn save(
        &self,
        key: &K,
        table: &str,
        data: Fabrix,
        strategy: &sql_adt::SaveStrategy,
    ) -> DynConnResult<usize> {
        Ok(gv!(self, key).save(table, data, strategy).await?)
    }

    async fn delete(&self, key: &K, delete: &sql_adt::Delete) -> DynConnResult<u64> {
        Ok(gv!(self, key).delete(delete).await?)
    }

    async fn select(&self, key: &K, select: &sql_adt::Select) -> DynConnResult<Fabrix> {
        Ok(gv!(self, key).select(select).await?)
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

        let db1 = SqlExecutor::<DatabasePg>::from_str(CONN2).unwrap();
        let db2 = SqlExecutor::<DatabaseSqlite>::from_str(CONN3).unwrap();

        let k1 = Uuid::new_v4();
        dc.store.insert(k1, Box::new(db1));
        let k2 = Uuid::new_v4();
        dc.store.insert(k1, Box::new(db2));

        let arc_dc = Arc::new(dc);

        let foo = arc_dc.clone();
        let bar = arc_dc.clone();

        let task1 = async move {
            foo.connect(&k1).await.expect("postgres connection failed");
            foo.get_table_exists(&k1, "dev").await
        };

        let task2 = async move {
            bar.connect(&k2).await.expect("sqlite connection failed");
            bar.get_table_schema(&k2, "dev").await
        };

        let (res1, res2) = tokio::join!(task1, task2);

        println!("{:?}", res1);
        println!("{:?}", res2);
    }
}
