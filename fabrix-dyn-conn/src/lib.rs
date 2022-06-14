//! Fabrix Dyn Conn
//!
//! Used for maintaining dynamic connections of different sources, such as databases, files, etc.

pub mod error;

use std::hash::Hash;
use std::sync::Arc;

use dashmap::iter::Iter;
use dashmap::mapref::one::{Ref, RefMut};
use dashmap::DashMap;

// TODO:
// 1. make DynConn as a trait
// 2. impl custom logic for K & V
// 3. test case use actix-web (https://actix.rs/docs/application/)

#[derive(Default)]
pub struct DynConn<K, V>
where
    K: Eq + Hash,
{
    pub store: Arc<DashMap<K, V>>,
}

impl<K, V> DynConn<K, V>
where
    K: Eq + Hash,
{
    pub fn new() -> Self {
        Self {
            store: Arc::new(DashMap::new()),
        }
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.store.contains_key(key)
    }

    pub fn get(&self, key: &K) -> Option<Ref<K, V>> {
        self.store.get(key)
    }

    pub fn get_mut(&self, key: &K) -> Option<RefMut<K, V>> {
        self.store.get_mut(key)
    }

    pub fn try_get(&self, key: &K) -> Option<Ref<K, V>> {
        self.store.try_get(key).try_unwrap()
    }

    pub fn try_get_mut(&self, key: &K) -> Option<RefMut<K, V>> {
        self.store.try_get_mut(key).try_unwrap()
    }

    pub fn iter(&self) -> Iter<K, V> {
        self.store.iter()
    }

    pub fn insert(&mut self, key: K, value: V) {
        self.store.insert(key, value);
    }

    pub fn remove(&mut self, key: &K) {
        self.store.remove(key);
    }
}

#[cfg(test)]
mod tests {

    use std::str::FromStr;

    // use fabrix_core::fx;
    use fabrix_sql::{DatabasePg, DatabaseSqlite, SqlEngine, SqlExecutor, SqlHelper};
    use uuid::Uuid;

    use super::*;

    // const CONN1: &str = "mysql://root:secret@localhost:3306/dev";
    const CONN2: &str = "postgres://root:secret@localhost:5432/dev";
    const CONN3: &str = "sqlite://dev.sqlite";

    #[tokio::test]
    async fn dyn_conn_creation() {
        // as an example, we won't need to establish real connections
        let db1 = SqlExecutor::<DatabasePg>::from_str(CONN2).unwrap();
        let db2 = SqlExecutor::<DatabaseSqlite>::from_str(CONN3).unwrap();

        let mut dc = DynConn::<Uuid, Box<dyn SqlEngine>>::new();

        let k1 = Uuid::new_v4();
        let k2 = Uuid::new_v4();

        dc.insert(k1, Box::new(db1));
        dc.insert(k2, Box::new(db2));

        let foo = dc.store.get(&k2).unwrap();
        let s = foo
            .value()
            .as_any()
            .downcast_ref::<SqlExecutor<DatabaseSqlite>>()
            .unwrap();

        println!("{:?}", s.get_conn_str());
    }

    #[tokio::test]
    async fn dyn_conn_use() -> std::io::Result<()> {
        // as an example, we won't need to establish real connections
        let mut db1 = SqlExecutor::<DatabasePg>::from_str(CONN2).unwrap();
        db1.connect().await.expect("postgres connection failed");
        let mut db2 = SqlExecutor::<DatabaseSqlite>::from_str(CONN3).unwrap();
        db2.connect().await.expect("sqlite connection failed");

        let mut dc = DynConn::<Uuid, Box<dyn SqlEngine>>::new();

        let k1 = Uuid::new_v4();
        let k2 = Uuid::new_v4();

        dc.insert(k1, Box::new(db1));
        dc.insert(k2, Box::new(db2));

        let foo = dc.store.clone();
        let bar = dc.store.clone();

        let task1 = async move {
            let k = foo.get(&k1).unwrap();
            let p1 = k
                .as_any()
                .downcast_ref::<SqlExecutor<DatabasePg>>()
                .unwrap();

            p1.list_tables().await
        };

        let task2 = async move {
            let k = bar.get(&k2).unwrap();
            let p1 = k
                .as_any()
                .downcast_ref::<SqlExecutor<DatabaseSqlite>>()
                .unwrap();

            p1.list_tables().await
        };

        let (res1, res2) = tokio::join!(task1, task2);

        println!("{:?}", res1);
        println!("{:?}", res2);

        Ok(())
    }
}
