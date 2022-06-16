//! Fabrix Dyn Conn
//!
//! Used for maintaining dynamic connections of different sources, such as databases, files, etc.

pub mod error;
pub mod sql;

use dashmap::iter::{Iter, IterMut};
use dashmap::mapref::entry::Entry;
use dashmap::mapref::one::{Ref, RefMut};
pub use error::*;
pub use sql::*;

use std::hash::Hash;
use std::sync::Arc;

use dashmap::DashMap;

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

    pub fn init<I>(data: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
    {
        let store = DashMap::new();
        for (key, value) in data {
            store.insert(key, value);
        }

        Self {
            store: Arc::new(store),
        }
    }

    // May deadlock if called when holding a mutable reference into the map
    pub fn get(&self, key: &K) -> Option<Ref<K, V>> {
        self.store.get(key)
    }

    pub fn try_get(&self, key: &K) -> DynConnResult<Ref<K, V>> {
        let rf = self.store.try_get(key);

        if rf.is_locked() {
            return Err(DynConnError::Locked);
        }

        rf.try_unwrap().ok_or(DynConnError::Absent)
    }

    // May deadlock if called when holding a mutable reference into the map
    pub fn get_mut(&self, key: &K) -> Option<RefMut<K, V>> {
        self.store.get_mut(key)
    }

    pub fn try_get_mut(&self, key: &K) -> DynConnResult<RefMut<K, V>> {
        let rfm = self.store.try_get_mut(key);

        if rfm.is_locked() {
            return Err(DynConnError::Locked);
        }

        rfm.try_unwrap().ok_or(DynConnError::Absent)
    }

    // May deadlock if called when holding a mutable reference into the map
    pub fn entry(&self, key: K) -> Entry<K, V> {
        self.store.entry(key)
    }

    pub fn try_entry(&self, key: K) -> DynConnResult<Entry<K, V>> {
        self.store.try_entry(key).ok_or(DynConnError::Locked)
    }

    // May deadlock if called when holding a mutable reference into the map
    pub fn iter(&self) -> Iter<K, V> {
        self.store.iter()
    }

    // May deadlock if called when holding a mutable reference into the map
    pub fn iter_mut(&self) -> IterMut<K, V> {
        self.store.iter_mut()
    }

    // May deadlock if called when holding a mutable reference into the map
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        self.store.insert(key, value)
    }

    // May deadlock if called when holding a mutable reference into the map
    pub fn remove(&self, key: &K) -> Option<(K, V)> {
        self.store.remove(key)
    }
}

/// DynConnInfo
///
/// Lets you get information from the underlying store of a DynConn
pub trait DynConnInfo<K, V, Info>
where
    K: Clone + Send + Sync,
    V: Send + Sync,
    for<'a> Info: From<&'a V> + Send + Sync,
{
    fn list_all(&self) -> Vec<(K, Info)>;

    fn get_info(&self, key: &K) -> Option<Info>;
}

#[cfg(test)]
mod dyn_conn_tests {

    use std::str::FromStr;

    use fabrix_sql::{
        DatabaseMysql, DatabasePg, DatabaseSqlite, SqlEngine, SqlExecutor, SqlHelper, SqlMeta,
    };
    use uuid::Uuid;

    use super::*;

    const CONN1: &str = "mysql://root:secret@localhost:3306/dev";
    const CONN2: &str = "postgres://root:secret@localhost:5432/dev";
    const CONN3: &str = "sqlite://dev.sqlite";

    #[tokio::test]
    async fn dyn_conn_creation() {
        // as an example, we won't need to establish real connections
        let db1 = SqlExecutor::<DatabaseMysql>::from_str(CONN1).unwrap();
        let db2 = SqlExecutor::<DatabasePg>::from_str(CONN2).unwrap();
        let db3 = SqlExecutor::<DatabaseSqlite>::from_str(CONN3).unwrap();

        let dc = DynConn::<Uuid, Box<dyn SqlEngine>>::new();

        let k1 = Uuid::new_v4();
        let k2 = Uuid::new_v4();
        let k3 = Uuid::new_v4();

        dc.insert(k1, Box::new(db1));
        dc.insert(k2, Box::new(db2));
        dc.insert(k3, Box::new(db3));

        let foo = dc.get(&k2).unwrap();
        let s = foo
            .value()
            .as_any()
            .downcast_ref::<SqlExecutor<DatabaseSqlite>>()
            .unwrap();

        println!("{:?}", s.get_conn_str());
    }

    #[tokio::test]
    async fn dyn_conn_async() -> std::io::Result<()> {
        // as an example, we won't need to establish real connections
        let mut db1 = SqlExecutor::<DatabasePg>::from_str(CONN2).unwrap();
        db1.connect().await.expect("postgres connection failed");
        let mut db2 = SqlExecutor::<DatabaseSqlite>::from_str(CONN3).unwrap();
        db2.connect().await.expect("sqlite connection failed");

        let dc = DynConn::<Uuid, Box<dyn SqlEngine>>::new();

        let k1 = Uuid::new_v4();
        let k2 = Uuid::new_v4();

        dc.store.insert(k1, Box::new(db1));
        dc.store.insert(k2, Box::new(db2));

        let foo = dc.store.clone();
        let bar = dc.store.clone();

        let task1 = async move {
            let k = foo.get(&k1).unwrap();
            let p1 = k
                .as_any()
                .downcast_ref::<SqlExecutor<DatabasePg>>()
                .unwrap();

            p1.get_tables_name().await
        };

        let task2 = async move {
            let k = bar.get(&k2).unwrap();
            let p1 = k
                .as_any()
                .downcast_ref::<SqlExecutor<DatabaseSqlite>>()
                .unwrap();

            p1.get_tables_name().await
        };

        let (res1, res2) = tokio::join!(task1, task2);

        println!("{:?}", res1);
        println!("{:?}", res2);

        Ok(())
    }
}
