//! Fabrix Dyn Conn
//!
//! Used for maintaining dynamic connections of different sources, such as databases, files, etc.

use std::hash::Hash;
use std::sync::Arc;

use dashmap::DashMap;

#[derive(Default)]
pub struct DynConn<K, V>
where
    K: Eq + Hash,
{
    store: Arc<DashMap<K, V>>,
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

    // pub fn get(&self, key: &K) -> Option<&V> {
    //     let foo = self.store.get(key);
    //     let bar = foo.as_ref().map(|v| v.value());
    //     bar
    // }

    // pub fn list_all(&self) -> Vec<(&K, &V)> {
    //     self.store.iter().map(|c| c.pair()).collect()
    // }

    pub fn insert(&mut self, key: K, value: V) {
        self.store.insert(key, value);
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
