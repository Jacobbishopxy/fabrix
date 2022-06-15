//! Fabrix Dyn Conn
//!
//! Used for maintaining dynamic connections of different sources, such as databases, files, etc.

pub mod error;
pub mod sql;

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
}

#[cfg(test)]
mod dyn_conn_tests {

    use std::str::FromStr;

    use fabrix_sql::{DatabasePg, DatabaseSqlite, SqlEngine, SqlExecutor, SqlHelper, SqlMeta};
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

        let dc = DynConn::<Uuid, Box<dyn SqlEngine>>::new();

        let k1 = Uuid::new_v4();
        let k2 = Uuid::new_v4();

        dc.store.insert(k1, Box::new(db1));
        dc.store.insert(k2, Box::new(db2));

        let foo = dc.store.get(&k2).unwrap();
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
