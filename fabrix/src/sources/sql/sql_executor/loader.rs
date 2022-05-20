//! Fabrix sql executor pool

use async_trait::async_trait;
use futures::TryStreamExt;
use itertools::Either;
use sqlx::mysql::MySqlQueryResult;
use sqlx::postgres::PgQueryResult;
use sqlx::sqlite::SqliteQueryResult;
use sqlx::{Executor, MySql, MySqlPool, PgPool, Postgres, Sqlite, SqlitePool, Transaction};

use super::{fetch_process, fetch_process_cst, types::SqlRow, SqlRowProcessor};
use crate::{sql_adt::ExecutionResult, D1Value, D2Value, Row, SqlBuilder, SqlResult, ValueType};

/// turn MySqlQueryResult into ExecutionResult
impl From<MySqlQueryResult> for ExecutionResult {
    fn from(result: MySqlQueryResult) -> Self {
        ExecutionResult {
            rows_affected: result.rows_affected(),
        }
    }
}

/// turn PgQueryResult into ExecutionResult
impl From<PgQueryResult> for ExecutionResult {
    fn from(result: PgQueryResult) -> Self {
        ExecutionResult {
            rows_affected: result.rows_affected(),
        }
    }
}

/// turn SqliteQueryResult into ExecutionResult
impl From<SqliteQueryResult> for ExecutionResult {
    fn from(result: SqliteQueryResult) -> Self {
        ExecutionResult {
            rows_affected: result.rows_affected(),
        }
    }
}

/// Loader transaction aims to provide a common interface for all database transaction objects
pub(crate) enum LoaderTransaction<'a> {
    Mysql(Transaction<'a, MySql>),
    Pg(Transaction<'a, Postgres>),
    Sqlite(Transaction<'a, Sqlite>),
}

impl<'a> LoaderTransaction<'a> {
    /// execute a query
    pub async fn execute(&mut self, sql: &str) -> SqlResult<ExecutionResult> {
        match self {
            Self::Mysql(tx) => {
                let result = sqlx::query(sql).execute(tx).await?;
                Ok(ExecutionResult::from(result))
            }
            Self::Pg(tx) => {
                let result = sqlx::query(sql).execute(tx).await?;
                Ok(ExecutionResult::from(result))
            }
            Self::Sqlite(tx) => {
                let result = sqlx::query(sql).execute(tx).await?;
                Ok(ExecutionResult::from(result))
            }
        }
    }

    /// rollback transaction
    pub async fn rollback(self) -> SqlResult<()> {
        match self {
            Self::Mysql(tx) => Ok(tx.rollback().await?),
            Self::Pg(tx) => Ok(tx.rollback().await?),
            Self::Sqlite(tx) => Ok(tx.rollback().await?),
        }
    }

    /// commit the transaction
    pub async fn commit(self) -> SqlResult<()> {
        match self {
            LoaderTransaction::Mysql(tx) => Ok(tx.commit().await?),
            LoaderTransaction::Pg(tx) => Ok(tx.commit().await?),
            LoaderTransaction::Sqlite(tx) => Ok(tx.commit().await?),
        }
    }
}

pub(crate) enum ExecutionResultOrData {
    ExecutionResult(ExecutionResult),
    // Data(Vec<Row>),
}

/// customized sql row processor fn
type CstPrc = Box<dyn Fn(SqlRow) -> SqlResult<D1Value> + Sync + Send>;

/// database loader interface
#[async_trait]
pub trait FabrixDatabaseLoader<T>: Send + Sync {
    /// connect to database
    async fn connect(conn_str: &str) -> SqlResult<T>;

    /// disconnect from the current database
    async fn disconnect(&self);

    /// fetch all and return 2d Value Vec
    async fn fetch_all(&self, query: &str) -> SqlResult<D2Value>;

    /// fetch all with schema
    async fn fetch_all_with_schema(
        &self,
        query: &str,
        value_types: &[ValueType],
    ) -> SqlResult<D2Value>;

    /// fetch all, customized processing method
    async fn fetch_all_cst_prc_fn(&self, query: &str, f: CstPrc) -> SqlResult<D2Value>;

    /// fetch all with primary key. Make sure the first select column is always the primary key
    async fn fetch_all_to_rows(&self, query: &str) -> SqlResult<Vec<Row>>;

    /// fetch one and return 1d Value Vec
    async fn fetch_one(&self, query: &str) -> SqlResult<D1Value>;

    /// fetch one with schema
    async fn fetch_one_with_schema(
        &self,
        query: &str,
        value_types: &[ValueType],
    ) -> SqlResult<D1Value>;

    /// fetch one, customized processing method
    async fn fetch_one_cst_prc_fn(&self, query: &str, f: CstPrc) -> SqlResult<D1Value>;

    /// fetch optional
    async fn fetch_optional(&self, query: &str) -> SqlResult<Option<D1Value>>;

    /// fetch optional with schema
    async fn fetch_optional_with_schema(
        &self,
        query: &str,
        value_types: &[ValueType],
    ) -> SqlResult<Option<D1Value>>;

    /// fetch optional, customized processing method
    async fn fetch_optional_cst_prc_fn(&self, query: &str, f: CstPrc)
        -> SqlResult<Option<D1Value>>;

    /// fetch many
    async fn fetch_many(&self, queries: &str) -> SqlResult<Vec<ExecutionResultOrData>>;

    /// sql string execution
    async fn execute(&self, query: &str) -> SqlResult<ExecutionResult>;

    /// multiple sql string execution. Beware, this is not atomic, if needs to be atomic, use transaction
    async fn execute_many(&self, queries: &str) -> SqlResult<ExecutionResult>;

    /// create a transaction instance and begin
    async fn begin_transaction(&self) -> SqlResult<LoaderTransaction<'_>>;
}

pub trait DatabaseType: Send + Sync
where
    Self: Sized,
{
    fn new_mysql_pool(pool: MySqlPool) -> Self {
        unimplemented!()
    }

    fn new_pg_pool(pool: PgPool) -> Self {
        unimplemented!()
    }

    fn new_sqlite_pool(pool: SqlitePool) -> Self {
        unimplemented!()
    }

    fn get_pool(&self) -> LoaderPool;

    fn get_driver() -> SqlBuilder;
}

pub struct DatabaseMysql(MySqlPool);

impl DatabaseType for DatabaseMysql {
    fn new_mysql_pool(pool: MySqlPool) -> Self {
        DatabaseMysql(pool)
    }

    fn get_pool(&self) -> LoaderPool {
        LoaderPool::Mysql(&self.0)
    }

    fn get_driver() -> SqlBuilder {
        SqlBuilder::Mysql
    }
}

pub struct DatabasePg(PgPool);

impl DatabaseType for DatabasePg {
    fn new_pg_pool(pool: PgPool) -> Self {
        DatabasePg(pool)
    }

    fn get_pool(&self) -> LoaderPool {
        LoaderPool::Pg(&self.0)
    }

    fn get_driver() -> SqlBuilder {
        SqlBuilder::Postgres
    }
}

pub struct DatabaseSqlite(SqlitePool);

impl DatabaseType for DatabaseSqlite {
    fn new_sqlite_pool(pool: SqlitePool) -> Self {
        DatabaseSqlite(pool)
    }

    fn get_pool(&self) -> LoaderPool {
        LoaderPool::Sqlite(&self.0)
    }

    fn get_driver() -> SqlBuilder {
        SqlBuilder::Sqlite
    }
}

/// LoaderPool: Enum type of `sqlx` db pool
pub enum LoaderPool<'a> {
    Mysql(&'a MySqlPool),
    Pg(&'a PgPool),
    Sqlite(&'a SqlitePool),
}

// impl From<MySqlPool> for LoaderPool {
//     fn from(pool: MySqlPool) -> Self {
//         LoaderPool::Mysql(pool)
//     }
// }

// impl From<PgPool> for LoaderPool {
//     fn from(pool: PgPool) -> Self {
//         LoaderPool::Pg(pool)
//     }
// }

// impl From<SqlitePool> for LoaderPool {
//     fn from(pool: SqlitePool) -> Self {
//         LoaderPool::Sqlite(pool)
//     }
// }

#[async_trait]
impl<T> FabrixDatabaseLoader<T> for T
where
    T: DatabaseType,
{
    async fn connect(conn_str: &str) -> SqlResult<T> {
        match T::get_driver() {
            SqlBuilder::Mysql => MySqlPool::connect(conn_str)
                .await
                .map(|pool| Ok(T::new_mysql_pool(pool)))?,
            SqlBuilder::Postgres => PgPool::connect(conn_str)
                .await
                .map(|pool| Ok(T::new_pg_pool(pool)))?,
            SqlBuilder::Sqlite => SqlitePool::connect(conn_str)
                .await
                .map(|pool| Ok(T::new_sqlite_pool(pool)))?,
        }
    }

    async fn disconnect(&self) {
        match self.get_pool() {
            LoaderPool::Mysql(pool) => pool.close().await,
            LoaderPool::Pg(pool) => pool.close().await,
            LoaderPool::Sqlite(pool) => pool.close().await,
        }
    }

    async fn fetch_all(&self, query: &str) -> SqlResult<D2Value> {
        let mut srp = SqlRowProcessor::new();
        let res = match self.get_pool() {
            LoaderPool::Mysql(pool) => fetch_process!(pool, query, &mut srp, process, fetch_all),
            LoaderPool::Pg(pool) => fetch_process!(pool, query, &mut srp, process, fetch_all),
            LoaderPool::Sqlite(pool) => fetch_process!(pool, query, &mut srp, process, fetch_all),
        };

        Ok(res)
    }

    async fn fetch_all_with_schema(
        &self,
        query: &str,
        value_types: &[ValueType],
    ) -> SqlResult<D2Value> {
        let res = match self.get_pool() {
            LoaderPool::Mysql(pool) => {
                let mut srp = SqlRowProcessor::new_with_cache(&SqlBuilder::Mysql, value_types);
                fetch_process!(pool, query, &mut srp, process, fetch_all)
            }
            LoaderPool::Pg(pool) => {
                let mut srp = SqlRowProcessor::new_with_cache(&SqlBuilder::Postgres, value_types);
                fetch_process!(pool, query, &mut srp, process, fetch_all)
            }
            LoaderPool::Sqlite(pool) => {
                let mut srp = SqlRowProcessor::new_with_cache(&SqlBuilder::Sqlite, value_types);
                fetch_process!(pool, query, &mut srp, process, fetch_all)
            }
        };

        Ok(res)
    }

    async fn fetch_all_cst_prc_fn(
        &self,
        query: &str,
        f: Box<dyn Fn(SqlRow) -> SqlResult<D1Value> + Sync + Send>,
    ) -> SqlResult<D2Value> {
        let srp = SqlRowProcessor::new();

        let res = match self.get_pool() {
            LoaderPool::Mysql(pool) => fetch_process_cst!(pool, query, &srp, &f, fetch_all),
            LoaderPool::Pg(pool) => fetch_process_cst!(pool, query, &srp, &f, fetch_all),
            LoaderPool::Sqlite(pool) => fetch_process_cst!(pool, query, &srp, &f, fetch_all),
        };

        Ok(res)
    }

    async fn fetch_all_to_rows(&self, query: &str) -> SqlResult<Vec<Row>> {
        let mut srp = SqlRowProcessor::new();
        let res = match self.get_pool() {
            LoaderPool::Mysql(pool) => {
                fetch_process!(pool, query, &mut srp, process_to_row, fetch_all)
            }
            LoaderPool::Pg(pool) => {
                fetch_process!(pool, query, &mut srp, process_to_row, fetch_all)
            }
            LoaderPool::Sqlite(pool) => {
                fetch_process!(pool, query, &mut srp, process_to_row, fetch_all)
            }
        };

        Ok(res)
    }

    async fn fetch_one(&self, query: &str) -> SqlResult<D1Value> {
        let mut srp = SqlRowProcessor::new();
        let res = match self.get_pool() {
            LoaderPool::Mysql(pool) => fetch_process!(pool, query, &mut srp, process, fetch_one),
            LoaderPool::Pg(pool) => fetch_process!(pool, query, &mut srp, process, fetch_one),
            LoaderPool::Sqlite(pool) => fetch_process!(pool, query, &mut srp, process, fetch_one),
        };

        Ok(res)
    }

    async fn fetch_one_with_schema(
        &self,
        query: &str,
        value_types: &[ValueType],
    ) -> SqlResult<D1Value> {
        let res = match self.get_pool() {
            LoaderPool::Mysql(pool) => {
                let mut srp = SqlRowProcessor::new_with_cache(&SqlBuilder::Mysql, value_types);
                fetch_process!(pool, query, &mut srp, process, fetch_one)
            }
            LoaderPool::Pg(pool) => {
                let mut srp = SqlRowProcessor::new_with_cache(&SqlBuilder::Postgres, value_types);
                fetch_process!(pool, query, &mut srp, process, fetch_one)
            }
            LoaderPool::Sqlite(pool) => {
                let mut srp = SqlRowProcessor::new_with_cache(&SqlBuilder::Sqlite, value_types);
                fetch_process!(pool, query, &mut srp, process, fetch_one)
            }
        };

        Ok(res)
    }

    async fn fetch_one_cst_prc_fn(&self, query: &str, f: CstPrc) -> SqlResult<D1Value> {
        let srp = SqlRowProcessor::new();
        let res = match self.get_pool() {
            LoaderPool::Mysql(pool) => fetch_process_cst!(pool, query, &srp, &f, fetch_one),
            LoaderPool::Pg(pool) => fetch_process_cst!(pool, query, &srp, &f, fetch_one),
            LoaderPool::Sqlite(pool) => fetch_process_cst!(pool, query, &srp, &f, fetch_one),
        };

        Ok(res)
    }

    async fn fetch_optional(&self, query: &str) -> SqlResult<Option<D1Value>> {
        let mut srp = SqlRowProcessor::new();

        let res = match self.get_pool() {
            LoaderPool::Mysql(pool) => {
                fetch_process!(pool, query, &mut srp, process, fetch_optional)
            }
            LoaderPool::Pg(pool) => fetch_process!(pool, query, &mut srp, process, fetch_optional),
            LoaderPool::Sqlite(pool) => {
                fetch_process!(pool, query, &mut srp, process, fetch_optional)
            }
        };

        Ok(res)
    }

    async fn fetch_optional_with_schema(
        &self,
        query: &str,
        value_types: &[ValueType],
    ) -> SqlResult<Option<D1Value>> {
        let res = match self.get_pool() {
            LoaderPool::Mysql(pool) => {
                let mut srp = SqlRowProcessor::new_with_cache(&SqlBuilder::Mysql, value_types);
                fetch_process!(pool, query, &mut srp, process, fetch_optional)
            }
            LoaderPool::Pg(pool) => {
                let mut srp = SqlRowProcessor::new_with_cache(&SqlBuilder::Postgres, value_types);
                fetch_process!(pool, query, &mut srp, process, fetch_optional)
            }
            LoaderPool::Sqlite(pool) => {
                let mut srp = SqlRowProcessor::new_with_cache(&SqlBuilder::Sqlite, value_types);
                fetch_process!(pool, query, &mut srp, process, fetch_optional)
            }
        };

        Ok(res)
    }

    async fn fetch_optional_cst_prc_fn(
        &self,
        query: &str,
        f: CstPrc,
    ) -> SqlResult<Option<D1Value>> {
        let srp = SqlRowProcessor::new();

        let res = match self.get_pool() {
            LoaderPool::Mysql(pool) => fetch_process_cst!(pool, query, &srp, &f, fetch_optional),
            LoaderPool::Pg(pool) => fetch_process_cst!(pool, query, &srp, &f, fetch_optional),
            LoaderPool::Sqlite(pool) => fetch_process_cst!(pool, query, &srp, &f, fetch_optional),
        };

        Ok(res)
    }

    async fn fetch_many(&self, queries: &str) -> SqlResult<Vec<ExecutionResultOrData>> {
        // let mut srp = SqlRowProcessor::new();
        let mut res = vec![];

        match self.get_pool() {
            LoaderPool::Mysql(pool) => {
                let mut stream = pool.fetch_many(queries);
                while let Ok(Some(e)) = stream.try_next().await {
                    match e {
                        Either::Left(l) => {
                            res.push(ExecutionResultOrData::ExecutionResult(l.into()));
                        }
                        Either::Right(_) => todo!(),
                    };
                }
            }
            LoaderPool::Pg(pool) => {
                let mut stream = pool.fetch_many(queries);
                while let Ok(Some(e)) = stream.try_next().await {
                    match e {
                        Either::Left(l) => {
                            res.push(ExecutionResultOrData::ExecutionResult(l.into()));
                        }
                        Either::Right(_) => todo!(),
                    };
                }
            }
            LoaderPool::Sqlite(pool) => {
                let mut stream = pool.fetch_many(queries);
                while let Ok(Some(e)) = stream.try_next().await {
                    match e {
                        Either::Left(l) => {
                            res.push(ExecutionResultOrData::ExecutionResult(l.into()));
                        }
                        Either::Right(_) => todo!(),
                    };
                }
            }
        };

        Ok(res)
    }

    async fn execute(&self, query: &str) -> SqlResult<ExecutionResult> {
        let eff = match self.get_pool() {
            LoaderPool::Mysql(pool) => sqlx::query(query).execute(pool).await?.into(),
            LoaderPool::Pg(pool) => sqlx::query(query).execute(pool).await?.into(),
            LoaderPool::Sqlite(pool) => sqlx::query(query).execute(pool).await?.into(),
        };
        Ok(eff)
    }

    async fn execute_many(&self, queries: &str) -> SqlResult<ExecutionResult> {
        let mut rows_affected = 0;

        match self.get_pool() {
            LoaderPool::Mysql(pool) => {
                let mut stream = pool.execute_many(queries);
                while let Ok(Some(r)) = stream.try_next().await {
                    rows_affected += r.rows_affected();
                }
            }
            LoaderPool::Pg(pool) => {
                let mut stream = pool.execute_many(queries);
                while let Ok(Some(r)) = stream.try_next().await {
                    rows_affected += r.rows_affected();
                }
            }
            LoaderPool::Sqlite(pool) => {
                let mut stream = pool.execute_many(queries);
                while let Ok(Some(r)) = stream.try_next().await {
                    rows_affected += r.rows_affected();
                }
            }
        };

        Ok(rows_affected.into())
    }

    async fn begin_transaction(&self) -> SqlResult<LoaderTransaction<'_>> {
        let txn = match self.get_pool() {
            LoaderPool::Mysql(pool) => LoaderTransaction::Mysql(pool.begin().await?),
            LoaderPool::Pg(pool) => LoaderTransaction::Pg(pool.begin().await?),
            LoaderPool::Sqlite(pool) => LoaderTransaction::Sqlite(pool.begin().await?),
        };

        Ok(txn)
    }
}

#[cfg(test)]
mod test_pool {
    use super::*;
    use crate::{value, DdlQuery, SqlBuilder};
    use futures::TryStreamExt;
    use sqlx::{Executor, Row};

    const CONN1: &str = "mysql://root:secret@localhost:3306/dev";
    const CONN2: &str = "postgres://root:secret@localhost:5432/dev";
    const CONN3: &str = "sqlite://dev.sqlite";

    const TABLE_NAME: &str = "dev";

    #[tokio::test]
    async fn test_sqlx_execute_many() {
        let pool = sqlx::MySqlPool::connect(CONN1).await.unwrap();

        let sql = r#"
        CREATE TABLE IF NOT EXISTS recipes (
            recipe_id INT NOT NULL,
            recipe_name VARCHAR(30) NOT NULL,
            PRIMARY KEY (recipe_id),
            UNIQUE (recipe_name)
          );

        INSERT INTO recipes
            (recipe_id, recipe_name)
        VALUES
            (1,"Tacos"),
            (2,"Tomato Soup"),
            (3,"Grilled Cheese");

        INSERT INTO recipes
            (recipe_id, recipe_name)
        VALUES
            (3, 'Cake'),
            (4, 'Pizza'),
            (5, 'Salad');
        "#;

        let mut stream = pool.execute_many(sql);

        println!("{:?}", "Execution starts...");

        loop {
            match stream.try_next().await {
                Ok(Some(r)) => {
                    assert!(r.rows_affected() > 0);
                    println!("{:?}", r);
                }
                Ok(None) => break,
                Err(e) => {
                    println!("{:?}", e);
                    break;
                }
            }
        }
    }

    #[tokio::test]
    async fn test_sqlx_fetch_many() {
        // TODO: test query.fetch_many
    }

    // Test get a table's schema
    #[tokio::test]
    async fn test_get_table_schema() {
        // MySQL
        let pool1 = sqlx::MySqlPool::connect(CONN1).await.unwrap();

        let que = SqlBuilder::Mysql.check_table_schema(TABLE_NAME);

        let res = sqlx::query(&que)
            .try_map(|row: sqlx::mysql::MySqlRow| {
                let name: String = row.get_unchecked(0);
                let col_type: String = row.get_unchecked(1);
                let is_nullable: String = row.get_unchecked(2);
                Ok(vec![value!(name), value!(col_type), value!(is_nullable)])
            })
            .fetch_all(&pool1)
            .await;
        println!("{:?}", res);
        assert!(res.is_ok());

        // Pg
        let pool2 = LoaderPool::from(sqlx::PgPool::connect(CONN2).await.unwrap());

        let que = SqlBuilder::Postgres.check_table_schema(TABLE_NAME);

        let d2value = pool2.fetch_all(&que).await.unwrap();

        println!("{:?}", d2value);

        // Sqlite
        let sqlx_pool = sqlx::SqlitePool::connect(CONN3).await.unwrap();

        let que = SqlBuilder::Sqlite.check_table_schema(TABLE_NAME);

        let res = sqlx::query(&que)
            .try_map(|row: sqlx::sqlite::SqliteRow| {
                let name: String = row.get_unchecked(0);
                let col_type: String = row.get_unchecked(1);
                let is_nullable: String = row.get_unchecked(2);
                Ok(vec![value!(name), value!(col_type), value!(is_nullable)])
            })
            .fetch_all(&sqlx_pool)
            .await;
        println!("{:?}", res);
        assert!(res.is_ok());
    }

    // Test table if exists
    #[tokio::test]
    async fn test_fetch_optional() {
        // MySQL
        let pool1 = LoaderPool::from(sqlx::MySqlPool::connect(CONN1).await.unwrap());

        let que = SqlBuilder::Mysql.check_table_exists(TABLE_NAME);

        let res = pool1.fetch_optional(&que).await.unwrap();

        println!("{:?}", res);
        assert!(res.is_some());

        // Pg
        let pool2 = LoaderPool::from(sqlx::PgPool::connect(CONN2).await.unwrap());

        let que = SqlBuilder::Postgres.check_table_exists(TABLE_NAME);

        let res = pool2.fetch_optional(&que).await.unwrap();

        println!("{:?}", res);
        assert!(res.is_some());

        // Sqlite
        let pool3 = LoaderPool::from(sqlx::SqlitePool::connect(CONN3).await.unwrap());

        let que = SqlBuilder::Sqlite.check_table_exists(TABLE_NAME);

        let res = pool3.fetch_optional(&que).await.unwrap();

        println!("{:?}", res);
        assert!(res.is_some());
    }
}
