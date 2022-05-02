//! Fabrix sql executor pool

use async_trait::async_trait;
use futures::TryStreamExt;
use itertools::Either;
use sqlx::mysql::MySqlQueryResult;
use sqlx::postgres::PgQueryResult;
use sqlx::sqlite::SqliteQueryResult;
use sqlx::{Executor, MySql, MySqlPool, PgPool, Postgres, Sqlite, SqlitePool, Transaction};

use super::{fetch_process, fetch_process_cst, types::SqlRow, SqlRowProcessor};
use crate::{
    sql::sql_adt::ExecutionResult, D1Value, D2Value, Row, SqlBuilder, SqlResult, ValueType,
};

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
pub(crate) trait FabrixDatabaseLoader: Send + Sync {
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

/// LoaderPool: Enum type of `sqlx` db pool
pub(crate) enum LoaderPool {
    Mysql(MySqlPool),
    Pg(PgPool),
    Sqlite(SqlitePool),
}

impl From<MySqlPool> for LoaderPool {
    fn from(pool: MySqlPool) -> Self {
        LoaderPool::Mysql(pool)
    }
}

impl From<PgPool> for LoaderPool {
    fn from(pool: PgPool) -> Self {
        LoaderPool::Pg(pool)
    }
}

impl From<SqlitePool> for LoaderPool {
    fn from(pool: SqlitePool) -> Self {
        LoaderPool::Sqlite(pool)
    }
}

#[async_trait]
impl FabrixDatabaseLoader for LoaderPool {
    async fn disconnect(&self) {
        match self {
            Self::Mysql(pool) => pool.close().await,
            Self::Pg(pool) => pool.close().await,
            Self::Sqlite(pool) => pool.close().await,
        }
    }

    async fn fetch_all(&self, query: &str) -> SqlResult<D2Value> {
        let mut srp = SqlRowProcessor::new();
        let res = match self {
            Self::Mysql(pool) => fetch_process!(pool, query, &mut srp, process, fetch_all),
            Self::Pg(pool) => fetch_process!(pool, query, &mut srp, process, fetch_all),
            Self::Sqlite(pool) => fetch_process!(pool, query, &mut srp, process, fetch_all),
        };

        Ok(res)
    }

    async fn fetch_all_with_schema(
        &self,
        query: &str,
        value_types: &[ValueType],
    ) -> SqlResult<D2Value> {
        let res = match self {
            Self::Mysql(pool) => {
                let mut srp = SqlRowProcessor::new_with_cache(&SqlBuilder::Mysql, value_types);
                fetch_process!(pool, query, &mut srp, process, fetch_all)
            }
            Self::Pg(pool) => {
                let mut srp = SqlRowProcessor::new_with_cache(&SqlBuilder::Postgres, value_types);
                fetch_process!(pool, query, &mut srp, process, fetch_all)
            }
            Self::Sqlite(pool) => {
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

        let res = match self {
            Self::Mysql(pool) => fetch_process_cst!(pool, query, &srp, &f, fetch_all),
            Self::Pg(pool) => fetch_process_cst!(pool, query, &srp, &f, fetch_all),
            Self::Sqlite(pool) => fetch_process_cst!(pool, query, &srp, &f, fetch_all),
        };

        Ok(res)
    }

    async fn fetch_all_to_rows(&self, query: &str) -> SqlResult<Vec<Row>> {
        let mut srp = SqlRowProcessor::new();
        let res = match self {
            Self::Mysql(pool) => fetch_process!(pool, query, &mut srp, process_to_row, fetch_all),
            Self::Pg(pool) => fetch_process!(pool, query, &mut srp, process_to_row, fetch_all),
            Self::Sqlite(pool) => fetch_process!(pool, query, &mut srp, process_to_row, fetch_all),
        };

        Ok(res)
    }

    async fn fetch_one(&self, query: &str) -> SqlResult<D1Value> {
        let mut srp = SqlRowProcessor::new();
        let res = match self {
            Self::Mysql(pool) => fetch_process!(pool, query, &mut srp, process, fetch_one),
            Self::Pg(pool) => fetch_process!(pool, query, &mut srp, process, fetch_one),
            Self::Sqlite(pool) => fetch_process!(pool, query, &mut srp, process, fetch_one),
        };

        Ok(res)
    }

    async fn fetch_one_with_schema(
        &self,
        query: &str,
        value_types: &[ValueType],
    ) -> SqlResult<D1Value> {
        let res = match self {
            Self::Mysql(pool) => {
                let mut srp = SqlRowProcessor::new_with_cache(&SqlBuilder::Mysql, value_types);
                fetch_process!(pool, query, &mut srp, process, fetch_one)
            }
            Self::Pg(pool) => {
                let mut srp = SqlRowProcessor::new_with_cache(&SqlBuilder::Postgres, value_types);
                fetch_process!(pool, query, &mut srp, process, fetch_one)
            }
            Self::Sqlite(pool) => {
                let mut srp = SqlRowProcessor::new_with_cache(&SqlBuilder::Sqlite, value_types);
                fetch_process!(pool, query, &mut srp, process, fetch_one)
            }
        };

        Ok(res)
    }

    async fn fetch_one_cst_prc_fn(&self, query: &str, f: CstPrc) -> SqlResult<D1Value> {
        let srp = SqlRowProcessor::new();
        let res = match self {
            Self::Mysql(pool) => fetch_process_cst!(pool, query, &srp, &f, fetch_one),
            Self::Pg(pool) => fetch_process_cst!(pool, query, &srp, &f, fetch_one),
            Self::Sqlite(pool) => fetch_process_cst!(pool, query, &srp, &f, fetch_one),
        };

        Ok(res)
    }

    async fn fetch_optional(&self, query: &str) -> SqlResult<Option<D1Value>> {
        let mut srp = SqlRowProcessor::new();

        let res = match self {
            Self::Mysql(pool) => fetch_process!(pool, query, &mut srp, process, fetch_optional),
            Self::Pg(pool) => fetch_process!(pool, query, &mut srp, process, fetch_optional),
            Self::Sqlite(pool) => fetch_process!(pool, query, &mut srp, process, fetch_optional),
        };

        Ok(res)
    }

    async fn fetch_optional_with_schema(
        &self,
        query: &str,
        value_types: &[ValueType],
    ) -> SqlResult<Option<D1Value>> {
        let res = match self {
            Self::Mysql(pool) => {
                let mut srp = SqlRowProcessor::new_with_cache(&SqlBuilder::Mysql, value_types);
                fetch_process!(pool, query, &mut srp, process, fetch_optional)
            }
            Self::Pg(pool) => {
                let mut srp = SqlRowProcessor::new_with_cache(&SqlBuilder::Postgres, value_types);
                fetch_process!(pool, query, &mut srp, process, fetch_optional)
            }
            Self::Sqlite(pool) => {
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

        let res = match self {
            Self::Mysql(pool) => fetch_process_cst!(pool, query, &srp, &f, fetch_optional),
            Self::Pg(pool) => fetch_process_cst!(pool, query, &srp, &f, fetch_optional),
            Self::Sqlite(pool) => fetch_process_cst!(pool, query, &srp, &f, fetch_optional),
        };

        Ok(res)
    }

    async fn fetch_many(&self, queries: &str) -> SqlResult<Vec<ExecutionResultOrData>> {
        // let mut srp = SqlRowProcessor::new();
        let mut res = vec![];

        match self {
            Self::Mysql(pool) => {
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
            Self::Pg(pool) => {
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
            Self::Sqlite(pool) => {
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
        let eff = match self {
            Self::Mysql(pool) => sqlx::query(query).execute(pool).await?.into(),
            Self::Pg(pool) => sqlx::query(query).execute(pool).await?.into(),
            Self::Sqlite(pool) => sqlx::query(query).execute(pool).await?.into(),
        };
        Ok(eff)
    }

    async fn execute_many(&self, queries: &str) -> SqlResult<ExecutionResult> {
        let mut rows_affected = 0;

        match self {
            Self::Mysql(pool) => {
                let mut stream = pool.execute_many(queries);
                while let Ok(Some(r)) = stream.try_next().await {
                    rows_affected += r.rows_affected();
                }
            }
            Self::Pg(pool) => {
                let mut stream = pool.execute_many(queries);
                while let Ok(Some(r)) = stream.try_next().await {
                    rows_affected += r.rows_affected();
                }
            }
            Self::Sqlite(pool) => {
                let mut stream = pool.execute_many(queries);
                while let Ok(Some(r)) = stream.try_next().await {
                    rows_affected += r.rows_affected();
                }
            }
        };

        Ok(rows_affected.into())
    }

    async fn begin_transaction(&self) -> SqlResult<LoaderTransaction<'_>> {
        let txn = match self {
            Self::Mysql(pool) => LoaderTransaction::Mysql(pool.begin().await?),
            Self::Pg(pool) => LoaderTransaction::Pg(pool.begin().await?),
            Self::Sqlite(pool) => LoaderTransaction::Sqlite(pool.begin().await?),
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
        unimplemented!()
    }

    #[tokio::test]
    async fn test_sqlx_execute() {
        let pool3 = sqlx::SqlitePool::connect(CONN3).await.unwrap();

        let que = "INSERT INTO \"test\" (\"id\", \"first_name\", \"last_name\", \"email\", \"gender\", \"ip_address\", \"company\", \"city\", \"birth\") VALUES (96, 'Blondie', 'D\\'Ruel', 'bdruel2n@sun.com', 'Genderqueer', '151.50.91.25', 'Tekfly', 'Changshengqiao', '6/16/1984'), (97, 'Etti', 'Klimko', 'eklimko2o@arizona.edu', 'Bigender', '41.14.13.78', 'Twinder', 'Papetoai', '8/2/1994'), (98, 'Early', 'Dowtry', 'edowtry2p@nba.com', 'Non-binary', '39.216.183.46', NULL, 'Kabarnet', '7/16/1998'), (99, 'Adelina', 'Tunn', 'atunn2q@reuters.com', 'Agender', '156.252.19.192', 'Omba', 'Nanqi', '9/14/1980'), (100, 'Kristien', 'Rabl', 'krabl2r@yahoo.com', 'Polygender', '213.123.199.87', 'Mita', 'Colmar', '3/20/1991')";

        let res = sqlx::query(que).execute(&pool3).await;

        println!("{:?}", res);
    }

    // Test get a table's schema
    #[tokio::test]
    async fn test_get_table_schema() {
        // MySQL
        let pool1 = sqlx::MySqlPool::connect(CONN1).await.unwrap();

        let que = SqlBuilder::Mysql.check_table_schema("dev");

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

        let que = SqlBuilder::Postgres.check_table_schema("dev");

        let d2value = pool2.fetch_all(&que).await.unwrap();

        println!("{:?}", d2value);

        // Sqlite
        let sqlx_pool = sqlx::SqlitePool::connect(CONN3).await.unwrap();

        let que = SqlBuilder::Sqlite.check_table_schema("tag");

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

        let que = SqlBuilder::Mysql.check_table_exists("test_table");

        let res = pool1.fetch_optional(&que).await.unwrap();

        println!("{:?}", res);
        assert!(res.is_some());

        // Pg
        let pool2 = LoaderPool::from(sqlx::PgPool::connect(CONN2).await.unwrap());

        let que = SqlBuilder::Postgres.check_table_exists("author");

        let res = pool2.fetch_optional(&que).await.unwrap();

        println!("{:?}", res);
        assert!(res.is_some());

        // Sqlite
        let pool3 = LoaderPool::from(sqlx::SqlitePool::connect(CONN3).await.unwrap());

        let que = SqlBuilder::Sqlite.check_table_exists("tag");

        let res = pool3.fetch_optional(&que).await.unwrap();

        println!("{:?}", res);
        assert!(res.is_some());
    }
}
