//! Sql Writer
//!
//! Writing by SQL.

use std::str::FromStr;

use async_trait::async_trait;
use fabrix_sql::{sql_adt, DatabaseType, SqlConnInfo, SqlEngine, SqlError, SqlExecutor};

use crate::{Fabrix, FabrixError, FabrixResult, IntoSource, WriteOptions};

// ================================================================================================
// Sql Writer
// ================================================================================================

pub struct Writer<T>
where
    T: DatabaseType,
{
    sql_writer: SqlExecutor<T>,
    save_strategy: Option<sql_adt::SaveStrategy>,
}

impl<T> Writer<T>
where
    T: DatabaseType,
{
    pub async fn new<C: TryInto<SqlConnInfo, Error = SqlError>>(
        conn: C,
    ) -> FabrixResult<Writer<T>> {
        let conn = conn.try_into()?;
        let mut sql_writer = SqlExecutor::new(conn);
        sql_writer.connect().await?;

        Ok(Self {
            sql_writer,
            save_strategy: None,
        })
    }

    pub async fn new_from_str(conn_str: &str) -> FabrixResult<Writer<T>> {
        let mut sql_writer = SqlExecutor::from_str(conn_str)?;
        sql_writer.connect().await?;

        Ok(Self {
            sql_writer,
            save_strategy: None,
        })
    }

    pub async fn new_writer<C: TryInto<SqlConnInfo, Error = SqlError>>(
        &mut self,
        conn: C,
    ) -> FabrixResult<Writer<T>> {
        self.sql_writer.disconnect().await?;
        let conn = conn.try_into()?;
        let mut sql_writer = SqlExecutor::new(conn);
        sql_writer.connect().await?;

        Ok(Self {
            sql_writer,
            save_strategy: None,
        })
    }

    pub async fn new_writer_from_str(&mut self, conn_str: &str) -> FabrixResult<Writer<T>> {
        self.sql_writer.disconnect().await?;
        let mut sql_writer = SqlExecutor::from_str(conn_str)?;
        sql_writer.connect().await?;

        Ok(Self {
            sql_writer,
            save_strategy: None,
        })
    }

    pub fn writer(&self) -> &SqlExecutor<T> {
        &self.sql_writer
    }

    pub fn with_save_strategy(&mut self, save_strategy: sql_adt::SaveStrategy) -> &mut Self {
        self.save_strategy = Some(save_strategy);
        self
    }

    pub async fn finish(&mut self, table_name: &str, fabrix: Fabrix) -> FabrixResult<()> {
        // default save strategy is `SaveStrategy::FailIfExists`
        let save_strategy = self
            .save_strategy
            .take()
            .unwrap_or(sql_adt::SaveStrategy::FailIfExists);

        self.sql_writer
            .save(table_name, fabrix, &save_strategy)
            .await?;

        Ok(())
    }
}

// ================================================================================================
// Sql write options & IntoSource impl
// ================================================================================================

#[derive(Default)]
pub struct SqlWriteOptions<'a> {
    pub table_name: Option<&'a str>,
    pub save_strategy: Option<sql_adt::SaveStrategy>,
}

impl<'a> WriteOptions for SqlWriteOptions<'a> {
    fn source_type() -> &'static str {
        "sql"
    }
}

#[async_trait]
impl<'a, T> IntoSource<'a, SqlWriteOptions<'_>> for Writer<T>
where
    T: DatabaseType,
{
    async fn async_write<'o>(
        &mut self,
        fabrix: Fabrix,
        options: &'o SqlWriteOptions,
    ) -> FabrixResult<()>
    where
        'o: 'a,
    {
        let SqlWriteOptions {
            table_name,
            save_strategy,
        } = options;

        let table_name =
            table_name.ok_or_else(|| FabrixError::new_common_error("table name not found"))?;

        if let Some(save_strategy) = save_strategy {
            self.save_strategy = Some(save_strategy.clone());
        }

        self.finish(table_name, fabrix).await
    }

    fn sync_write<'o>(&mut self, _fabrix: Fabrix, _options: &'o SqlWriteOptions) -> FabrixResult<()>
    where
        'o: 'a,
    {
        unimplemented!("sync_write is not allowed in sql writer")
    }
}

#[cfg(test)]
mod test_sql_writer {
    use fabrix_sql::DatabaseSqlite;

    use super::*;
    use crate::{date, fx};

    const CONN: &str = "sqlite://dev.sqlite";
    const TABLE: &str = "ds_sql_test";

    #[tokio::test]
    async fn test_write() {
        let mut writer = Writer::<DatabaseSqlite>::new_from_str(CONN).await.unwrap();

        let fx = fx![
            "ord";
            "ord" => [1, 3, 4, 5, 8, 10, 11],
            "name" => ["John", "Mary", "Mike", "Tom", "Jack", "Lily", "Lucy"],
            "age" => [20, 19, 31, 24, 15, 21, 23],
            "issue_date" => [date!(2010,5,6), date!(2012,1,18), date!(2011,9,8), date!(2019,7,2), date!(2018,8,8), date!(2022,5,14), date!(2021,6,12)],
        ].unwrap();

        let res = writer.finish(TABLE, fx).await;
        assert!(res.is_ok());
    }
}
