//! Sql Reader
//!
//! Reading by SQL.

use std::str::FromStr;

use async_trait::async_trait;
use fabrix_sql::{sql_adt, DatabaseType, SqlConnInfo, SqlEngine, SqlError, SqlExecutor, SqlMeta};

use crate::{Fabrix, FabrixError, FabrixResult, FromSource, ReadOptions};

// ================================================================================================
// Sql Reader
// ================================================================================================

/// Sql Reader
pub struct Reader<'a, T>
where
    T: DatabaseType,
{
    sql_reader: SqlExecutor<T>,
    table: Option<&'a str>,
    columns: Option<&'a [sql_adt::Column]>,
    filter: Option<&'a sql_adt::Expressions>,
    order: Option<&'a [sql_adt::Order]>,
    limit: Option<usize>,
    offset: Option<usize>,
    join: Option<&'a sql_adt::Join>,
    group_by: Option<&'a [sql_adt::Column]>,
    include_primary_key: Option<bool>,
}

impl<'a, T> Reader<'a, T>
where
    T: DatabaseType,
{
    pub async fn new<C>(conn: C) -> FabrixResult<Reader<'a, T>>
    where
        C: TryInto<SqlConnInfo, Error = SqlError>,
    {
        let conn = conn.try_into()?;
        let mut sql_reader = SqlExecutor::new(conn);
        sql_reader.connect().await?;

        Ok(Self {
            sql_reader,
            table: None,
            columns: None,
            filter: None,
            order: None,
            limit: None,
            offset: None,
            join: None,
            group_by: None,
            include_primary_key: None,
        })
    }

    pub async fn new_from_str(conn_str: &str) -> FabrixResult<Reader<'a, T>> {
        let mut sql_reader = SqlExecutor::from_str(conn_str)?;
        sql_reader.connect().await?;

        Ok(Self {
            sql_reader,
            table: None,
            columns: None,
            filter: None,
            order: None,
            limit: None,
            offset: None,
            join: None,
            group_by: None,
            include_primary_key: None,
        })
    }

    pub async fn new_reader<C>(&mut self, conn: C) -> FabrixResult<Reader<'a, T>>
    where
        C: TryInto<SqlConnInfo, Error = SqlError>,
    {
        self.sql_reader.disconnect().await?;
        let conn = conn.try_into()?;
        let mut sql_reader = SqlExecutor::new(conn);
        sql_reader.connect().await?;

        Ok(Self {
            sql_reader,
            table: None,
            columns: None,
            filter: None,
            order: None,
            limit: None,
            offset: None,
            join: None,
            group_by: None,
            include_primary_key: None,
        })
    }

    pub async fn new_reader_from_str(&mut self, conn_str: &str) -> FabrixResult<Reader<'a, T>> {
        self.sql_reader.disconnect().await?;
        let mut sql_reader = SqlExecutor::from_str(conn_str)?;
        sql_reader.connect().await?;

        Ok(Self {
            sql_reader,
            table: None,
            columns: None,
            filter: None,
            order: None,
            limit: None,
            offset: None,
            join: None,
            group_by: None,
            include_primary_key: None,
        })
    }

    pub fn reader(&self) -> &SqlExecutor<T> {
        &self.sql_reader
    }

    pub fn with_table(&mut self, table: &'a str) -> &mut Self {
        self.table = Some(table);
        self
    }

    pub fn with_columns(&mut self, columns: &'a [sql_adt::Column]) -> &mut Self {
        self.columns = Some(columns);
        self
    }

    pub fn with_filter(&mut self, filter: &'a sql_adt::Expressions) -> &mut Self {
        self.filter = Some(filter);
        self
    }

    pub fn with_order(&mut self, order: &'a [sql_adt::Order]) -> &mut Self {
        self.order = Some(order);
        self
    }

    pub fn with_limit(&mut self, limit: usize) -> &mut Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_offset(&mut self, offset: usize) -> &mut Self {
        self.offset = Some(offset);
        self
    }

    pub fn with_join(&mut self, join: &'a sql_adt::Join) -> &mut Self {
        self.join = Some(join);
        self
    }

    pub fn with_group_by(&mut self, group_by: &'a [sql_adt::Column]) -> &mut Self {
        self.group_by = Some(group_by);
        self
    }

    pub fn with_include_primary_key(&mut self, include_primary_key: bool) -> &mut Self {
        self.include_primary_key = Some(include_primary_key);
        self
    }

    pub async fn finish(&mut self) -> FabrixResult<Fabrix> {
        let table = self.table.ok_or(FabrixError::NotSet("table"))?.to_owned();
        let columns = self.columns.ok_or(FabrixError::NotSet("columns"))?.to_vec();

        let select = sql_adt::Select {
            table,
            columns,
            filter: self.filter.cloned(),
            order: self.order.map(|o| o.to_vec()),
            limit: self.limit,
            offset: self.offset,
            join: self.join.cloned(),
            group_by: self.group_by.map(|g| g.to_vec()),
            include_primary_key: self.include_primary_key,
        };

        let fx = self.sql_reader.select(&select).await?;

        Ok(fx)
    }
}

// ================================================================================================
// Sql read options & FromSource impl
// ================================================================================================

#[derive(Default)]
pub struct SqlReadOptions<'a> {
    pub table: Option<&'a str>,
    pub columns: Option<&'a [sql_adt::Column]>,
    pub filter: Option<&'a sql_adt::Expressions>,
    pub order: Option<&'a [sql_adt::Order]>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub join: Option<&'a sql_adt::Join>,
    pub group_by: Option<&'a [sql_adt::Column]>,
}

impl<'a> ReadOptions for SqlReadOptions<'a> {
    fn source_type() -> &'static str {
        "sql"
    }
}

impl<'a> SqlReadOptions<'a> {
    pub fn from_sql_select(select: &'a sql_adt::Select) -> Self {
        Self {
            table: Some(&select.table),
            columns: Some(&select.columns),
            filter: select.filter.as_ref(),
            order: select.order.as_deref(),
            limit: select.limit,
            offset: select.offset,
            join: select.join.as_ref(),
            group_by: select.group_by.as_deref(),
        }
    }
}

#[async_trait]
impl<'a, T> FromSource<'a, SqlReadOptions<'_>> for Reader<'a, T>
where
    T: DatabaseType,
{
    async fn async_read<'o>(&mut self, options: &'o SqlReadOptions) -> FabrixResult<Fabrix>
    where
        'o: 'a,
    {
        let SqlReadOptions {
            table,
            columns,
            filter,
            order,
            limit,
            offset,
            join,
            group_by,
        } = options;

        if let Some(table) = table {
            self.with_table(table);
        }
        if let Some(columns) = columns {
            self.with_columns(columns);
        }
        if let Some(filter) = filter {
            self.with_filter(filter);
        }
        if let Some(order) = order {
            self.with_order(order);
        }
        if let Some(limit) = limit {
            self.with_limit(*limit);
        }
        if let Some(offset) = offset {
            self.with_offset(*offset);
        }
        if let Some(join) = join {
            self.with_join(join);
        }
        if let Some(group_by) = group_by {
            self.with_group_by(group_by);
        }

        self.finish().await
    }

    fn sync_read<'o>(&mut self, _options: &'o SqlReadOptions) -> FabrixResult<Fabrix>
    where
        'o: 'a,
    {
        unimplemented!("sync_read is not allowed in sql reader")
    }
}

#[cfg(test)]
mod test_sql_reader {
    use fabrix_sql::DatabaseSqlite;

    use super::*;
    use crate::{xpr, xpr_and, xpr_or};

    const CONN: &str = "sqlite://dev.sqlite";
    const TABLE: &str = "ds_sql_test";

    #[tokio::test]
    async fn test_read() {
        let mut reader = Reader::<DatabaseSqlite>::new_from_str(CONN).await.unwrap();

        let columns = vec!["ord".into(), "name".into()];

        let filter = xpr!([
            xpr!("ord", ">=", 10),
            xpr_or!(),
            xpr!([
                xpr!("names", "in", ["John", "Lily", "Mike"]),
                xpr_and!(),
                xpr!("age", ">", 15)
            ])
        ]);

        reader.with_table(TABLE);
        reader.with_columns(&columns);
        reader.with_filter(&filter);

        let fx = reader.finish().await;
        assert!(fx.is_ok());

        println!("{:?}", fx);
    }
}
