//! Sql Reader
//!
//! Reading by SQL.

use async_trait::async_trait;

use crate::{Fabrix, FabrixResult, FromSource, ReadOptions};

use super::{sql_adt, sql_executor::SqlConnInfo, SqlExecutor};

// ================================================================================================
// Sql Reader
// ================================================================================================

/// Sql Reader
pub struct Reader<'a> {
    sql_reader: Option<SqlExecutor>,
    table: Option<&'a str>,
    columns: Option<&'a [sql_adt::ColumnAlias]>,
    filter: Option<&'a [sql_adt::Expression]>,
    order: Option<&'a [sql_adt::Order]>,
    limit: Option<usize>,
    offset: Option<usize>,
}

impl<'a> Reader<'a> {
    pub fn new<C: Into<SqlConnInfo>>(conn: C) -> Self {
        Self {
            sql_reader: Some(SqlExecutor::new(conn.into())),
            table: None,
            columns: None,
            filter: None,
            order: None,
            limit: None,
            offset: None,
        }
    }

    pub fn has_reader(&self) -> bool {
        self.sql_reader.is_some()
    }

    pub fn with_table(&mut self, table: &'a str) -> &mut Self {
        self.table = Some(table);
        self
    }

    pub fn with_columns(&mut self, columns: &'a [sql_adt::ColumnAlias]) -> &mut Self {
        self.columns = Some(columns);
        self
    }

    pub fn with_filter(&mut self, filter: &'a [sql_adt::Expression]) -> &mut Self {
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

    pub fn finish(&mut self) -> FabrixResult<Fabrix> {
        // TODO:
        todo!()
    }
}

// ================================================================================================
// Sql read options & FromSource impl
// ================================================================================================

#[derive(Default)]
pub struct SqlReadOptions<'a> {
    table: Option<&'a str>,
    columns: Option<&'a [sql_adt::ColumnAlias]>,
    filter: Option<&'a [sql_adt::Expression]>,
    order: Option<&'a [sql_adt::Order]>,
    limit: Option<usize>,
    offset: Option<usize>,
}

impl<'a> ReadOptions for SqlReadOptions<'a> {
    fn source_type(&self) -> &str {
        "sql"
    }
}

#[async_trait]
impl<'a> FromSource<'a, SqlReadOptions<'_>> for Reader<'a> {
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

        self.finish()
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
    // TODO:
}
