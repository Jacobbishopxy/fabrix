//! Sql Writer
//!
//! Writing by SQL.

use async_trait::async_trait;

use crate::{Fabrix, FabrixResult, IntoSource, WriteOptions};

use super::{sql_adt, sql_executor::SqlConnInfo, SqlExecutor};

// ================================================================================================
// Sql Writer
// ================================================================================================

pub struct Writer {
    sql_writer: Option<SqlExecutor>,
    save_strategy: Option<sql_adt::SaveStrategy>,
}

impl Writer {
    pub fn new<C: Into<SqlConnInfo>>(conn: C) -> Self {
        Self {
            sql_writer: Some(SqlExecutor::new(conn.into())),
            save_strategy: None,
        }
    }

    pub fn has_writer(&self) -> bool {
        self.sql_writer.is_some()
    }

    pub fn with_save_strategy(&mut self, save_strategy: sql_adt::SaveStrategy) -> &mut Self {
        self.save_strategy = Some(save_strategy);
        self
    }

    pub async fn finish(&mut self, mut fabrix: Fabrix) -> FabrixResult<()> {
        todo!()
    }
}

// ================================================================================================
// Sql write options & IntoSource impl
// ================================================================================================

#[derive(Default)]
pub struct SqlWriteOptions {
    save_strategy: Option<sql_adt::SaveStrategy>,
}

impl WriteOptions for SqlWriteOptions {
    fn source_type(&self) -> &str {
        "sql"
    }
}

#[async_trait]
impl<'a> IntoSource<'a, SqlWriteOptions> for Writer {
    async fn async_write<'o>(
        &mut self,
        fabrix: Fabrix,
        options: &'o SqlWriteOptions,
    ) -> FabrixResult<()>
    where
        'o: 'a,
    {
        if let Some(save_strategy) = &options.save_strategy {
            self.save_strategy = Some(save_strategy.clone());
        }

        self.finish(fabrix).await
    }

    fn sync_write<'o>(&mut self, fabrix: Fabrix, options: &'o SqlWriteOptions) -> FabrixResult<()>
    where
        'o: 'a,
    {
        unimplemented!("sync_write is not allowed in sql writer")
    }
}
