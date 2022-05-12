//! Sql Writer
//!
//! Writing by SQL.

use async_trait::async_trait;

use crate::{Fabrix, FabrixError, FabrixResult, IntoSource, WriteOptions};

// ================================================================================================
// Sql Writer
// ================================================================================================

pub struct Writer {
    sql_writer: Option<()>,
}

// ================================================================================================
// Sql write options & IntoSource impl
// ================================================================================================

#[derive(Default)]
pub struct SqlWriteOptions {}

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
        todo!()
    }

    fn sync_write<'o>(&mut self, fabrix: Fabrix, options: &'o SqlWriteOptions) -> FabrixResult<()>
    where
        'o: 'a,
    {
        todo!()
    }
}
