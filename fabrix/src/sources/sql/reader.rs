//! Sql Reader
//!
//! Reading by SQL.

use async_trait::async_trait;

use crate::{Fabrix, FabrixResult, FromSource, ReadOptions};

// ================================================================================================
// Sql Reader
// ================================================================================================

/// Sql Reader
pub struct Reader {
    sql_reader: Option<()>,
}

// ================================================================================================
// Sql read options & FromSource impl
// ================================================================================================

#[derive(Default)]
pub struct SqlReadOptions {}

impl ReadOptions for SqlReadOptions {
    fn source_type(&self) -> &str {
        "sql"
    }
}

#[async_trait]
impl<'a> FromSource<'a, SqlReadOptions> for Reader {
    async fn async_read<'o>(&mut self, options: &'o SqlReadOptions) -> FabrixResult<Fabrix>
    where
        'o: 'a,
    {
        todo!()
    }

    fn sync_read<'o>(&mut self, options: &'o SqlReadOptions) -> FabrixResult<Fabrix>
    where
        'o: 'a,
    {
        todo!()
    }
}

#[cfg(test)]
mod test_sql_reader {
    // TODO:
}
