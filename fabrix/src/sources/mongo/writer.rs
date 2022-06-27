//! Mongo Writer
//!
//! Writing into MongoDB

// ================================================================================================
// Mongo Writer
// ================================================================================================

use fabrix_core::Fabrix;
use fabrix_mg::{Ec, MongoExecutor};

use crate::FabrixResult;

pub struct Writer<'a> {
    mg_reader: MongoExecutor,
    id: Option<&'a str>,
}

impl<'a> Writer<'a> {
    pub async fn new(
        conn: &str,
        database: &'a str,
        collection: &'a str,
    ) -> FabrixResult<Writer<'a>> {
        let ec = MongoExecutor::new(conn, database, collection).await?;

        Ok(Self {
            mg_reader: ec,
            id: None,
        })
    }

    pub fn with_database(&mut self, database: &str) -> &mut Self {
        self.mg_reader.set_database(database.to_string());
        self
    }

    pub fn with_collection(&mut self, collection: &str) -> &mut Self {
        self.mg_reader.set_collection(collection.to_string());
        self
    }

    pub fn with_id(&mut self, id: &'a str) -> &mut Self {
        self.id = Some(id);
        self
    }

    pub async fn finish(&mut self, fabrix: Fabrix) -> FabrixResult<()> {
        if let Some(id) = self.id {
            self.mg_reader.update_fx(id, &fabrix).await?;
        } else {
            self.mg_reader.insert_fx(&fabrix).await?;
        }

        Ok(())
    }
}

// ================================================================================================
// Mongo write options & FromSource impl
// ================================================================================================
