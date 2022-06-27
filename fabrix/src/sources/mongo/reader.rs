//! Mongo Reader
//!
//! Reading from MongoDB

use async_trait::async_trait;
use fabrix_core::Fabrix;
use fabrix_mg::{Ec, MongoExecutor};

use crate::{FabrixError, FabrixResult, FromSource, ReadOptions};

// ================================================================================================
// Mongo Reader
// ================================================================================================

pub struct Reader<'a> {
    mg_reader: MongoExecutor,
    id: Option<&'a str>,
}

impl<'a> Reader<'a> {
    pub async fn new(
        conn: &str,
        database: &'a str,
        collection: &'a str,
    ) -> FabrixResult<Reader<'a>> {
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

    pub async fn finish(&mut self) -> FabrixResult<Fabrix> {
        let id = self.id.ok_or(FabrixError::NotSet("it"))?;

        let fx = self.mg_reader.find_fx(id).await?;

        Ok(fx)
    }
}

// ================================================================================================
// Mongo read options & FromSource impl
// ================================================================================================

#[derive(Default)]
pub struct MongoReadOptions<'a> {
    pub database: Option<&'a str>,
    pub collection: Option<&'a str>,
    pub id: Option<&'a str>,
}

impl<'a> ReadOptions for MongoReadOptions<'a> {
    fn source_type() -> &'static str {
        "mongo"
    }
}

#[async_trait]
impl<'a> FromSource<'a, MongoReadOptions<'a>> for Reader<'a> {
    async fn async_read<'o>(&mut self, options: &'o MongoReadOptions) -> FabrixResult<Fabrix>
    where
        'o: 'a,
    {
        let MongoReadOptions {
            database,
            collection,
            id,
        } = options;

        if let Some(database) = database {
            self.with_database(database);
        }
        if let Some(collection) = collection {
            self.with_collection(collection);
        }
        if let Some(id) = id {
            self.with_id(id);
        }

        self.finish().await
    }

    fn sync_read<'o>(&mut self, _options: &'o MongoReadOptions) -> FabrixResult<Fabrix>
    where
        'o: 'a,
    {
        unimplemented!("sync_read is not allowed in mongo reader")
    }
}
