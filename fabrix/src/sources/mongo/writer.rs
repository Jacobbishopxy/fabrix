//! Mongo Writer
//!
//! Writing into MongoDB

// ================================================================================================
// Mongo Writer
// ================================================================================================

use async_trait::async_trait;
use fabrix_core::Fabrix;
use fabrix_mg::{MongoEc, MongoExecutor, SavingCategory};

use crate::{FabrixResult, IntoSource, WriteOptions};

pub struct Writer<'a> {
    mg_reader: MongoExecutor,
    saving_category: Option<&'a SavingCategory>,
    id: Option<&'a str>,
}

impl<'a> Writer<'a> {
    pub async fn new(
        conn: &str,
        database: &'a str,
        collection: &'a str,
    ) -> FabrixResult<Writer<'a>> {
        let ec = MongoExecutor::new_and_connect(conn, database, collection).await?;

        Ok(Self {
            mg_reader: ec,
            saving_category: None,
            id: None,
        })
    }

    pub fn with_database(&mut self, database: &str) -> &mut Self {
        self.mg_reader.set_database(database.to_string()).unwrap();
        self
    }

    pub fn with_collection(&mut self, collection: &str) -> &mut Self {
        self.mg_reader
            .set_collection(collection.to_string())
            .unwrap();
        self
    }

    pub fn with_saving_category(&mut self, saving_category: &'a SavingCategory) -> &mut Self {
        self.saving_category = Some(saving_category);
        self
    }

    pub fn with_id(&mut self, id: &'a str) -> &mut Self {
        self.id = Some(id);
        self
    }

    pub async fn finish(&mut self, fabrix: Fabrix) -> FabrixResult<()> {
        let saving_category = self
            .saving_category
            .unwrap_or(&SavingCategory::Column)
            .clone();
        if let Some(id) = self.id {
            self.mg_reader
                .update_fx(id, &fabrix, saving_category)
                .await?;
        } else {
            self.mg_reader.insert_fx(&fabrix, saving_category).await?;
        }

        Ok(())
    }
}

// ================================================================================================
// Mongo write options & FromSource impl
// ================================================================================================

#[derive(Default)]
pub struct MongoWriteOptions<'a> {
    pub database: Option<&'a str>,
    pub collection: Option<&'a str>,
    pub saving_category: Option<&'a SavingCategory>,
    pub id: Option<&'a str>,
}

impl<'a> WriteOptions for MongoWriteOptions<'a> {
    fn source_type() -> &'static str {
        "mongo"
    }
}

#[async_trait]
impl<'a> IntoSource<'a, MongoWriteOptions<'a>> for Writer<'a> {
    async fn async_write<'o>(
        &mut self,
        fabrix: Fabrix,
        options: &'o MongoWriteOptions,
    ) -> FabrixResult<()>
    where
        'o: 'a,
    {
        let MongoWriteOptions {
            database,
            collection,
            saving_category,
            id,
        } = options;

        if let Some(database) = database {
            self.with_database(database);
        }
        if let Some(collection) = collection {
            self.with_collection(collection);
        }
        if let Some(saving_category) = saving_category {
            self.with_saving_category(saving_category);
        }
        if let Some(id) = id {
            self.with_id(id);
        }

        self.finish(fabrix).await
    }

    fn sync_write<'o>(
        &mut self,
        _fabrix: Fabrix,
        _options: &'o MongoWriteOptions,
    ) -> FabrixResult<()>
    where
        'o: 'a,
    {
        unimplemented!("sync_write is not allowed in mongo writer")
    }
}
