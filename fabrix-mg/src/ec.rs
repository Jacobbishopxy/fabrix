//! MongoDB Executor
//!
//! mongodb

use bson::Document;
use futures::TryStreamExt;
use mongodb::IndexModel as MongoIndexModel;

use crate::{BaseCRUD, MgResult, MongoEc};

/// MongoDB client
#[derive(Clone)]
pub struct MongoExecutor {
    pub(crate) client: mongodb::Client,
    pub database: String,
    pub collection: String,
}

impl MongoExecutor {
    /// Create a new MongoDB client
    /// Database and collection names are required, and they can be switched later.
    pub async fn new<U, T>(uri: U, database: T, collection: T) -> MgResult<Self>
    where
        U: AsRef<str>,
        T: Into<String>,
    {
        let co = mongodb::options::ClientOptions::parse(uri.as_ref()).await?;

        let client = mongodb::Client::with_options(co)?;

        Ok(MongoExecutor {
            client,
            database: database.into(),
            collection: collection.into(),
        })
    }

    /// set the database
    pub fn set_database<T: Into<String>>(&mut self, database: T) {
        self.database = database.into();
    }

    /// set the collection
    pub fn set_collection<T: Into<String>>(&mut self, collection: T) {
        self.collection = collection.into();
    }

    /// show databases name
    pub async fn show_dbs(&self) -> MgResult<Vec<String>> {
        let dbs = self.client.list_database_names(None, None).await?;
        Ok(dbs)
    }

    /// show collections name in a database
    pub async fn show_collections(&self) -> MgResult<Vec<String>> {
        let collections = self
            .client
            .database(&self.database)
            .list_collection_names(None)
            .await?;
        Ok(collections)
    }

    /// list all indexes in a collection
    /// T is the type of the document
    pub async fn list_indexes(&self) -> MgResult<Vec<MongoIndexModel>> {
        let idx = self
            .schema::<Document>()
            .list_indexes(None)
            .await?
            .try_collect()
            .await?;
        Ok(idx)
    }

    /// list all indexes name
    pub async fn list_indexes_name(&self) -> MgResult<Vec<String>> {
        Ok(self.schema::<Document>().list_index_names().await?)
    }

    /// create index
    pub async fn create_index(&self, index: MongoIndexModel) -> MgResult<String> {
        let result = self.schema::<Document>().create_index(index, None).await?;
        Ok(result.index_name)
    }

    /// Create indexes by `T
    pub async fn create_indexes_by_type<T: BaseCRUD>(&self) -> MgResult<Vec<String>> {
        let indexes = T::show_indexes();

        let index_models = indexes.generate_mongo_index_module().into_iter();
        let mut result = vec![];

        for im in index_models {
            let ci = self.schema::<T>().create_index(im, None).await?;
            result.push(ci.index_name);
        }

        Ok(result)
    }

    /// drop index
    pub async fn drop_index(&self, index_name: &str) -> MgResult<()> {
        self.schema::<Document>()
            .drop_index(index_name, None)
            .await?;
        Ok(())
    }

    /// drop all indexes, except `_id_`
    pub async fn drop_all_indexes(&self) -> MgResult<()> {
        self.schema::<Document>().drop_indexes(None).await?;
        Ok(())
    }
}
