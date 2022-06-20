//! MongoDB Executor
//!
//! mongodb

use std::borrow::Cow;

use async_trait::async_trait;
use bson::{doc, oid::ObjectId, to_document, Document};
use futures::TryStreamExt;
use mongodb::{options::IndexOptions as MongoIndexOptions, IndexModel as MongoIndexModel};
use serde::{de::DeserializeOwned, Serialize};

use crate::{MgError, MgResult};

const INDEXES_PREFIX: &str = "idx";

/// MongoDB client
#[derive(Clone)]
pub struct MongoExecutor {
    client: mongodb::Client,
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

        let index_models = generate_mongo_index_module(&indexes).into_iter();
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

pub trait MongoEc: Send + Sync {
    /// get database
    fn database(&self) -> Cow<str>;

    /// set database
    fn set_database(&mut self, database: &str);

    /// get collection
    fn collection(&self) -> Cow<str>;

    /// set collection
    fn set_collection(&mut self, collection: &str);

    /// get typed collection
    fn schema<T>(&self) -> mongodb::Collection<T>;
}

impl MongoEc for MongoExecutor {
    fn database(&self) -> Cow<str> {
        Cow::Borrowed(&self.database)
    }

    fn set_database(&mut self, database: &str) {
        self.database = database.to_string();
    }

    fn collection(&self) -> Cow<str> {
        Cow::Borrowed(&self.collection)
    }

    fn set_collection(&mut self, collection: &str) {
        self.collection = collection.to_string();
    }

    fn schema<T>(&self) -> mongodb::Collection<T> {
        self.client
            .database(&self.database)
            .collection(&self.collection)
    }
}

pub trait MongoClientFactory {
    fn client(&self) -> &MongoExecutor;
}

impl MongoClientFactory for MongoExecutor {
    fn client(&self) -> &MongoExecutor {
        self
    }
}

#[derive(Debug, Clone)]
pub enum Dir {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
pub struct SingleIndex {
    pub key: (String, Dir),
    pub unique: bool,
    pub text: bool,
}

impl SingleIndex {
    pub fn new(key: (String, Dir), unique: bool, text: bool) -> Self {
        SingleIndex { key, unique, text }
    }
}

#[derive(Debug, Clone)]
pub struct SingleIndexOptions(pub Vec<SingleIndex>);

#[derive(Debug, Clone)]
pub struct CompoundIndexOptions {
    pub keys: Vec<(String, Dir)>,
    pub unique: bool,
    pub text: bool,
}

impl CompoundIndexOptions {
    pub fn new(keys: Vec<(String, Dir)>, unique: bool, text: bool) -> Self {
        CompoundIndexOptions { keys, unique, text }
    }
}

/// index options represent indexes in a collection, the default `_id` index is not included.
#[derive(Debug)]
pub enum IndexOptions {
    Single(SingleIndexOptions),
    Compound(CompoundIndexOptions),
    None,
}

/// Turn `IndexOptions` into `Vec<mongodb::MongoIndexModel>`.
/// Both single-index and compound-index are named in `MongoIndexOptions`.
fn generate_mongo_index_module(indexes: &IndexOptions) -> Vec<MongoIndexModel> {
    match indexes {
        IndexOptions::Single(s) => {
            s.0.iter()
                .map(|si| {
                    let name = si.key.0.to_owned();
                    let dir: i32 = match si.key.1 {
                        Dir::Asc => 1,
                        Dir::Desc => -1,
                    };
                    let unique = si.unique;
                    // let text = si.text;

                    let mio = MongoIndexOptions::builder()
                        .name(format!("_{}_{}", INDEXES_PREFIX, name))
                        .unique(unique)
                        .build();
                    MongoIndexModel::builder()
                        .keys(doc! { name : dir })
                        .options(mio)
                        .build()
                })
                .collect()
        }
        IndexOptions::Compound(c) => {
            let unique = c.unique;
            // let text = c.text;

            let mut indexes_name = String::new();
            let keys = c.keys.iter().fold(doc! {}, |mut acc, (name, dir)| {
                indexes_name.push_str(name);
                indexes_name.push('_');
                let dir: i32 = match dir {
                    Dir::Asc => 1,
                    Dir::Desc => -1,
                };
                acc.extend(doc! { name.to_owned() : dir });
                acc
            });

            let mio = MongoIndexOptions::builder()
                .name(format!("_{}_{}", INDEXES_PREFIX, indexes_name))
                .unique(unique)
                .build();
            let im = MongoIndexModel::builder().keys(keys).options(mio).build();

            vec![im]
        }
        IndexOptions::None => vec![],
    }
}

/// BaseCRUD trait
///
/// A Rust struct that implements this trait is a schema of MongoDB's collection.
/// According to the `crud` crate, any struct who derived `CRUD` will automatically implement this trait.
pub trait BaseCRUD {
    fn get_id(&self) -> Option<ObjectId>;

    fn remove_id(&mut self);

    fn mutate_id(&mut self, oid: ObjectId);

    /// Show `IndexOptions`, associate function.
    /// Automatically generated by `crud_derive`
    fn show_indexes() -> IndexOptions;
}

/// MongoCRUD trait
///
/// According to `crud` crate, any struct who derived `CRUD` will automatically implement this trait.
/// In other words, `MongoClient` can use methods in this trait to persist `TYPE` data.
#[async_trait]
pub trait MongoCRUD<TYPE>: MongoEc
where
    TYPE: Send + Sync + Clone + Serialize + DeserializeOwned + Unpin + BaseCRUD,
{
    /// Create a new document
    async fn create<'a>(&'a self, mut value: TYPE) -> MgResult<TYPE>
    where
        TYPE: 'a,
    {
        // in case of `id` field exists, we need to remove it
        value.remove_id();
        let insert = self
            .schema::<TYPE>()
            .insert_one(value.clone(), None)
            .await?;
        let oid = insert.inserted_id.as_object_id().unwrap();
        value.mutate_id(oid);
        Ok(value)
    }

    /// Read a document by id
    async fn read<'a>(&'a self, id: ObjectId) -> MgResult<Option<TYPE>>
    where
        TYPE: 'a,
    {
        let filter = doc! { "_id": id };
        let result = self.schema::<TYPE>().find_one(filter, None).await?;
        Ok(result)
    }

    /// Read many documents by ids
    async fn read_many<'a>(&'a self, ids: Vec<ObjectId>) -> MgResult<Vec<TYPE>>
    where
        TYPE: 'a,
    {
        let filter = doc! { "_id": { "$in": ids } };
        let res = self
            .schema::<TYPE>()
            .find(filter, None)
            .await?
            .try_collect()
            .await?;

        Ok(res)
    }

    /// Read all documents
    async fn read_all<'a>(&'a self) -> MgResult<Vec<TYPE>>
    where
        TYPE: 'a,
    {
        let res = self
            .schema::<TYPE>()
            .find(None, None)
            .await?
            .try_collect()
            .await?;

        Ok(res)
    }

    /// Update an existing document
    async fn update<'a>(&'a self, value: TYPE) -> MgResult<TYPE>
    where
        TYPE: 'a,
    {
        let oid = value.get_id().ok_or(MgError::OidNotFound)?;
        let filter = doc! {"_id": oid};
        let update = doc! {"$set": to_document(&value).unwrap()};
        self.schema::<TYPE>()
            .update_one(filter, update, None)
            .await?;
        Ok(value)
    }

    /// Delete an existing document
    async fn delete<'a>(&'a self, id: ObjectId) -> MgResult<Option<TYPE>>
    where
        TYPE: 'a,
    {
        let filter = doc! {"_id": id};
        let result = self
            .schema::<TYPE>()
            .find_one_and_delete(filter, None)
            .await?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;
    use crate::CRUD;

    const CONN: &str = "mongodb://root:secret@localhost:27017";
    const DB: &str = "dev";
    const CL: &str = "dev";

    #[derive(Serialize, Deserialize, Clone, Debug, CRUD)]
    struct Dev {
        pub id: Option<ObjectId>,
        pub name: String,
        pub age: i32,
    }

    #[tokio::test]
    async fn connection_success() {
        let ec = MongoExecutor::new(CONN, DB, CL).await;

        assert!(ec.is_ok());
    }

    #[tokio::test]
    async fn read_all_success() {
        let ec = MongoExecutor::new(CONN, DB, CL)
            .await
            .expect("connection failed");

        let foo = ec.read_all().await;
        println!("{:?}", foo);
    }
}
