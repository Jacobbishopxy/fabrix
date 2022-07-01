//! MongoDB Executor
//!
//! mongodb

use bson::{oid::ObjectId, Bson, Document};
use futures::TryStreamExt;
use mongodb::IndexModel as MongoIndexModel;

use crate::{BaseCRUD, MgError, MgResult};

pub struct Oid(ObjectId);

impl Oid {
    pub fn new(oid: ObjectId) -> Self {
        Oid(oid)
    }

    pub fn id(&self) -> &ObjectId {
        &self.0
    }
}

impl TryFrom<Bson> for Oid {
    type Error = MgError;

    fn try_from(value: Bson) -> Result<Self, Self::Error> {
        value.as_object_id().ok_or(MgError::OidParseFailed).map(Oid)
    }
}

impl TryFrom<Oid> for ObjectId {
    type Error = MgError;

    fn try_from(value: Oid) -> Result<Self, Self::Error> {
        Ok(value.0)
    }
}

impl TryFrom<ObjectId> for Oid {
    type Error = MgError;

    fn try_from(value: ObjectId) -> Result<Self, Self::Error> {
        Ok(Oid(value))
    }
}

impl TryFrom<&str> for Oid {
    type Error = MgError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(Oid(ObjectId::parse_str(value)?))
    }
}

/// MongoDB client
#[derive(Clone)]
pub struct MongoExecutor {
    pub(crate) client: Option<mongodb::Client>,
    pub(crate) conn_str: String,
    pub database: Option<String>,
    pub collection: Option<String>,
}

/// connection already established error.
macro_rules! conn_e_err {
    ($client:expr) => {
        if $client.is_some() {
            return Err($crate::MgError::ConnectionAlreadyEstablished);
        }
    };
}

/// connection not yet established error.
macro_rules! conn_n_err {
    ($client:expr) => {
        if $client.is_none() {
            return Err($crate::MgError::ConnectionNotYetEstablished);
        }
    };
}

/// database or collection not set error.
macro_rules! db_ns_err {
    ($database:expr) => {
        if $database.is_none() {
            return Err($crate::MgError::DatabaseOrCollectionNotSet);
        }
    };
}

impl MongoExecutor {
    /// Create a new MongoDB client
    pub fn new<T>(conn_str: T) -> Self
    where
        T: Into<String>,
    {
        MongoExecutor {
            client: None,
            conn_str: conn_str.into(),
            database: None,
            collection: None,
        }
    }

    pub async fn connect(&mut self) -> MgResult<()> {
        conn_e_err!(self.client);

        let co = mongodb::options::ClientOptions::parse(&self.conn_str).await?;
        let client = mongodb::Client::with_options(co)?;
        self.client = Some(client);

        Ok(())
    }

    pub async fn new_and_connect<T: Into<String>>(
        conn_str: T,
        database: T,
        collection: T,
    ) -> MgResult<Self> {
        let mut ec = Self::new(conn_str);
        ec.connect().await?;
        ec.set_database(database)?;
        ec.set_collection(collection)?;

        Ok(ec)
    }

    pub fn is_connected(&self) -> bool {
        self.client.is_some()
    }

    /// set the database
    pub fn set_database<T: Into<String>>(&mut self, database: T) -> MgResult<()> {
        conn_n_err!(self.client);
        self.database = Some(database.into());
        Ok(())
    }

    /// set the collection
    pub fn set_collection<T: Into<String>>(&mut self, collection: T) -> MgResult<()> {
        conn_n_err!(self.client);
        self.collection = Some(collection.into());
        Ok(())
    }

    /// show databases name
    pub async fn show_dbs(&self) -> MgResult<Vec<String>> {
        conn_n_err!(self.client);
        let dbs = self
            .client
            .as_ref()
            .unwrap()
            .list_database_names(None, None)
            .await?;
        Ok(dbs)
    }

    /// show collections name in a database
    pub async fn show_collections(&self) -> MgResult<Vec<String>> {
        conn_n_err!(self.client);
        db_ns_err!(self.database);
        let collections = self
            .client
            .as_ref()
            .unwrap()
            .database(self.database.as_ref().unwrap())
            .list_collection_names(None)
            .await?;
        Ok(collections)
    }

    /// list all indexes in a collection
    /// T is the type of the document
    pub async fn list_indexes(&self) -> MgResult<Vec<MongoIndexModel>> {
        conn_n_err!(self.client);
        let idx = self
            .schema::<Document>()?
            .list_indexes(None)
            .await?
            .try_collect()
            .await?;
        Ok(idx)
    }

    /// list all indexes name
    pub async fn list_indexes_name(&self) -> MgResult<Vec<String>> {
        conn_n_err!(self.client);
        let result = self.schema::<Document>()?.list_index_names().await?;
        Ok(result)
    }

    /// create index
    pub async fn create_index(&self, index: MongoIndexModel) -> MgResult<String> {
        conn_n_err!(self.client);
        let result = self.schema::<Document>()?.create_index(index, None).await?;
        Ok(result.index_name)
    }

    /// Create indexes by `T
    pub async fn create_indexes_by_type<T: BaseCRUD>(&self) -> MgResult<Vec<String>> {
        conn_n_err!(self.client);
        let indexes = T::show_indexes();

        let index_models = indexes.generate_mongo_index_module().into_iter();
        let mut result = vec![];

        for im in index_models {
            let ci = self.schema::<T>()?.create_index(im, None).await?;
            result.push(ci.index_name);
        }

        Ok(result)
    }

    /// drop index
    pub async fn drop_index(&self, index_name: &str) -> MgResult<()> {
        conn_n_err!(self.client);
        self.schema::<Document>()?
            .drop_index(index_name, None)
            .await?;
        Ok(())
    }

    /// drop all indexes, except `_id_`
    pub async fn drop_all_indexes(&self) -> MgResult<()> {
        conn_n_err!(self.client);
        self.schema::<Document>()?.drop_indexes(None).await?;
        Ok(())
    }

    /// drop current database
    pub async fn drop_database(&self) -> MgResult<()> {
        conn_n_err!(self.client);
        db_ns_err!(self.database);
        self.client
            .as_ref()
            .unwrap()
            .database(self.database.as_ref().unwrap())
            .drop(None)
            .await?;
        Ok(())
    }

    /// drop collection
    pub async fn drop_collection(&self, collection_name: &str) -> MgResult<()> {
        conn_n_err!(self.client);
        db_ns_err!(self.database);
        self.client
            .as_ref()
            .unwrap()
            .database(self.database.as_ref().unwrap())
            .collection::<Document>(collection_name)
            .drop(None)
            .await?;
        Ok(())
    }
}

pub trait MongoBaseEc: Send + Sync {
    /// get connection string
    fn conn_str(&self) -> &str;

    /// get database
    fn database(&self) -> Option<&str>;

    /// set database
    fn set_database(&mut self, database: &str);

    /// get collection
    fn collection(&self) -> Option<&str>;

    /// set collection
    fn set_collection(&mut self, collection: &str);

    /// get typed collection
    fn schema<T>(&self) -> MgResult<mongodb::Collection<T>>;
}

impl MongoBaseEc for MongoExecutor {
    fn conn_str(&self) -> &str {
        &self.conn_str
    }

    fn database(&self) -> Option<&str> {
        self.database.as_deref()
    }

    fn set_database(&mut self, database: &str) {
        self.database = Some(database.to_string());
    }

    fn collection(&self) -> Option<&str> {
        self.collection.as_deref()
    }

    fn set_collection(&mut self, collection: &str) {
        self.collection = Some(collection.to_string());
    }

    fn schema<T>(&self) -> MgResult<mongodb::Collection<T>> {
        conn_n_err!(self.client);
        db_ns_err!(self.database);
        db_ns_err!(self.collection);

        let s = self
            .client
            .as_ref()
            .unwrap()
            .database(self.database.as_ref().unwrap())
            .collection(self.collection.as_ref().unwrap());

        Ok(s)
    }
}

#[cfg(test)]
mod ec_tests {
    use super::*;

    const CONN: &str = "mongodb://root:secret@localhost:27017";
    const DB: &str = "dev";
    const CL: &str = "dev";

    #[tokio::test]
    async fn connection_and_show_dbs_success() {
        let ec = MongoExecutor::new_and_connect(CONN, DB, CL).await;
        assert!(ec.is_ok());

        println!("{:?}", ec.unwrap().show_dbs().await);
    }
}
