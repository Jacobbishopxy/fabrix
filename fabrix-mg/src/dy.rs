//! Dynamic
//!
//! query & mutation

use std::borrow::Borrow;

use async_trait::async_trait;
use bson::{Bson, Document};
use mongodb::options::UpdateModifications;
use mongodb::results::{DeleteResult, InsertOneResult, UpdateResult};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::MgResult;

#[async_trait]
pub trait Ec {
    async fn delete_one(&self, query: Document) -> MgResult<DeleteResult>;

    async fn delete_many(&self, query: Document) -> MgResult<DeleteResult>;

    async fn distinct(&self, field_name: &str, filter: Option<Document>) -> MgResult<Vec<Bson>>;

    async fn update_one(
        &self,
        query: Document,
        update: UpdateModifications,
    ) -> MgResult<UpdateResult>;

    async fn update_many(
        &self,
        query: Document,
        update: UpdateModifications,
    ) -> MgResult<UpdateResult>;

    async fn find_one<T>(&self, query: Document) -> MgResult<T>
    where
        T: DeserializeOwned + Unpin + Send + Sync;

    async fn find_many<T>(&self, query: Document) -> MgResult<Vec<T>>
    where
        T: DeserializeOwned + Unpin + Send + Sync;

    async fn find_one_and_delete<T>(&self, query: Document) -> MgResult<T>
    where
        T: DeserializeOwned + Unpin + Send + Sync;

    async fn find_one_and_update<T>(
        &self,
        query: Document,
        update: UpdateModifications,
    ) -> MgResult<T>
    where
        T: DeserializeOwned + Unpin + Send + Sync;

    async fn find_one_and_replace<T>(&self, query: Document, replace: Document) -> MgResult<T>
    where
        T: DeserializeOwned + Unpin + Send + Sync;

    async fn insert_one<T>(&self, doc: impl Borrow<T>) -> MgResult<InsertOneResult>
    where
        T: Serialize + Unpin + Send + Sync;

    async fn insert_many<T>(
        &self,
        doc: impl IntoIterator<Item = impl Borrow<T>>,
    ) -> MgResult<InsertOneResult>
    where
        T: Serialize + Unpin + Send + Sync;

    async fn replace_one<T>(
        &self,
        query: Document,
        replace: impl Borrow<T>,
    ) -> MgResult<UpdateResult>
    where
        T: Serialize + Unpin + Send + Sync;
}
