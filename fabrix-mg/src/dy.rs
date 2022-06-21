//! Dynamic
//!
//! query & mutation

use std::borrow::Borrow;

use async_trait::async_trait;
use bson::{Bson, Document};
use futures::{StreamExt, TryStreamExt};
use mongodb::options::UpdateModifications;
use mongodb::results::{DeleteResult, InsertManyResult, InsertOneResult, UpdateResult};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::{MgError, MgResult, MongoEc, MongoExecutor};

#[async_trait(?Send)]
pub trait Ec: MongoEc {
    async fn delete_one(&self, query: Document) -> MgResult<DeleteResult>;

    async fn delete_many(&self, query: Document) -> MgResult<DeleteResult>;

    async fn distinct(
        &self,
        field_name: impl AsRef<str>,
        filter: impl Into<Option<Document>>,
    ) -> MgResult<Vec<Bson>>;

    async fn update_one(
        &self,
        query: Document,
        update: impl Into<UpdateModifications>,
    ) -> MgResult<UpdateResult>;

    async fn update_many(
        &self,
        query: Document,
        update: impl Into<UpdateModifications>,
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

    async fn find_one_and_replace<T>(
        &self,
        query: Document,
        replace: impl Borrow<T>,
    ) -> MgResult<T>
    where
        T: Serialize + DeserializeOwned;

    async fn insert_one<T>(&self, doc: impl Borrow<T>) -> MgResult<InsertOneResult>
    where
        T: Serialize + Unpin + Send + Sync;

    async fn insert_many<T>(
        &self,
        doc: impl IntoIterator<Item = impl Borrow<T>>,
    ) -> MgResult<InsertManyResult>
    where
        T: Serialize + Unpin + Send + Sync;

    async fn replace_one<T>(
        &self,
        query: Document,
        replace: impl Borrow<T>,
    ) -> MgResult<UpdateResult>
    where
        T: Serialize;
}

#[async_trait(?Send)]
impl Ec for MongoExecutor {
    async fn delete_one(&self, query: Document) -> MgResult<DeleteResult> {
        Ok(self.schema::<Document>().delete_one(query, None).await?)
    }

    async fn delete_many(&self, query: Document) -> MgResult<DeleteResult> {
        Ok(self.schema::<Document>().delete_many(query, None).await?)
    }

    async fn distinct(
        &self,
        field_name: impl AsRef<str>,
        filter: impl Into<Option<Document>>,
    ) -> MgResult<Vec<Bson>> {
        Ok(self
            .schema::<Document>()
            .distinct(field_name, filter, None)
            .await?)
    }

    async fn update_one(
        &self,
        query: Document,
        update: impl Into<UpdateModifications>,
    ) -> MgResult<UpdateResult> {
        Ok(self
            .schema::<Document>()
            .update_one(query, update, None)
            .await?)
    }

    async fn update_many(
        &self,
        query: Document,
        update: impl Into<UpdateModifications>,
    ) -> MgResult<UpdateResult> {
        Ok(self
            .schema::<Document>()
            .update_many(query, update, None)
            .await?)
    }

    async fn find_one<T>(&self, query: Document) -> MgResult<T>
    where
        T: DeserializeOwned + Unpin + Send + Sync,
    {
        self.schema::<T>()
            .find_one(query, None)
            .await?
            .ok_or(MgError::ResultNotFound)
    }

    async fn find_many<T>(&self, query: Document) -> MgResult<Vec<T>>
    where
        T: DeserializeOwned + Unpin + Send + Sync,
    {
        Ok(self
            .schema::<T>()
            .find(query, None)
            .await?
            .map(|v| v.map_err(MgError::from))
            .try_collect::<Vec<_>>()
            .await?)
    }

    async fn find_one_and_delete<T>(&self, query: Document) -> MgResult<T>
    where
        T: DeserializeOwned + Unpin + Send + Sync,
    {
        self.schema::<T>()
            .find_one_and_delete(query, None)
            .await?
            .ok_or(MgError::ResultNotFound)
    }

    async fn find_one_and_update<T>(
        &self,
        query: Document,
        update: UpdateModifications,
    ) -> MgResult<T>
    where
        T: DeserializeOwned + Unpin + Send + Sync,
    {
        self.schema::<T>()
            .find_one_and_update(query, update, None)
            .await?
            .ok_or(MgError::ResultNotFound)
    }

    async fn find_one_and_replace<T>(&self, query: Document, replace: impl Borrow<T>) -> MgResult<T>
    where
        T: Serialize + DeserializeOwned,
    {
        self.schema::<T>()
            .find_one_and_replace(query, replace, None)
            .await?
            .ok_or(MgError::ResultNotFound)
    }

    async fn insert_one<T>(&self, doc: impl Borrow<T>) -> MgResult<InsertOneResult>
    where
        T: Serialize + Unpin + Send + Sync,
    {
        Ok(self.schema::<T>().insert_one(doc, None).await?)
    }

    async fn insert_many<T>(
        &self,
        doc: impl IntoIterator<Item = impl Borrow<T>>,
    ) -> MgResult<InsertManyResult>
    where
        T: Serialize + Unpin + Send + Sync,
    {
        Ok(self.schema::<T>().insert_many(doc, None).await?)
    }

    async fn replace_one<T>(
        &self,
        query: Document,
        replace: impl Borrow<T>,
    ) -> MgResult<UpdateResult>
    where
        T: Serialize,
    {
        Ok(self.schema::<T>().replace_one(query, replace, None).await?)
    }
}
