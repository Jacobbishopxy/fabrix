//! Dynamic
//!
//! query & mutation

use std::borrow::Borrow;

use async_trait::async_trait;
use bson::oid::ObjectId;
use bson::{doc, Bson, Document};
use fabrix_core::Fabrix;
use futures::{StreamExt, TryStreamExt};
use mongodb::options::UpdateModifications;
use mongodb::results::{DeleteResult, InsertManyResult, InsertOneResult, UpdateResult};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::{MgError, MgResult, MongoEc, MongoExecutor};

#[async_trait(?Send)]
pub trait RawEc: MongoEc {
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
impl RawEc for MongoExecutor {
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

#[async_trait(?Send)]
pub trait Ec: RawEc {
    async fn delete_by_id<I>(&self, id: I) -> MgResult<()>
    where
        I: TryInto<ObjectId, Error = MgError>,
    {
        self.find_one_and_delete(doc! {"_id": id.try_into()?})
            .await?;
        Ok(())
    }

    async fn delete_by_ids<I, E>(&self, ids: I) -> MgResult<()>
    where
        I: IntoIterator<Item = E>,
        E: TryInto<ObjectId, Error = MgError>,
    {
        let ids = ids
            .into_iter()
            .map(|e| -> MgResult<ObjectId> { e.try_into() })
            .collect::<MgResult<Vec<_>>>()?;
        self.delete_many(doc! {"_id": { "$in": ids }}).await?;

        Ok(())
    }

    async fn update_by_id<I>(&self, id: I, data: &Fabrix) -> MgResult<()>
    where
        I: TryInto<ObjectId, Error = MgError>,
    {
        let d = bson::to_document(data)?;
        self.find_one_and_update(
            doc! {"_id": id.try_into()?},
            UpdateModifications::Document(d),
        )
        .await?;

        Ok(())
    }

    async fn find_by_id<I>(&self, id: I) -> MgResult<Fabrix>
    where
        I: TryInto<ObjectId, Error = MgError>,
    {
        self.find_one::<Fabrix>(doc! {"_id": id.try_into()?}).await
    }

    async fn find_by_ids<I, E>(&self, ids: I) -> MgResult<Vec<Fabrix>>
    where
        I: IntoIterator<Item = E>,
        E: TryInto<ObjectId, Error = MgError>,
    {
        let ids = ids
            .into_iter()
            .map(|e| -> MgResult<ObjectId> { e.try_into() })
            .collect::<MgResult<Vec<_>>>()?;
        self.find_many(doc! {"_id": { "$in": ids }}).await
    }

    async fn replace_by_id<I>(&self, id: I, data: &Fabrix) -> MgResult<()>
    where
        I: TryInto<ObjectId, Error = MgError>,
    {
        let d = bson::to_document(data)?;
        self.find_one_and_replace(doc! {"_id": id.try_into()?}, d)
            .await?;
        Ok(())
    }
}

#[async_trait(?Send)]
impl Ec for MongoExecutor {}

#[cfg(test)]
mod dy_tests {
    use super::*;

    use bson::doc;
    use fabrix_core::{fx, Fabrix};

    const CONN: &str = "mongodb://root:secret@localhost:27017";
    const DB: &str = "dev";
    const CL: &str = "dev";

    #[tokio::test]
    async fn insert_one_and_find_one_by_raw_ec_success() {
        let ec = MongoExecutor::new(CONN, DB, CL)
            .await
            .expect("connection failed");

        let df = fx![
            "ord";
            "names" => ["Jacob", "Sam", "Jason"],
            "ord" => [1,2,3],
            "val" => [Some(10), None, Some(8)]
        ]
        .unwrap();

        let foo = ec.insert_one::<Fabrix>(&df).await;
        assert!(foo.is_ok());

        let id = foo.unwrap().inserted_id;
        println!("{:?}", id);

        let bar = ec.find_one::<Fabrix>(doc! {"_id": id}).await;
        assert!(bar.is_ok());
        println!("{:?}", bar.unwrap());
    }

    // TODO:
    // test `Ec` methods
}
