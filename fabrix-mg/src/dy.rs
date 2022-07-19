//! Dynamic
//!
//! query & mutation

use std::borrow::Borrow;
use std::collections::HashMap;

use async_trait::async_trait;
use bson::oid::ObjectId;
use bson::{doc, Bson, Document};
use fabrix_core::Fabrix;
use fabrix_json::{FabrixColumnWised, JsonType};
use futures::{StreamExt, TryStreamExt};
use mongodb::options::UpdateModifications;
use mongodb::results::{DeleteResult, InsertManyResult, InsertOneResult, UpdateResult};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::{MgError, MgResult, MongoBaseEc, MongoExecutor, Oid};

#[async_trait]
pub trait MongoRawEc: MongoBaseEc {
    async fn delete_one(&self, query: Document) -> MgResult<DeleteResult>;

    async fn delete_many(&self, query: Document) -> MgResult<DeleteResult>;

    async fn distinct(
        &self,
        field_name: impl AsRef<str> + Send,
        filter: impl Into<Option<Document>> + Send,
    ) -> MgResult<Vec<Bson>>;

    async fn update_one(
        &self,
        query: Document,
        update: impl Into<UpdateModifications> + Send,
    ) -> MgResult<UpdateResult>;

    async fn update_many(
        &self,
        query: Document,
        update: impl Into<UpdateModifications> + Send,
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
        replace: impl Borrow<T> + Send,
    ) -> MgResult<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync;

    async fn insert_one<T>(&self, doc: impl Borrow<T> + Send + Sync) -> MgResult<InsertOneResult>
    where
        T: Serialize + Unpin + Send + Sync;

    async fn insert_many<T>(
        &self,
        doc: impl IntoIterator<Item = impl Borrow<T> + Send> + Send + Sync,
    ) -> MgResult<InsertManyResult>
    where
        T: Serialize + Unpin + Send + Sync;

    async fn replace_one<T>(
        &self,
        query: Document,
        replace: impl Borrow<T> + Send,
    ) -> MgResult<UpdateResult>
    where
        T: Serialize + Send + Sync;
}

#[async_trait]
impl MongoRawEc for MongoExecutor {
    async fn delete_one(&self, query: Document) -> MgResult<DeleteResult> {
        Ok(self.schema::<Document>()?.delete_one(query, None).await?)
    }

    async fn delete_many(&self, query: Document) -> MgResult<DeleteResult> {
        Ok(self.schema::<Document>()?.delete_many(query, None).await?)
    }

    async fn distinct(
        &self,
        field_name: impl AsRef<str> + Send,
        filter: impl Into<Option<Document>> + Send,
    ) -> MgResult<Vec<Bson>> {
        Ok(self
            .schema::<Document>()?
            .distinct(field_name, filter, None)
            .await?)
    }

    async fn update_one(
        &self,
        query: Document,
        update: impl Into<UpdateModifications> + Send,
    ) -> MgResult<UpdateResult> {
        Ok(self
            .schema::<Document>()?
            .update_one(query, update, None)
            .await?)
    }

    async fn update_many(
        &self,
        query: Document,
        update: impl Into<UpdateModifications> + Send,
    ) -> MgResult<UpdateResult> {
        Ok(self
            .schema::<Document>()?
            .update_many(query, update, None)
            .await?)
    }

    async fn find_one<T>(&self, query: Document) -> MgResult<T>
    where
        T: DeserializeOwned + Unpin + Send + Sync,
    {
        self.schema::<T>()?
            .find_one(query, None)
            .await?
            .ok_or(MgError::ResultNotFound)
    }

    async fn find_many<T>(&self, query: Document) -> MgResult<Vec<T>>
    where
        T: DeserializeOwned + Unpin + Send + Sync,
    {
        Ok(self
            .schema::<T>()?
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
        self.schema::<T>()?
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
        self.schema::<T>()?
            .find_one_and_update(query, update, None)
            .await?
            .ok_or(MgError::ResultNotFound)
    }

    async fn find_one_and_replace<T>(
        &self,
        query: Document,
        replace: impl Borrow<T> + Send,
    ) -> MgResult<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        self.schema::<T>()?
            .find_one_and_replace(query, replace, None)
            .await?
            .ok_or(MgError::ResultNotFound)
    }

    async fn insert_one<T>(&self, doc: impl Borrow<T> + Send + Sync) -> MgResult<InsertOneResult>
    where
        T: Serialize + Unpin + Send + Sync,
    {
        Ok(self.schema::<T>()?.insert_one(doc, None).await?)
    }

    async fn insert_many<T>(
        &self,
        doc: impl IntoIterator<Item = impl Borrow<T> + Send> + Send + Sync,
    ) -> MgResult<InsertManyResult>
    where
        T: Serialize + Unpin + Send + Sync,
    {
        Ok(self.schema::<T>()?.insert_many(doc, None).await?)
    }

    async fn replace_one<T>(
        &self,
        query: Document,
        replace: impl Borrow<T> + Send,
    ) -> MgResult<UpdateResult>
    where
        T: Serialize + Send + Sync,
    {
        Ok(self
            .schema::<T>()?
            .replace_one(query, replace, None)
            .await?)
    }
}

#[derive(Clone, Debug)]
pub enum SavingCategory {
    Column,
    Row,
    Dataset,
}

impl From<SavingCategory> for JsonType {
    fn from(v: SavingCategory) -> Self {
        match v {
            SavingCategory::Column => JsonType::Column,
            SavingCategory::Row => JsonType::Row,
            SavingCategory::Dataset => JsonType::Dataset,
        }
    }
}

#[async_trait]
pub trait MongoEc: MongoRawEc {
    async fn delete_fx<I>(&self, id: I) -> MgResult<()>
    where
        I: TryInto<Oid, Error = MgError> + Send,
    {
        self.find_one_and_delete(doc! {"_id": id.try_into()?.id()})
            .await?;
        Ok(())
    }

    async fn delete_fxs<I, E>(&self, ids: I) -> MgResult<()>
    where
        I: IntoIterator<Item = E> + Send,
        E: TryInto<Oid, Error = MgError>,
    {
        let ids = ids
            .into_iter()
            .map(|e| -> MgResult<ObjectId> { e.try_into().map(|o| *o.id()) })
            .collect::<MgResult<Vec<_>>>()?;
        self.delete_many(doc! {"_id": { "$in": ids }}).await?;

        Ok(())
    }

    // TODO:
    // update operation should based on `Fabrix` index; if index is `None`, call replace method
    async fn update_fx<I>(
        &self,
        id: I,
        data: &Fabrix,
        saving_category: SavingCategory,
    ) -> MgResult<()>
    where
        I: TryInto<Oid, Error = MgError> + Send,
    {
        let d = match saving_category {
            SavingCategory::Column => bson::to_document(&FabrixColumnWised::from(data.clone()))?,
            SavingCategory::Row => todo!(),
            SavingCategory::Dataset => todo!(),
        };
        self.find_one_and_update(
            doc! {"_id": id.try_into()?.id()},
            UpdateModifications::Document(d),
        )
        .await?;

        Ok(())
    }

    async fn find_fx<I>(&self, id: I, saving_category: SavingCategory) -> MgResult<Fabrix>
    where
        I: TryInto<Oid, Error = MgError> + Send,
    {
        match saving_category {
            SavingCategory::Column => Ok(self
                .find_one::<FabrixColumnWised>(doc! {"_id": id.try_into()?.id()})
                .await?
                .into()),
            SavingCategory::Row => todo!(),
            SavingCategory::Dataset => todo!(),
        }
    }

    async fn find_fxs<I, E>(&self, ids: I, saving_category: SavingCategory) -> MgResult<Vec<Fabrix>>
    where
        I: IntoIterator<Item = E> + Send,
        E: TryInto<Oid, Error = MgError>,
    {
        let ids = ids
            .into_iter()
            .map(|e| -> MgResult<ObjectId> { e.try_into().map(|o| *o.id()) })
            .collect::<MgResult<Vec<_>>>()?;
        match saving_category {
            SavingCategory::Column => Ok(self
                .find_many::<FabrixColumnWised>(doc! {"_id": { "$in": ids }})
                .await?
                .into_iter()
                .map(Fabrix::from)
                .collect()),
            SavingCategory::Row => todo!(),
            SavingCategory::Dataset => todo!(),
        }
    }

    async fn replace_fx<I>(
        &self,
        id: I,
        data: &Fabrix,
        saving_category: SavingCategory,
    ) -> MgResult<()>
    where
        I: TryInto<Oid, Error = MgError> + Send,
    {
        let d = match saving_category {
            SavingCategory::Column => bson::to_document(&FabrixColumnWised::from(data.clone()))?,
            SavingCategory::Row => todo!(),
            SavingCategory::Dataset => todo!(),
        };
        self.find_one_and_replace(doc! {"_id": id.try_into()?.id()}, d)
            .await?;
        Ok(())
    }

    async fn insert_fx(&self, fx: &Fabrix, saving_category: SavingCategory) -> MgResult<Oid> {
        let ins = match saving_category {
            SavingCategory::Column => {
                self.insert_one::<FabrixColumnWised>(&FabrixColumnWised::from(fx.clone()))
                    .await
            }
            SavingCategory::Row => todo!(),
            SavingCategory::Dataset => todo!(),
        };

        ins.map(|r| Oid::new(r.inserted_id.as_object_id().unwrap()))
    }

    async fn insert_fxs(
        &self,
        fxs: &[Fabrix],
        saving_category: SavingCategory,
    ) -> MgResult<HashMap<usize, Oid>> {
        let ins = match saving_category {
            SavingCategory::Column => {
                let d = fxs
                    .iter()
                    .map(|i| FabrixColumnWised::from(i.clone()))
                    .collect::<Vec<_>>();
                self.insert_many::<FabrixColumnWised>(&d).await
            }
            SavingCategory::Row => todo!(),
            SavingCategory::Dataset => todo!(),
        };

        ins.map(|r| {
            r.inserted_ids
                .into_iter()
                .map(|(k, v)| (k, Oid::new(v.as_object_id().unwrap())))
                .collect::<HashMap<_, _>>()
        })
    }
}

#[async_trait]
impl MongoEc for MongoExecutor {}

#[cfg(test)]
mod dy_tests {
    use super::*;

    use bson::doc;
    use fabrix_core::{date, datetime, fx, time, Fabrix};

    const CONN: &str = "mongodb://root:secret@localhost:27017";
    const DB: &str = "dev";
    const CL: &str = "dev";

    #[tokio::test]
    async fn insert_one_and_find_one_by_raw_ec_success() {
        let ec = MongoExecutor::new_and_connect(CONN, DB, CL)
            .await
            .expect("connection failed");

        let df = fx![
            "ord";
            "names" => ["Jacob", "Sam", "Jason"],
            "ord" => [1,2,3],
            "val" => [Some(10), None, Some(8)]
        ]
        .unwrap();
        let dfc = FabrixColumnWised::from(df);

        let foo = ec.insert_one::<FabrixColumnWised>(&dfc).await;
        assert!(foo.is_ok());

        let id = foo.unwrap().inserted_id;
        println!("{:?}", id);

        let bar = ec.find_one::<FabrixColumnWised>(doc! {"_id": id}).await;
        assert!(bar.is_ok());
        println!("{:?}", Fabrix::from(bar.unwrap()));
    }

    #[tokio::test]
    async fn insert_one_and_find_by_id_success() {
        let ec = MongoExecutor::new_and_connect(CONN, DB, CL)
            .await
            .expect("connection failed");

        let df = fx![
            "idx";
            "idx" => [1,2,3],
            "names" => ["Jacob", "Sam", "Jason"],
            "val" => [Some(10), None, Some(8)],
            "date" => [date!(2020,1,1), date!(2020,1,2), date!(2020,1,3)],
            "time" => [time!(12,0,0), time!(12,0,1), time!(12,0,2)],
            "datetime" => [datetime!(2020,1,1,12,0,0), datetime!(2020,1,1,12,0,1), datetime!(2020,1,1,12,0,2)],
        ]
        .unwrap();

        let foo = ec.insert_fx(&df, SavingCategory::Column).await;
        assert!(foo.is_ok());

        let oid = *foo.unwrap().id();
        let bar = ec.find_fx(oid, SavingCategory::Column).await;
        assert!(bar.is_ok());

        assert_eq!(df, bar.unwrap());
    }
}
