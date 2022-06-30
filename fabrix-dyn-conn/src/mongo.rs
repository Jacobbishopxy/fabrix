//! DynConnForMongo
//!
//! Mongo

use std::{collections::HashMap, hash::Hash};

use async_trait::async_trait;
use fabrix_core::Fabrix;
use fabrix_mg::{MgError, MongoBaseEc, MongoEc, MongoExecutor, Oid};

use crate::{gmv, gv, DynConn, DynConnError, DynConnInfo, DynConnResult};

#[async_trait]
pub trait DynConnForMongo<K>
where
    K: Eq + Hash + Send + Sync,
{
    // ================================================================================================
    // MongoInfo
    // ================================================================================================

    fn is_connected(&self, key: &K) -> DynConnResult<bool>;

    async fn connect(&self, key: &K) -> DynConnResult<()>;

    fn get_database(&self, key: &K) -> DynConnResult<String>;

    fn set_database(&self, key: &K, database: &str) -> DynConnResult<()>;

    fn get_collection(&self, key: &K) -> DynConnResult<String>;

    fn set_collection(&self, key: &K, collection: &str) -> DynConnResult<()>;

    // ================================================================================================
    // MongoExecutor
    // ================================================================================================

    async fn delete_fx<I>(&self, key: &K, id: I) -> DynConnResult<()>
    where
        I: TryInto<Oid, Error = MgError> + Send;

    async fn delete_fxs<I, E>(&self, key: &K, ids: I) -> DynConnResult<()>
    where
        I: IntoIterator<Item = E> + Send,
        E: TryInto<Oid, Error = MgError>;

    async fn find_fx<I>(&self, key: &K, id: I) -> DynConnResult<Fabrix>
    where
        I: TryInto<Oid, Error = MgError> + Send;

    async fn find_fxs<I, E>(&self, key: &K, ids: I) -> DynConnResult<Vec<Fabrix>>
    where
        I: IntoIterator<Item = E> + Send,
        E: TryInto<Oid, Error = MgError>;

    async fn replace_fx<I>(&self, key: &K, id: I, data: &Fabrix) -> DynConnResult<()>
    where
        I: TryInto<Oid, Error = MgError> + Send;

    async fn insert_fx(&self, key: &K, fx: &Fabrix) -> DynConnResult<Oid>;

    async fn insert_fxs(&self, key: &K, fxs: &[Fabrix]) -> DynConnResult<HashMap<usize, Oid>>;
}

#[async_trait]
impl<K> DynConnForMongo<K> for DynConn<K, MongoExecutor>
where
    K: Eq + Hash + Send + Sync,
{
    fn is_connected(&self, key: &K) -> DynConnResult<bool> {
        Ok(gv!(self, key).is_connected())
    }

    async fn connect(&self, key: &K) -> DynConnResult<()> {
        Ok(gmv!(self, key).connect().await?)
    }

    fn get_database(&self, key: &K) -> DynConnResult<String> {
        let result = gv!(self, key)
            .database()
            .ok_or(DynConnError::Mg(MgError::DatabaseOrCollectionNotSet))?
            .to_string();

        Ok(result)
    }

    fn set_database(&self, key: &K, database: &str) -> DynConnResult<()> {
        gmv!(self, key).set_database(database)?;
        Ok(())
    }

    fn get_collection(&self, key: &K) -> DynConnResult<String> {
        let result = gv!(self, key)
            .collection()
            .ok_or(DynConnError::Mg(MgError::DatabaseOrCollectionNotSet))?
            .to_string();

        Ok(result)
    }

    fn set_collection(&self, key: &K, collection: &str) -> DynConnResult<()> {
        gmv!(self, key).set_collection(collection)?;
        Ok(())
    }

    async fn delete_fx<I>(&self, key: &K, id: I) -> DynConnResult<()>
    where
        I: TryInto<Oid, Error = MgError> + Send,
    {
        Ok(gv!(self, key).delete_fx(id).await?)
    }

    async fn delete_fxs<I, E>(&self, key: &K, ids: I) -> DynConnResult<()>
    where
        I: IntoIterator<Item = E> + Send,
        E: TryInto<Oid, Error = MgError>,
    {
        Ok(gv!(self, key).delete_fxs(ids).await?)
    }

    async fn find_fx<I>(&self, key: &K, id: I) -> DynConnResult<Fabrix>
    where
        I: TryInto<Oid, Error = MgError> + Send,
    {
        Ok(gv!(self, key).find_fx(id).await?)
    }

    async fn find_fxs<I, E>(&self, key: &K, ids: I) -> DynConnResult<Vec<Fabrix>>
    where
        I: IntoIterator<Item = E> + Send,
        E: TryInto<Oid, Error = MgError>,
    {
        Ok(gv!(self, key).find_fxs(ids).await?)
    }

    async fn replace_fx<I>(&self, key: &K, id: I, data: &Fabrix) -> DynConnResult<()>
    where
        I: TryInto<Oid, Error = MgError> + Send,
    {
        gv!(self, key).replace_fx(id, data).await?;
        Ok(())
    }

    async fn insert_fx(&self, key: &K, fx: &Fabrix) -> DynConnResult<Oid> {
        Ok(gv!(self, key).insert_fx(fx).await?)
    }

    async fn insert_fxs(&self, key: &K, fxs: &[Fabrix]) -> DynConnResult<HashMap<usize, Oid>> {
        Ok(gv!(self, key).insert_fxs(fxs).await?)
    }
}

#[derive(Clone, Debug)]
pub struct DynConnMongoInfo {
    pub conn_str: String,
}

impl DynConnMongoInfo {
    pub fn new(value: &str) -> Self {
        Self {
            conn_str: value.to_string(),
        }
    }
}

impl From<&MongoExecutor> for DynConnMongoInfo {
    fn from(ec: &MongoExecutor) -> Self {
        DynConnMongoInfo::new(ec.conn_str())
    }
}

impl<K> DynConnInfo<K, MongoExecutor, DynConnMongoInfo> for DynConn<K, MongoExecutor>
where
    K: Clone + Eq + Hash + Send + Sync,
{
    fn list_all(&self) -> Vec<(K, DynConnMongoInfo)> {
        self.iter()
            .map(|i| {
                let key = i.key().clone();
                let info = DynConnMongoInfo::from(i.value());
                (key, info)
            })
            .collect()
    }

    fn get_info(&self, key: &K) -> Option<DynConnMongoInfo> {
        self.get(key).map(|i| DynConnMongoInfo::from(i.value()))
    }
}
