//! DynConnForMongo
//!
//! Mongo

use std::{
    hash::Hash,
    ops::{Deref, DerefMut},
};

use async_trait::async_trait;
use fabrix_mg::{MongoEc, MongoExecutor};

use crate::{gmv, gv, DynConn, DynConnResult};

#[async_trait]
pub trait DynConnForMongo<K>
where
    K: Eq + Hash + Send + Sync,
{
    // ================================================================================================
    // MongoInfo
    // ================================================================================================

    fn get_database(&self, key: &K) -> DynConnResult<String>;

    fn set_database(&self, key: &K, database: &str) -> DynConnResult<()>;

    fn get_collection(&self, key: &K) -> DynConnResult<String>;

    fn set_collection(&self, key: &K, collection: &str) -> DynConnResult<()>;

    // ================================================================================================
    // MongoHelper
    // ================================================================================================

    // ================================================================================================
    // MongoExecutor
    // ================================================================================================
}

#[async_trait]
impl<K, V> DynConnForMongo<K> for DynConn<K, V>
where
    K: Eq + Hash + Send + Sync,
    V: Deref<Target = MongoExecutor>,
    V: DerefMut<Target = MongoExecutor>,
    V: Send + Sync,
{
    fn get_database(&self, key: &K) -> DynConnResult<String> {
        Ok(gv!(self, key).database().to_string())
    }

    fn set_database(&self, key: &K, database: &str) -> DynConnResult<()> {
        gmv!(self, key).set_database(database);
        Ok(())
    }

    fn get_collection(&self, key: &K) -> DynConnResult<String> {
        Ok(gv!(self, key).collection().to_string())
    }

    fn set_collection(&self, key: &K, collection: &str) -> DynConnResult<()> {
        gmv!(self, key).set_collection(collection);
        Ok(())
    }
}
