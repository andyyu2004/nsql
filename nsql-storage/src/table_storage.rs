use std::sync::Arc;

use nsql_storage_engine::StorageEngine;

use crate::schema::Schema;
use crate::tuple::{Tuple, TupleIndex};

pub struct TableStorage<S> {
    storage: S,
    info: TableStorageInfo,
}

impl<S: StorageEngine> TableStorage<S> {
    #[inline]
    pub fn initialize(storage: S, info: TableStorageInfo) -> Result<Self, S::Error> {
        Ok(Self { storage, info })
    }

    #[inline]
    pub fn append(&self, tx: &S::Transaction<'_>, tuple: &Tuple) -> Result<(), S::Error> {
        todo!();
        Ok(())
    }

    #[inline]
    pub fn update(
        &self,
        tx: &S::Transaction<'_>,
        id: &Tuple,
        tuple: &Tuple,
    ) -> Result<(), S::Error> {
        todo!();
        Ok(())
    }

    #[inline]
    pub fn scan(
        &self,
        tx: S::Transaction<'_>,
        projection: Option<Box<[TupleIndex]>>,
    ) -> impl Iterator<Item = Result<Vec<Tuple>, S::Error>> + Send {
        [].into_iter()
        // self.heap
        //     .scan(tx, move |tid, tuple| {
        //         let mut tuple = match &projection {
        //             Some(projection) => tuple.project(tid, projection),
        //             None => nsql_rkyv::deserialize(tuple),
        //         };
        //
        //         tuple
        //     })
        //     .await
    }
}

pub struct TableStorageInfo {
    schema: Arc<Schema>,
}

impl TableStorageInfo {
    #[inline]
    pub fn create(schema: Arc<Schema>) -> Self {
        Self { schema }
    }
}

// #[cfg(test)]
// mod tests;
