use std::sync::Arc;

use futures_util::Stream;
use nsql_storage_engine::StorageEngine;

use crate::schema::Schema;
use crate::tuple::{Tuple, TupleIndex};

pub struct TableStorage<S> {
    storage: S,
    info: TableStorageInfo,
}

impl<S: StorageEngine> TableStorage<S> {
    #[inline]
    pub async fn initialize(storage: S, info: TableStorageInfo) -> Result<Self, S::Error> {
        Ok(Self { storage, info })
    }

    #[inline]
    pub async fn append(&self, tx: &S::Transaction<'_>, tuple: &Tuple) -> Result<(), S::Error> {
        todo!();
        Ok(())
    }

    #[inline]
    pub async fn update(
        &self,
        tx: &S::Transaction<'_>,
        id: &Tuple,
        tuple: &Tuple,
    ) -> Result<(), S::Error> {
        todo!();
        Ok(())
    }

    #[inline]
    pub async fn scan(
        &self,
        tx: S::Transaction<'_>,
        projection: Option<Box<[TupleIndex]>>,
    ) -> impl Stream<Item = Result<Vec<Tuple>, S::Error>> + Send {
        futures_util::stream::empty()
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
