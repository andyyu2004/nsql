use std::sync::Arc;

use nsql_storage_engine::{StorageEngine, Transaction};

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
    pub fn append(
        &self,
        _tx: &mut S::WriteTransaction<'_>,
        _tuple: &Tuple,
    ) -> Result<(), S::Error> {
        todo!();
        Ok(())
    }

    #[inline]
    pub fn update(
        &self,
        _tx: &S::Transaction<'_>,
        _id: &Tuple,
        _tuple: &Tuple,
    ) -> Result<(), S::Error> {
        todo!();
        Ok(())
    }

    #[inline]
    pub fn scan(
        &self,
        _tx: &impl Transaction<'_, S>,
        _projection: Option<Box<[TupleIndex]>>,
    ) -> impl Iterator<Item = Result<Vec<Tuple>, S::Error>> + Send + 'static {
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
