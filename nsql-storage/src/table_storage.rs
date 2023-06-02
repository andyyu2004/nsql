use std::sync::Arc;

use ::next_gen::prelude::*;
use fix_hidden_lifetime_bug::fix_hidden_lifetime_bug;
use next_gen::generator_fn::GeneratorFn;
use nsql_catalog::{Column, TableRef};
use nsql_storage_engine::{
    fallible_iterator, FallibleIterator, ReadTree, StorageEngine, Transaction, WriteTree,
};

use crate::tuple::{Tuple, TupleIndex};
use crate::value::Value;

pub struct TableStorage<'env: 'txn, 'txn, S: StorageEngine> {
    storage: S,
    tree: S::ReadTree<'env, 'txn>,
    info: TableStorageInfo,
}

#[fix_hidden_lifetime_bug]
impl<'env, 'txn, S: StorageEngine> TableStorage<'env, 'txn, S> {
    #[inline]
    pub fn initialize(
        storage: S,
        tx: &'txn S::WriteTransaction<'env>,
        info: TableStorageInfo,
    ) -> Result<Self, S::Error> {
        // create the tree
        storage.open_write_tree(tx, &info.storage_tree_name)?;
        Self::open(storage, tx, info)
    }

    pub fn open(
        storage: S,
        tx: &'txn impl Transaction<'env, S>,
        info: TableStorageInfo,
    ) -> Result<Self, S::Error> {
        let tree = storage.open_tree(tx, &info.storage_tree_name)?.unwrap();
        Ok(Self { storage, info, tree })
    }

    #[inline]
    pub fn append(&self, tx: &S::WriteTransaction<'_>, tuple: &Tuple) -> Result<(), S::Error> {
        assert_eq!(
            tuple.len(),
            self.info.columns.len(),
            "tuple length did not match the expected number of columns, expected {}, got {}",
            self.info.columns.len(),
            tuple.len()
        );

        let mut pk_tuple = vec![];
        let mut non_pk_tuple = vec![];

        for (value, col) in tuple.values().zip(&self.info.columns) {
            assert_eq!(
                value.ty(),
                col.logical_type(),
                "expected column type {:?}, got {:?}",
                col.logical_type(),
                value.ty()
            );

            if col.is_primary_key() {
                pk_tuple.push(value);
            } else {
                non_pk_tuple.push(value);
            }
        }

        let mut tree = self.storage.open_write_tree(tx, &self.info.storage_tree_name)?;
        let pk_bytes = nsql_rkyv::to_bytes(&pk_tuple);
        let non_pk_bytes = nsql_rkyv::to_bytes(&non_pk_tuple);
        tree.put(&pk_bytes, &non_pk_bytes)?;

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
    #[fix_hidden_lifetime_bug]
    pub fn scan(
        self: Arc<Self>,
        projection: Option<Box<[TupleIndex]>>,
    ) -> Result<impl FallibleIterator<Item = Tuple, Error = S::Error> + 'txn, S::Error> {
        let mut gen = Box::pin(GeneratorFn::empty());
        gen.as_mut().init(range_gen::<S>, (self, projection));
        Ok(fallible_iterator::convert(gen))
    }
}

#[generator(yield(Result<Tuple, S::Error>))]
fn range_gen<'env, 'txn, S: StorageEngine>(
    storage: Arc<TableStorage<'env, 'txn, S>>,
    projection: Option<Box<[TupleIndex]>>,
) {
    let mut range = match storage.tree.range(..) {
        Ok(range) => range,
        Err(err) => {
            yield_!(Err(err));
            return;
        }
    };
    loop {
        match range.next() {
            Err(err) => {
                yield_!(Err(err));
                return;
            }
            Ok(None) => return,
            Ok(Some((k, v))) => {
                let k = unsafe { rkyv::archived_root::<Vec<Value>>(&k) };
                let v = unsafe { rkyv::archived_root::<Vec<Value>>(&v) };
                //         let mut tuple = match &projection {
                //             Some(projection) => tuple.project(tid, projection),
                //             None => nsql_rkyv::deserialize(tuple),
                //         };
                todo!();
                yield_!(Ok(Tuple::new(Box::new([]))))
            }
        }
    }
}
pub struct TableStorageInfo {
    columns: Vec<Arc<Column>>,
    storage_tree_name: String,
}

impl TableStorageInfo {
    #[inline]
    pub fn new<S>(table_ref: TableRef<S>, columns: Vec<Arc<Column>>) -> Self {
        assert!(
            columns.iter().any(|c| c.is_primary_key()),
            "expected at least one primary key column (this should be checked in the binder)"
        );

        Self { columns, storage_tree_name: format!("{table_ref}") }
    }
}

// #[cfg(test)]
// mod tests;
