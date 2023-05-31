use nsql_core::Name;
use nsql_storage_engine::{FallibleIterator, ReadTree, StorageEngine, Transaction, WriteTree};

use crate::schema::LogicalType;
use crate::tuple::{Tuple, TupleIndex};

pub struct TableStorage<'env: 'txn, 'txn, S: StorageEngine> {
    storage: S,
    tree: S::ReadTree<'env, 'txn>,
    info: TableStorageInfo,
}

impl<'env, 'txn, S: StorageEngine> TableStorage<'env, 'txn, S> {
    #[inline]
    pub fn initialize(
        storage: S,
        tx: &'txn mut S::WriteTransaction<'env>,
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
    pub fn append(&self, tx: &mut S::WriteTransaction<'_>, tuple: &Tuple) -> Result<(), S::Error> {
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
                col.ty,
                "expected column type {:?}, got {:?}",
                col.ty,
                value.ty()
            );

            if col.is_primary_key {
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
    pub fn scan(
        &self,
        projection: Option<Box<[TupleIndex]>>,
    ) -> Result<impl FallibleIterator<Item = Tuple, Error = S::Error> + '_, S::Error> {
        let iter = self.tree.range(..)?;
        // Ok(fallible_iterator::convert([].into_iter()))
        Ok(iter.map(move |(k, v)| {
            // let k = unsafe { rkyv::archived_root::<Vec<Value>>(&k) };
            // let v = unsafe { rkyv::archived_root::<Vec<Value>>(&v) };
            // dbg!(k, v);
            // todo!();
            Ok(Tuple::new(Box::new([])))
        }))
        //
        // Ok(tree.range(..)?.map(|(k, v)| {
        //     // let k = unsafe { rkyv::archived_root::<Vec<Value>>(&k) };
        //     // let v = unsafe { rkyv::archived_root::<Vec<Value>>(&v) };
        //     // dbg!(k, v);
        //     todo!();
        //     Ok(Tuple::new(Box::new([])))
        // }))
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
    columns: Vec<ColumnStorageInfo>,
    storage_tree_name: String,
}

#[derive(Clone)]
pub struct ColumnStorageInfo {
    ty: LogicalType,
    is_primary_key: bool,
}

impl ColumnStorageInfo {
    #[inline]
    pub fn new(ty: LogicalType, is_primary_key: bool) -> Self {
        Self { ty, is_primary_key }
    }
}

impl TableStorageInfo {
    #[inline]
    pub fn create(namespace: &Name, table: &Name, columns: Vec<ColumnStorageInfo>) -> Self {
        assert!(
            columns.iter().any(|c| c.is_primary_key),
            "expected at least one primary key column (this should be checked in the binder)"
        );

        Self { columns, storage_tree_name: format!("{}.{}", namespace, table) }
    }
}

// #[cfg(test)]
// mod tests;
