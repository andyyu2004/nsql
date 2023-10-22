use std::fmt;
use std::sync::Arc;

use ::next_gen::prelude::*;
use fix_hidden_lifetime_bug::fix_hidden_lifetime_bug;
use next_gen::generator_fn::GeneratorFn;
use nsql_core::{LogicalType, Name, Oid};
use nsql_storage_engine::{
    fallible_iterator, ExecutionMode, FallibleIterator, KeyExists, ReadTree,
    ReadWriteExecutionMode, StorageEngine, WriteTree,
};
use rkyv::AlignedVec;

use crate::eval::FunctionCatalog;
use crate::index::{IndexStorage, IndexStorageInfo};
use crate::tuple::{IntoTuple, Tuple, TupleIndex};
use crate::value::Value;

#[allow(explicit_outlives_requirements)]
pub struct TableStorage<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> {
    tree: M::Tree<'txn>,
    info: TableStorageInfo,
    indexes: Box<[IndexStorage<'env, 'txn, S, M>]>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> fmt::Debug
    for TableStorage<'env, 'txn, S, M>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableStorage").field("info", &self.info).finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct PrimaryKeyConflict {
    pub key: Tuple,
}

impl<'env, 'txn, S: StorageEngine> TableStorage<'env, 'txn, S, ReadWriteExecutionMode> {
    #[inline]
    pub fn create(
        storage: &S,
        tx: &'txn S::WriteTransaction<'env>,
        info: TableStorageInfo,
        indexes: Vec<IndexStorageInfo>,
    ) -> Result<Self, S::Error> {
        // create the tree
        storage.open_write_tree(tx, &info.name)?;
        Self::open(storage, tx, info, indexes)
    }

    #[inline]
    pub fn update(&mut self, tuple: &Tuple) -> Result<(), S::Error> {
        let (k, v) = self.split_tuple(tuple);
        debug_assert!(self.tree.delete(&k)?, "updating a tuple that didn't exist");
        self.tree.update(&k, &v)?;

        Ok(())
    }

    pub fn delete(&mut self, key: impl IntoTuple) -> Result<bool, S::Error> {
        let k = key.into_tuple();
        let k = nsql_rkyv::to_bytes(&k);
        self.tree.delete(&k)
    }

    #[inline]
    pub fn insert(
        &mut self,
        catalog: &dyn FunctionCatalog<'env, S, ReadWriteExecutionMode>,
        tx: &S::WriteTransaction<'env>,
        tuple: &Tuple,
    ) -> Result<Result<(), PrimaryKeyConflict>, anyhow::Error> {
        for index in self.indexes.iter_mut() {
            index.insert(catalog, tx, tuple)?;
        }

        let (k, v) = self.split_tuple(tuple);

        if let Err(KeyExists) = self.tree.insert(&k, &v)? {
            let key = unsafe { nsql_rkyv::deserialize_raw::<Vec<Value>>(&k) }.into();
            return Ok(Err(PrimaryKeyConflict { key }));
        }

        Ok(Ok(()))
    }

    /// Split tuple into primary key and non-primary key components
    fn split_tuple(&self, tuple: &Tuple) -> (AlignedVec, AlignedVec) {
        assert_eq!(
            tuple.width(),
            self.info.columns.len(),
            "tuple length did not match the expected number of columns, expected {}, got {} (info={:?}, tuple={})",
            self.info.columns.len(),
            tuple.width(),
            self.info,
            tuple,
        );

        let mut pk_tuple = vec![];
        let mut non_pk_tuple = vec![];

        assert_eq!(tuple.width(), self.info.columns.len());

        for (value, col) in tuple.values().zip(&self.info.columns) {
            if col.is_primary_key {
                pk_tuple.push(value);
            } else {
                non_pk_tuple.push(value);
            }
        }

        let pk_bytes = nsql_rkyv::to_bytes(&pk_tuple);
        let non_pk_bytes = nsql_rkyv::to_bytes(&non_pk_tuple);

        (pk_bytes, non_pk_bytes)
    }
}

#[fix_hidden_lifetime_bug]
impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> TableStorage<'env, 'txn, S, M> {
    #[track_caller]
    pub fn open(
        storage: &S,
        tx: M::TransactionRef<'txn>,
        info: TableStorageInfo,
        indexes: Vec<IndexStorageInfo>,
    ) -> Result<Self, S::Error> {
        let tree = M::open_tree(storage, tx, &info.name)?;
        let indexes = indexes
            .into_iter()
            .map(|info| IndexStorage::open(storage, tx, info))
            .collect::<Result<_, _>>()?;
        Ok(Self { info, tree, indexes })
    }

    #[inline]
    pub fn get(&self, key: impl IntoTuple) -> Result<Option<Tuple>, S::Error> {
        let k = key.into_tuple();
        let k = nsql_rkyv::to_bytes(&k);
        let v = self.tree.get(&k)?;
        Ok(v.map(|v| unsplit_tuple(&self.info, None, &k, &v)))
    }

    #[inline]
    #[fix_hidden_lifetime_bug]
    pub fn scan(
        &self,
        projection: Option<Box<[TupleIndex]>>,
    ) -> Result<impl FallibleIterator<Item = Tuple, Error = S::Error> + '_, S::Error> {
        let mut gen = Box::pin(GeneratorFn::empty());
        gen.as_mut().init(range_gen::<S, M>, (self, projection));
        Ok(fallible_iterator::convert(gen))
    }

    /// The `arc` variant is useful for when you want to return the iterator from a function and
    /// the storage is a local variable for example.
    #[inline]
    #[fix_hidden_lifetime_bug]
    pub fn scan_arc(
        self: Arc<Self>,
        projection: Option<Box<[TupleIndex]>>,
    ) -> Result<impl FallibleIterator<Item = Tuple, Error = S::Error> + 'txn, S::Error> {
        let mut gen = Box::pin(GeneratorFn::empty());
        gen.as_mut().init(range_gen_arc::<S, M>, (self, projection));
        Ok(fallible_iterator::convert(gen))
    }
}

fn unsplit_tuple(
    info: &TableStorageInfo,
    projection: Option<&[TupleIndex]>,
    k: &[u8],
    v: &[u8],
) -> Tuple {
    // FIXME this is a very naive and inefficient algorithm
    let ks = unsafe { rkyv::archived_root::<Vec<Value>>(k) };
    let vs = unsafe { rkyv::archived_root::<Vec<Value>>(v) };
    let n = info.columns.len();
    debug_assert_eq!(
        n,
        ks.len() + vs.len(),
        "expected {} columns, got {} columns (column_def: {:#?})",
        n,
        ks.len() + vs.len(),
        info.columns
    );
    let (mut i, mut j) = (0, 0);
    let mut tuple = Vec::with_capacity(n);

    for col in &info.columns {
        if col.is_primary_key {
            tuple.push(&ks[i]);
            i += 1;
        } else {
            tuple.push(&vs[j]);
            j += 1;
        }
    }

    match &projection {
        Some(projection) => Tuple::project_archived(tuple.as_slice(), projection),
        None => tuple.into_iter().map(nsql_rkyv::deserialize).collect(),
    }
}

// FIXME dedup the code from below
#[generator(yield(Result<Tuple, S::Error>))]
fn range_gen<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
    storage: &TableStorage<'env, 'txn, S, M>,
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
                let tuple = unsplit_tuple(&storage.info, projection.as_deref(), &k, &v);
                yield_!(Ok(tuple))
            }
        }
    }
}

// FIXME dedup the code from above
#[generator(yield(Result<Tuple, S::Error>))]
fn range_gen_arc<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
    storage: Arc<TableStorage<'env, 'txn, S, M>>,
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
                let tuple = unsplit_tuple(&storage.info, projection.as_deref(), &k, &v);
                yield_!(Ok(tuple))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableStorageInfo {
    name: Name,
    columns: Vec<ColumnStorageInfo>,
}

impl TableStorageInfo {
    pub fn derive_name(oid: Oid<()>) -> Name {
        format!("{oid}").into()
    }

    #[inline]
    pub fn new(oid: Oid<()>, columns: Vec<ColumnStorageInfo>) -> Self {
        assert!(
            !columns.is_empty(),
            "expected at least one column (this should be checked in the binder)"
        );

        assert!(
            columns.iter().any(|c| c.is_primary_key),
            "expected at least one primary key column (this should be checked in the binder)"
        );

        Self { columns, name: Self::derive_name(oid) }
    }

    #[inline]
    pub fn name(&self) -> &Name {
        &self.name
    }
}

#[derive(Debug, Clone)]
pub struct ColumnStorageInfo {
    pub name: Name,
    pub logical_type: LogicalType,
    pub is_primary_key: bool,
}

impl ColumnStorageInfo {
    pub fn new(name: impl Into<Name>, logical_type: LogicalType, is_primary_key: bool) -> Self {
        Self { name: name.into(), logical_type, is_primary_key }
    }
}
