use std::error::Error;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use dashmap::DashMap;
use nsql_core::Name;
use nsql_storage_engine::StorageEngine;

use crate::entry::Oid;
use crate::private::CatalogEntity;

pub enum Conflict<S, T> {
    AlreadyExists(PhantomData<S>, T),
    WriteWrite(WriteOperation, T),
}

#[derive(Debug)]
pub enum WriteOperation {
    Create,
    Update,
    Delete,
}

impl fmt::Display for WriteOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WriteOperation::Create => write!(f, "create"),
            WriteOperation::Update => write!(f, "update"),
            WriteOperation::Delete => write!(f, "delete"),
        }
    }
}

impl<S: StorageEngine, T: CatalogEntity<S>> Error for Conflict<S, T> {}

impl<S: StorageEngine, T: CatalogEntity<S>> fmt::Debug for Conflict<S, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl<S: StorageEngine, T: CatalogEntity<S>> fmt::Display for Conflict<S, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Conflict::AlreadyExists(_, entity) => {
                write!(f, "{} `{}` already exists", T::desc(), entity.name())
            }
            Conflict::WriteWrite(op, entity) => {
                write!(f, "write-write conflict on {op} of {} `{}`", T::desc(), entity.name())
            }
        }
    }
}

#[derive(Debug)]
pub struct CatalogSet<S, T> {
    entries: DashMap<Oid<T>, VersionedEntry<S, T>>,
    name_mapping: DashMap<Name, Oid<T>>,
    _marker: std::marker::PhantomData<S>,
}

impl<S, T> CatalogSet<S, T> {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: DashMap::with_capacity(capacity),
            name_mapping: DashMap::with_capacity(capacity),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<S, T> Default for CatalogSet<S, T> {
    fn default() -> Self {
        Self {
            entries: Default::default(),
            name_mapping: Default::default(),
            _marker: std::marker::PhantomData,
        }
    }
}

// FIXME hack implementation that will get by for now
// This should eventually maybe share code with the heap tuple versioning
#[derive(Debug)]
struct VersionedEntry<S, T> {
    versions: Vec<Arc<CatalogEntry<S, T>>>,
}

impl<S: StorageEngine, T> VersionedEntry<S, T> {
    pub(crate) fn new(entry: CatalogEntry<S, T>) -> Self {
        Self { versions: vec![Arc::new(entry)] }
    }
}

impl<S: StorageEngine, T> VersionedEntry<S, T> {
    /// Get the latest visible version for the given transaction.
    pub(crate) fn version_for_tx(
        &self,
        tx: &S::Transaction<'_>,
    ) -> Option<Arc<CatalogEntry<S, T>>> {
        todo!()
    }

    pub(crate) fn latest(&self) -> Arc<CatalogEntry<S, T>> {
        Arc::clone(self.versions.last().expect("must have at least one version"))
    }

    pub(crate) fn delete(&mut self, tx: &S::Transaction<'_>) {
        let version = self
            .version_for_tx(tx)
            .expect("caller must ensure there is a visible version for the given transaction");
        version.delete(tx);
    }

    pub(crate) fn push_version(&mut self, version: CatalogEntry<S, T>) {
        todo!()
    }
}

impl<S: StorageEngine, T: CatalogEntity<S>> CatalogSet<S, T> {
    #[inline]
    pub fn create(
        &self,
        tx: &S::Transaction<'_>,
        info: T::CreateInfo,
    ) -> Result<Oid<T>, Conflict<S, T>> {
        self.insert(tx, T::create(tx, info))
    }

    pub(crate) fn entries(&self, tx: &S::Transaction<'_>) -> Vec<(Oid<T>, Arc<T>)> {
        self.entries
            .iter()
            .enumerate()
            .flat_map(|(idx, entry)| {
                entry.version_for_tx(tx).map(|entry| (Oid::new(idx as u64), entry.value()))
            })
            .collect()
    }

    pub(crate) fn get(&self, tx: &S::Transaction<'_>, oid: Oid<T>) -> Option<Arc<T>> {
        self.entries.get(&oid).unwrap().version_for_tx(tx).map(|entry| entry.value())
    }

    pub(crate) fn get_by_name(
        &self,
        tx: &S::Transaction<'_>,
        name: &str,
    ) -> Option<(Oid<T>, Arc<T>)> {
        self.find(name).and_then(|oid| self.get(tx, oid).map(|item| (oid, item)))
    }

    pub(crate) fn find(&self, name: impl AsRef<str>) -> Option<Oid<T>> {
        Some(*self.name_mapping.get(name.as_ref())?.value())
    }

    pub(crate) fn delete(
        &self,
        tx: &S::Transaction<'_>,
        oid: Oid<T>,
    ) -> Result<(), Conflict<S, T>> {
        self.entries.get_mut(&oid).expect("passed invalid `oid` somehow").delete(tx);
        Ok(())
    }

    pub(crate) fn insert(
        &self,
        tx: &S::Transaction<'_>,
        value: T,
    ) -> Result<Oid<T>, Conflict<S, T>> {
        // NOTE: this function takes &self and not &mut self, so we need to be mindful of correctness under concurrent usage.
        todo!()
    }

    fn latest(&self, oid: Oid<T>) -> Arc<CatalogEntry<S, T>> {
        self.entries.get(&oid).unwrap().latest()
    }

    pub fn len(&self) -> usize {
        // don't read from `name_mapping` here as it will deadlock due to it's usage in `insert`
        self.entries.len()
    }

    fn next_oid(&self) -> Oid<T> {
        Oid::new(self.len() as u64)
    }
}

#[derive(Debug)]
struct CatalogEntry<S, T> {
    value: Arc<T>,
    _marker: PhantomData<S>,
}

impl<S: StorageEngine, T> CatalogEntry<S, T> {
    #[inline]
    pub fn value(&self) -> Arc<T> {
        Arc::clone(&self.value)
    }

    pub(crate) fn new(tx: &S::Transaction<'_>, value: T) -> Self {
        Self { value: Arc::new(value), _marker: PhantomData }
    }

    fn delete(&self, tx: &S::Transaction<'_>) {
        todo!()
    }
}
