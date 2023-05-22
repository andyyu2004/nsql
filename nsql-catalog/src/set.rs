use std::error::Error;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use dashmap::DashMap;
use nsql_core::Name;
use nsql_storage_engine::{StorageEngine, Transaction};

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
    entries: DashMap<Oid<T>, Arc<CatalogEntry<S, T>>>,
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

impl<S: StorageEngine, T: CatalogEntity<S>> CatalogSet<S, T> {
    #[inline]
    pub fn create(
        &self,
        tx: &mut S::WriteTransaction<'_>,
        info: T::CreateInfo,
    ) -> Result<Oid<T>, Conflict<S, T>> {
        let entity = T::create(tx, info);
        self.insert(tx, entity)
    }

    pub(crate) fn entries(&self, _tx: &impl Transaction<'_, S>) -> Vec<(Oid<T>, Arc<T>)> {
        self.entries
            .iter()
            .enumerate()
            .map(|(idx, entry)| (Oid::new(idx as u64), entry.value().value()))
            .collect()
    }

    pub(crate) fn get(&self, _tx: &impl Transaction<'_, S>, oid: Oid<T>) -> Option<Arc<T>> {
        self.entries.get(&oid).map(|entry| entry.value().value())
    }

    pub(crate) fn get_by_name(
        &self,
        tx: &impl Transaction<'_, S>,
        name: &str,
    ) -> Option<(Oid<T>, Arc<T>)> {
        self.find(name).and_then(|oid| self.get(tx, oid).map(|item| (oid, item)))
    }

    pub(crate) fn find(&self, name: impl AsRef<str>) -> Option<Oid<T>> {
        Some(*self.name_mapping.get(name.as_ref())?.value())
    }

    pub(crate) fn delete(
        &self,
        _tx: &mut S::WriteTransaction<'_>,
        oid: Oid<T>,
    ) -> Result<(), Conflict<S, T>> {
        self.entries.remove(&oid);
        Ok(())
    }

    pub(crate) fn insert(
        &self,
        tx: &mut S::WriteTransaction<'_>,
        value: T,
    ) -> Result<Oid<T>, Conflict<S, T>> {
        let oid = self.next_oid();
        self.name_mapping.insert(value.name(), oid);
        self.entries.insert(oid, Arc::new(CatalogEntry::new(tx, value)));
        Ok(oid)
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

    pub(crate) fn new(_tx: &mut S::WriteTransaction<'_>, value: T) -> Self {
        Self { value: Arc::new(value), _marker: PhantomData }
    }
}
