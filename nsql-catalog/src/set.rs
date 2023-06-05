use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use indexmap::IndexMap;
use nsql_core::Name;
use nsql_storage_engine::{StorageEngine, Transaction};
use parking_lot::RwLock;

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
    entries: RwLock<IndexMap<Oid<T>, Arc<CatalogEntry<S, T>>>>,
    name_mapping: RwLock<HashMap<Name, Oid<T>>>,
    _marker: std::marker::PhantomData<S>,
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

impl<S: StorageEngine, T: CatalogEntity<S>> CatalogSet<S, T> {
    #[inline]
    pub fn create(
        &self,
        tx: &S::WriteTransaction<'_>,
        info: T::CreateInfo,
    ) -> Result<Oid<T>, Conflict<S, T>> {
        self.insert(tx, info)
    }

    pub(crate) fn entries(&self, _tx: &impl Transaction<'_, S>) -> Vec<Arc<T>> {
        self.entries.read().values().map(|entry| entry.value()).collect()
    }

    pub(crate) fn get(&self, _tx: &impl Transaction<'_, S>, oid: Oid<T>) -> Option<Arc<T>> {
        self.entries.read().get(&oid).map(|entry| entry.value())
    }

    pub(crate) fn get_by_name(
        &self,
        tx: &impl Transaction<'_, S>,
        name: &str,
    ) -> Option<(Oid<T>, Arc<T>)> {
        self.find(name).and_then(|oid| self.get(tx, oid).map(|item| (oid, item)))
    }

    pub(crate) fn find(&self, name: impl AsRef<str>) -> Option<Oid<T>> {
        Some(*self.name_mapping.read().get(name.as_ref())?)
    }

    pub(crate) fn delete(
        &self,
        _tx: &S::WriteTransaction<'_>,
        oid: Oid<T>,
    ) -> Result<(), Conflict<S, T>> {
        self.entries.write().remove(&oid);
        Ok(())
    }

    pub(crate) fn insert(
        &self,
        tx: &S::WriteTransaction<'_>,
        info: T::CreateInfo,
    ) -> Result<Oid<T>, Conflict<S, T>> {
        let oid = self.next_oid();
        let value = T::create(tx, oid, info);
        if self.name_mapping.read().contains_key(&value.name()) {
            return Err(Conflict::AlreadyExists(PhantomData, value));
        }
        self.name_mapping.write().insert(value.name(), oid);
        self.entries.write().insert(oid, Arc::new(CatalogEntry::new(tx, value)));
        Ok(oid)
    }

    pub fn len(&self) -> usize {
        self.entries.read().len()
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

    pub(crate) fn new(_tx: &S::WriteTransaction<'_>, value: T) -> Self {
        Self { value: Arc::new(value), _marker: PhantomData }
    }
}
