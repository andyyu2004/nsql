use std::error::Error;
use std::fmt;
use std::sync::{Arc, RwLock};

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use nsql_core::Name;
use nsql_serde::{
    AsyncReadExt, AsyncWriteExt, StreamDeserialize, StreamDeserializeWith, StreamDeserializer,
    StreamSerialize, StreamSerializer,
};
use nsql_transaction::{Transaction, Version};

use crate::entry::Oid;
use crate::private::CatalogEntity;

pub enum Conflict<T> {
    AlreadyExists(T),
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

impl<T: CatalogEntity> Error for Conflict<T> {}

impl<T: CatalogEntity> fmt::Debug for Conflict<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl<T: CatalogEntity> fmt::Display for Conflict<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Conflict::AlreadyExists(entity) => {
                write!(f, "{} `{}` already exists", T::desc(), entity.name())
            }
            Conflict::WriteWrite(op, entity) => {
                write!(f, "write-write conflict on {op} of {} `{}`", T::desc(), entity.name())
            }
        }
    }
}

#[derive(Debug)]
pub struct CatalogSet<T> {
    entries: DashMap<Oid<T>, VersionedEntry<T>>,
    name_mapping: DashMap<Name, Oid<T>>,
}

impl<T> CatalogSet<T> {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: DashMap::with_capacity(capacity),
            name_mapping: DashMap::with_capacity(capacity),
        }
    }
}

impl<T: StreamSerialize> StreamSerialize for CatalogSet<T> {
    async fn serialize<S: StreamSerializer>(&self, ser: &mut S) -> nsql_serde::Result<()> {
        ser.write_u32(self.entries.len() as u32).await?;
        for entry in self.entries.iter() {
            let _ = entry;
            // let tx = todo!();
            // entry.version_for_tx(tx).value().serialize(ser).await?;
        }
        Ok(())
    }
}

impl<T> StreamDeserializeWith for CatalogSet<T>
where
    T: CatalogEntity,
    T::CreateInfo: StreamDeserialize,
{
    type Context<'a> = Transaction;

    async fn deserialize_with<D: StreamDeserializer>(
        tx: &Self::Context<'_>,
        de: &mut D,
    ) -> nsql_serde::Result<Self> {
        let len = de.read_u32().await? as usize;
        let set = Self::with_capacity(len);
        for _ in 0..len {
            let item = T::CreateInfo::deserialize(de).await?;
            set.create(tx, item)
                .expect("creation should not fail as it was serialized from a valid state");
        }
        Ok(set)
    }
}

impl<T> Default for CatalogSet<T> {
    fn default() -> Self {
        Self { entries: Default::default(), name_mapping: Default::default() }
    }
}

// FIXME hack implementation that will get by for now
// This should eventually maybe share code with the heap tuple versioning
#[derive(Debug)]
struct VersionedEntry<T> {
    versions: Vec<Arc<CatalogEntry<T>>>,
}

impl<T> VersionedEntry<T> {
    pub(crate) fn new(entry: CatalogEntry<T>) -> Self {
        Self { versions: vec![Arc::new(entry)] }
    }
}

impl<T> VersionedEntry<T> {
    /// Get the latest visible version for the given transaction.
    pub(crate) fn version_for_tx(&self, tx: &Transaction) -> Option<Arc<CatalogEntry<T>>> {
        self.versions.iter().rev().find(|version| tx.can_see(version.version())).map(Arc::clone)
    }

    pub(crate) fn latest(&self) -> Arc<CatalogEntry<T>> {
        Arc::clone(self.versions.last().expect("must have at least one version"))
    }

    pub(crate) fn delete(&mut self, tx: &Transaction) {
        let version = self
            .version_for_tx(tx)
            .expect("caller must ensure there is a visible version for the given transaction");
        version.delete(tx);
    }

    pub(crate) fn push_version(&mut self, version: CatalogEntry<T>) {
        assert!(self.latest().version().xmax().is_some(), "must delete the latest version first?");
        debug_assert!(
            self.versions.is_empty()
                || self.versions.last().unwrap().version().xmin() < version.version().xmin(),
            "versions must be ordered by xmin"
        );
        self.versions.push(Arc::new(version))
    }
}

impl<T: CatalogEntity> CatalogSet<T> {
    #[inline]
    pub fn create(&self, tx: &Transaction, info: T::CreateInfo) -> Result<Oid<T>, Conflict<T>> {
        self.insert(tx, T::new(tx, info))
    }

    pub(crate) fn entries(&self, tx: &Transaction) -> Vec<(Oid<T>, Arc<T>)> {
        self.entries
            .iter()
            .enumerate()
            .flat_map(|(idx, entry)| {
                entry.version_for_tx(tx).map(|entry| (Oid::new(idx as u64), entry.value()))
            })
            .collect()
    }

    pub(crate) fn get(&self, tx: &Transaction, oid: Oid<T>) -> Option<Arc<T>> {
        self.entries.get(&oid).unwrap().version_for_tx(tx).map(|entry| entry.value())
    }

    pub(crate) fn get_by_name(&self, tx: &Transaction, name: &str) -> Option<(Oid<T>, Arc<T>)> {
        self.find(name).and_then(|oid| self.get(tx, oid).map(|item| (oid, item)))
    }

    pub(crate) fn find(&self, name: impl AsRef<str>) -> Option<Oid<T>> {
        Some(*self.name_mapping.get(name.as_ref())?.value())
    }

    pub(crate) fn delete(&self, tx: &Transaction, oid: Oid<T>) -> Result<(), Conflict<T>> {
        self.entries.get_mut(&oid).expect("passed invalid `oid` somehow").delete(tx);
        Ok(())
    }

    pub(crate) fn insert(&self, tx: &Transaction, value: T) -> Result<Oid<T>, Conflict<T>> {
        // NOTE: this function takes &self and not &mut self, so we need to be mindful of correctness under concurrent usage.

        match self.name_mapping.entry(value.name()) {
            Entry::Occupied(mut entry) => {
                // If there is an existing entry, we need to check for conflicts with our insertion
                let latest = self.latest(*entry.get());
                if !tx.can_see(latest.version()) {
                    // If the existing entry is not visible to our transaction, then either
                    match latest.version().xmax() {
                        // 1. It was deleted before our transaction started (or we deleted it ourserlves),
                        //    in which case we can insert the new entry
                        Some(xmax) if xmax <= tx.xid() => {
                            let oid = self.next_oid();
                            entry.insert(oid);
                            debug_assert!(
                                self.entries.get(&oid).is_none(),
                                "next_oid() should return a unique oid"
                            );

                            let _entry = self.entries.entry(oid).or_insert_with(|| {
                                VersionedEntry::new(CatalogEntry::new(tx, value))
                            });

                            Ok(oid)
                        }
                        // 2. It was created after our transaction started, in which case we have a write-write conflict
                        _ => Err(Conflict::WriteWrite(WriteOperation::Create, value)),
                    }
                } else {
                    // Otherwise, the existing entry is visible to our transaction, so we return an `AlreadyExists` conflict
                    Err(Conflict::AlreadyExists(value))
                }
            }
            Entry::Vacant(entry) => {
                let oid = self.next_oid();
                entry.insert(oid);
                debug_assert!(
                    self.entries.get(&oid).is_none(),
                    "next_oid() should return a unique oid"
                );

                self.entries
                    .entry(oid)
                    .or_insert_with(|| VersionedEntry::new(CatalogEntry::new(tx, value)));

                Ok(oid)
            }
        }
    }

    fn latest(&self, oid: Oid<T>) -> Arc<CatalogEntry<T>> {
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
struct CatalogEntry<T> {
    version: RwLock<Version>,
    value: Arc<T>,
}

impl<T> CatalogEntry<T> {
    #[inline]
    pub fn value(&self) -> Arc<T> {
        Arc::clone(&self.value)
    }

    pub(crate) fn new(tx: &Transaction, value: T) -> Self {
        Self { version: RwLock::new(tx.version()), value: Arc::new(value) }
    }

    pub(crate) fn version(&self) -> Version {
        *self.version.read().unwrap()
    }

    fn delete(&self, tx: &Transaction) {
        let mut version = self.version.write().unwrap();
        *version = version.delete_with(tx);
    }
}
