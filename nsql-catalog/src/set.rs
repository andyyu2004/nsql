use std::error::Error;
use std::fmt;
use std::sync::Arc;

use dashmap::DashMap;
use nsql_core::Name;
use nsql_serde::{
    AsyncReadExt, AsyncWriteExt, StreamDeserialize, StreamDeserializeWith, StreamDeserializer,
    StreamSerialize, StreamSerializer,
};
use nsql_transaction::{Transaction, Txid};

use crate::entry::Oid;
use crate::private::CatalogEntity;

pub struct AlreadyExists<T>(T);

impl<T: CatalogEntity> Error for AlreadyExists<T> {}

impl<T: CatalogEntity> fmt::Debug for AlreadyExists<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl<T: CatalogEntity> fmt::Display for AlreadyExists<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} `{}` already exists", T::desc(), self.0.name())
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
            entry.committed_version().value().serialize(ser).await?;
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

#[derive(Debug)]
struct VersionedEntry<T> {
    versions: Vec<CatalogEntry<T>>,
}

impl<T> Default for VersionedEntry<T> {
    fn default() -> Self {
        Self { versions: Default::default() }
    }
}

impl<T> VersionedEntry<T> {
    fn committed_version(&self) -> CatalogEntry<T> {
        todo!()
    }

    fn version_for_tx(&self, tx: &Transaction) -> Option<CatalogEntry<T>> {
        self.versions.iter().rev().find(|version| tx.can_see(version.txid())).cloned()
    }

    fn push_version(&mut self, version: CatalogEntry<T>) {
        self.versions.push(version)
    }
}

impl<T: CatalogEntity> CatalogSet<T> {
    #[inline]
    pub fn create(
        &self,
        tx: &Transaction,
        info: T::CreateInfo,
    ) -> Result<Oid<T>, AlreadyExists<T>> {
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
        self.entries.get(&oid).and_then(|entry| entry.version_for_tx(tx)).map(|entry| entry.value())
    }

    pub(crate) fn get_by_name(&self, tx: &Transaction, name: &str) -> Option<(Oid<T>, Arc<T>)> {
        self.find(name).and_then(|oid| self.get(tx, oid).map(|item| (oid, item)))
    }

    pub(crate) fn find(&self, name: impl AsRef<str>) -> Option<Oid<T>> {
        Some(*self.name_mapping.get(name.as_ref())?.value())
    }

    pub(crate) fn insert(&self, tx: &Transaction, value: T) -> Result<Oid<T>, AlreadyExists<T>> {
        let oid = self.next_oid();
        if self.name_mapping.insert(value.name(), oid).is_some() {
            return Err(AlreadyExists(value));
        }

        self.entries.entry(oid).or_default().push_version(CatalogEntry::new(tx, value));
        Ok(oid)
    }

    fn next_oid(&self) -> Oid<T> {
        // FIXME this won't work under concurrent workloads, we should use an atomic counter instead
        // We can run into the case where the lengths are not empty if we are halfway through an insert
        assert_eq!(self.entries.len(), self.name_mapping.len());
        Oid::new(self.entries.len() as u64)
    }
}

#[derive(Debug)]
struct CatalogEntry<T> {
    txid: Txid,
    value: Arc<T>,
    deleted: bool,
}

impl<T> Clone for CatalogEntry<T> {
    fn clone(&self) -> Self {
        Self { txid: self.txid, value: Arc::clone(&self.value), deleted: self.deleted }
    }
}

impl<T> CatalogEntry<T> {
    pub(crate) fn new(tx: &Transaction, value: T) -> Self {
        Self { txid: tx.id(), value: Arc::new(value), deleted: false }
    }

    pub fn txid(&self) -> Txid {
        self.txid
    }

    pub fn value(&self) -> Arc<T> {
        Arc::clone(&self.value)
    }
}
