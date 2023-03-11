use std::io;
use std::sync::Arc;

use dashmap::DashMap;
use nsql_core::Name;
use nsql_serde::{
    AsyncReadExt, AsyncWriteExt, Deserialize, DeserializeWith, Deserializer, Serialize, Serializer,
};
use nsql_transaction::{Transaction, Txid};

use crate::entry::Oid;
use crate::private::CatalogEntity;

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

impl<T: Serialize> Serialize for CatalogSet<T> {
    async fn serialize(&self, ser: &mut dyn Serializer) -> nsql_serde::Result<()> {
        ser.write_u32(self.entries.len() as u32).await?;
        for entry in self.entries.iter() {
            entry.value().committed_version().item().serialize(ser).await?;
        }
        Ok(())
    }
}

impl<T> DeserializeWith for CatalogSet<T>
where
    T: CatalogEntity,
    T::CreateInfo: Deserialize,
{
    type Context<'a> = Transaction;

    async fn deserialize_with(
        tx: &Self::Context<'_>,
        de: &mut dyn Deserializer<'_>,
    ) -> nsql_serde::Result<Self> {
        let len = de.read_u32().await? as usize;
        let set = Self::with_capacity(len);
        for _ in 0..len {
            let item = T::CreateInfo::deserialize(de).await?;
            set.create(tx, item);
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
    pub(crate) fn entries<'a>(&'a self, tx: &'a Transaction) -> Vec<Arc<T>> {
        self.entries
            .iter()
            .flat_map(|entry| entry.version_for_tx(tx))
            .map(|entry| entry.item())
            .collect()
    }

    pub(crate) fn get(&self, tx: &Transaction, oid: Oid<T>) -> Option<Arc<T>> {
        self.entries.get(&oid).and_then(|entry| entry.version_for_tx(tx)).map(|entry| entry.item())
    }

    pub(crate) fn get_by_name(&self, tx: &Transaction, name: &str) -> Option<(Oid<T>, Arc<T>)> {
        self.find(name).and_then(|oid| self.get(tx, oid).map(|item| (oid, item)))
    }

    pub(crate) fn find(&self, name: impl AsRef<str>) -> Option<Oid<T>> {
        Some(*self.name_mapping.get(name.as_ref())?.value())
    }

    #[inline]
    fn create(&self, tx: &Transaction, info: T::CreateInfo) -> Oid<T> {
        self.insert(tx, T::new(info))
    }

    pub(crate) fn insert(&self, tx: &Transaction, value: T) -> Oid<T> {
        let oid = self.next_oid();
        self.name_mapping.insert(value.name().clone(), oid);
        self.entries.entry(oid).or_default().push_version(CatalogEntry::new(tx, value));
        oid
    }

    fn next_oid(&self) -> Oid<T> {
        assert_eq!(self.entries.len(), self.name_mapping.len());
        Oid::new(self.entries.len() as u64)
    }
}

#[derive(Debug)]
struct CatalogEntry<T> {
    txid: Txid,
    item: Arc<T>,
    deleted: bool,
}

impl<T> Clone for CatalogEntry<T> {
    fn clone(&self) -> Self {
        Self { txid: self.txid, item: Arc::clone(&self.item), deleted: self.deleted }
    }
}

impl<T> CatalogEntry<T> {
    pub(crate) fn new(tx: &Transaction, value: T) -> Self {
        Self { txid: tx.id(), item: Arc::new(value), deleted: false }
    }

    pub fn txid(&self) -> Txid {
        self.txid
    }

    pub fn item(&self) -> Arc<T> {
        Arc::clone(&self.item)
    }
}
