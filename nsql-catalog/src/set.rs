use std::collections::HashMap;
use std::sync::Arc;

use nsql_transaction::{Transaction, Txid};

use crate::entry::{EntryName, Oid};
use crate::CatalogEntity;

pub(crate) struct CatalogSet<T> {
    entries: HashMap<Oid, VersionedEntry<T>>,
    name_mapping: HashMap<EntryName, Oid>,
}

struct VersionedEntry<T> {
    versions: Vec<CatalogEntry<T>>,
}

impl<T> Default for VersionedEntry<T> {
    fn default() -> Self {
        Self { versions: Default::default() }
    }
}

impl<T> VersionedEntry<T> {
    fn version_for_tx(&self, tx: &Transaction) -> Option<&CatalogEntry<T>> {
        self.versions.iter().rev().find(|version| tx.can_see(version.txid()))
    }

    fn push_version(&mut self, version: CatalogEntry<T>) {
        self.versions.push(version)
    }
}

impl<T: CatalogEntity> CatalogSet<T> {
    pub(crate) fn entries<'a>(
        &'a self,
        tx: &'a Transaction,
    ) -> impl Iterator<Item = &'a CatalogEntry<T>> {
        self.entries.values().flat_map(|entry| entry.version_for_tx(tx))
    }

    pub(crate) fn find(&self, tx: &Transaction, name: impl AsRef<str>) -> Option<&CatalogEntry<T>> {
        self.name_mapping
            .get(name.as_ref())
            .map(|oid| self.entries.get(oid).expect("mapping points to non-existent entry"))
            .and_then(|entry| entry.version_for_tx(tx))
    }

    pub(crate) fn insert(&mut self, tx: &Transaction, value: T) {
        let oid = self.next_oid();
        self.name_mapping.insert(value.name().clone(), oid);
        self.entries.entry(oid).or_default().push_version(CatalogEntry::new(tx, value));
    }

    fn next_oid(&self) -> Oid {
        assert_eq!(self.entries.len(), self.name_mapping.len());
        Oid::new(self.entries.len() as u64)
    }
}

pub struct CatalogEntry<T> {
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
}
