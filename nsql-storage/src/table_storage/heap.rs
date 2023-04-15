use std::marker::PhantomData;
use std::sync::Arc;

use nsql_buffer::Pool;
use nsql_pager::PageIndex;
use nsql_transaction::Transaction;
use rkyv::{Archive, Deserialize, Serialize};

use self::view::{HeapViewMut, SlotIndex};
use super::fsm::FreeSpaceMap;
use crate::tuple::Tuple;

mod view;

pub struct Heap<T> {
    meta: HeapMeta,
    fsm: FreeSpaceMap,
    pool: Arc<dyn Pool>,
    _phantom: std::marker::PhantomData<T>,
}

// this is intentionally different to the magic on a heap data page header
const HEAP_META_MAGIC: [u8; 4] = *b"HEPM";

#[derive(Debug, Archive, Serialize, Deserialize)]
struct HeapMeta {
    magic: [u8; 4],
    meta_page_idx: PageIndex,
    fsm_meta_page_idx: PageIndex,
}

impl<T> Heap<T> {
    pub async fn initalize(pool: Arc<dyn Pool>) -> nsql_buffer::Result<Self> {
        let meta_page = pool.alloc().await?;
        let fsm = FreeSpaceMap::initialize(Arc::clone(&pool)).await?;
        let meta = HeapMeta {
            magic: HEAP_META_MAGIC,
            fsm_meta_page_idx: fsm.meta_page_idx(),
            meta_page_idx: meta_page.page_idx(),
        };

        let mut guard = meta_page.write();
        let bytes = nsql_rkyv::to_bytes(&meta);
        guard[..bytes.len()].copy_from_slice(&bytes);

        Ok(Self { meta, fsm, pool, _phantom: PhantomData })
    }

    pub async fn load(pool: Arc<dyn Pool>, meta_page_idx: PageIndex) -> nsql_buffer::Result<Self> {
        let meta_page = pool.load(meta_page_idx).await?;
        let guard = meta_page.read();
        let (meta_bytes, _) = guard.split_array_ref();
        let meta = unsafe { nsql_rkyv::archived_root::<HeapMeta>(meta_bytes) };
        let meta = nsql_rkyv::deserialize::<HeapMeta>(meta);

        let fsm = FreeSpaceMap::load(Arc::clone(&pool), meta.fsm_meta_page_idx).await?;

        Ok(Self { meta, fsm, pool, _phantom: PhantomData })
    }
}

/// A stable identifier for an item in the heap
pub struct HeapId {
    page_idx: PageIndex,
    slot: SlotIndex,
}

impl<T: Archive> Heap<T> {
    pub async fn append(&self, _tx: &Transaction, tuple: Tuple) -> nsql_buffer::Result<HeapId> {
        let serialized = nsql_rkyv::to_bytes(&tuple);
        self.with_free_space(serialized.len() as u16, |page_idx, view| {
            let slot = unsafe { view.push_raw(&serialized) }
                .expect("there should be sufficient space as we checked the fsm");
            Ok(HeapId { page_idx, slot })
        })
        .await
    }

    async fn with_free_space<R>(
        &self,
        required_space: u16,
        f: impl FnOnce(PageIndex, &mut HeapViewMut<'_, T>) -> nsql_buffer::Result<R>,
    ) -> nsql_buffer::Result<R> {
        match self.fsm.find(required_space).await? {
            Some(idx) => {
                let page = self.pool.load(idx).await?;
                let mut guard = page.write();

                let mut view = HeapViewMut::<T>::view_mut(&mut guard)?;
                let free_space = view.free_space();
                assert!(free_space >= required_space);
                let r = f(page.page_idx(), &mut view)?;
                // assume that the caller will write to the page and use the requested space
                self.fsm.update(&guard, free_space - required_space).await?;
                Ok(r)
            }
            None => {
                let page = self.pool.alloc().await?;
                let mut guard = page.write();

                let view = HeapViewMut::<T>::initialize(&mut guard);
                let free_space = view.free_space();
                assert!(free_space >= required_space);
                self.fsm.update(&guard, free_space).await?;
                let mut view = HeapViewMut::<T>::view_mut(&mut guard)?;
                let r = f(page.page_idx(), &mut view)?;
                // assume that the caller will write to the page and use the requested space
                self.fsm.update(&guard, free_space - required_space).await?;
                Ok(r)
            }
        }
    }
}

#[cfg(test)]
mod tests;
