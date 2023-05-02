use std::marker::PhantomData;
use std::sync::Arc;

use async_stream::try_stream;
use futures_util::Stream;
use nsql_buffer::{BufferHandle, Pool};
use nsql_pager::PageIndex;
use nsql_rkyv::{archived_size_of, DefaultSerializer};
use nsql_transaction::{Transaction, Txid, Version};
use rkyv::option::ArchivedOption;
use rkyv::with::Inline;
use rkyv::{Archive, Archived, Deserialize, Serialize};

use self::view::{HeapView, HeapViewMut, Slot, SlotIndex};
use super::fsm::FreeSpaceMap;

mod view;

pub struct Heap<T> {
    meta_page: BufferHandle,
    fsm: FreeSpaceMap,
    pool: Arc<dyn Pool>,
    _phantom: std::marker::PhantomData<T>,
}

// this is intentionally different to the magic on a heap data page header
const HEAP_META_MAGIC: [u8; 4] = *b"HEPM";

#[derive(Debug, Archive, Serialize, Deserialize)]
#[repr(C)]
struct HeapMeta {
    magic: [u8; 4],
    meta_page_idx: PageIndex,
    fsm_meta_page_idx: PageIndex,
    head_and_tail: Option<HeadAndTail>,
}

#[derive(Debug, Archive, Serialize, Deserialize)]
#[repr(C)]
struct HeadAndTail {
    head: PageIndex,
    tail: PageIndex,
}

impl From<HeadAndTail> for Archived<HeadAndTail> {
    fn from(head_and_tail: HeadAndTail) -> Self {
        Self { head: head_and_tail.head.into(), tail: head_and_tail.tail.into() }
    }
}

impl<T> Heap<T> {
    pub async fn initialize(pool: Arc<dyn Pool>) -> nsql_buffer::Result<Self> {
        let meta_page = pool.alloc().await?;
        let fsm = FreeSpaceMap::initialize(Arc::clone(&pool)).await?;
        let meta = HeapMeta {
            magic: HEAP_META_MAGIC,
            fsm_meta_page_idx: fsm.meta_page_idx(),
            meta_page_idx: meta_page.page_idx(),
            head_and_tail: None,
        };

        let mut guard = meta_page.write().await;
        let bytes = nsql_rkyv::to_bytes(&meta);
        guard[..bytes.len()].copy_from_slice(&bytes);
        drop(guard);

        Ok(Self { meta_page, fsm, pool, _phantom: PhantomData })
    }

    pub async fn load(pool: Arc<dyn Pool>, meta_page_idx: PageIndex) -> nsql_buffer::Result<Self> {
        let meta_page = pool.load(meta_page_idx).await?;
        let guard = meta_page.read().await;
        let (meta_bytes, _) = guard.split_array_ref();
        let meta = unsafe { nsql_rkyv::archived_root::<HeapMeta>(meta_bytes) };
        let meta = nsql_rkyv::deserialize::<HeapMeta>(meta);
        drop(guard);

        let fsm = FreeSpaceMap::load(Arc::clone(&pool), meta.fsm_meta_page_idx).await?;

        Ok(Self { meta_page, fsm, pool, _phantom: PhantomData })
    }
}

/// A stable identifier for an item in the heap
pub struct HeapId {
    page_idx: PageIndex,
    slot: SlotIndex,
}

#[derive(Archive, Serialize)]
pub struct Versioned<'a, T> {
    version: Version,
    #[with(Inline)]
    data: &'a T,
}

impl<T> Heap<T>
where
    T: Serialize<DefaultSerializer>,
{
    pub async fn get(&self, tx: &Transaction, id: HeapId) -> nsql_buffer::Result<Option<T>>
    where
        T::Archived: Deserialize<T, rkyv::Infallible>,
    {
        // FIXME respect transaction
        let page = self.pool.load(id.page_idx).await?;
        let guard = page.read().await;
        let view = HeapView::<T>::view(&guard)?;
        Ok(view.get(tx, id.slot))
    }

    pub async fn append(&self, tx: &Transaction, tuple: &T) -> nsql_buffer::Result<HeapId> {
        let serialized = nsql_rkyv::to_bytes(&Versioned { version: tx.version(), data: tuple });

        self.with_free_space(serialized.len() as u16, |page_idx, view| {
            let slot = unsafe { view.append_raw(tx, &serialized) }
                .expect("there should be sufficient space as we checked the fsm");
            Ok(HeapId { page_idx, slot })
        })
        .await
    }

    pub async fn scan(
        &self,
        tx: Arc<Transaction>,
    ) -> impl Stream<Item = nsql_buffer::Result<Vec<T>>> + Send
    where
        T: Send + Sync,
        T::Archived: Deserialize<T, rkyv::Infallible> + Send + Sync,
    {
        let pool = Arc::clone(&self.pool);
        let mut next = {
            let meta_guard = self.meta_page.read().await;
            let (meta_bytes, _) = meta_guard.split_array_ref();
            let meta = unsafe { nsql_rkyv::archived_root::<HeapMeta>(meta_bytes) };

            meta.head_and_tail.as_ref().map(|h| h.head.into())
        };

        try_stream! {
            while let Some(idx) = next {
                let mut tuples = vec![];
                let head = pool.load(idx).await?;
                let guard = head.read().await;
                let view = HeapView::<T>::view(&guard)?;
                view.scan_into(&tx, &mut tuples);
                next = view.right_link();
                yield tuples;
            }
        }
    }

    async fn with_free_space<R>(
        &self,
        required_space: u16,
        f: impl FnOnce(PageIndex, &mut HeapViewMut<'_, T>) -> nsql_buffer::Result<R>,
    ) -> nsql_buffer::Result<R> {
        let required_space = required_space + archived_size_of!(Slot);

        match self.fsm.find(required_space).await? {
            Some(idx) => {
                let page = self.pool.load(idx).await?;
                let mut guard = page.write().await;

                let mut view = HeapViewMut::<T>::view_mut(&mut guard)?;
                debug_assert!(view.free_space() >= required_space);
                let r = f(page.page_idx(), &mut view)?;
                let updated_free_space = view.free_space();
                // assume that the caller will write to the page and use the requested space
                self.fsm.update(&guard, updated_free_space).await?;
                Ok(r)
            }
            None => {
                let page = self.pool.alloc().await?;
                let page_idx = page.page_idx();
                let mut guard = page.write().await;

                let view = HeapViewMut::<T>::initialize(&mut guard);
                let initial_free_space = view.free_space();
                assert!(initial_free_space >= required_space);
                self.fsm.update(&guard, initial_free_space).await?;
                let mut view = HeapViewMut::<T>::view_mut(&mut guard)?;
                let r = f(page.page_idx(), &mut view)?;
                let updated_free_space = view.free_space();

                // update left and right page links
                let mut meta_guard = self.meta_page.write().await;
                let (meta_bytes, _) = meta_guard.split_array_mut();
                let mut meta = unsafe { nsql_rkyv::archived_root_mut::<HeapMeta>(meta_bytes) };

                match meta.head_and_tail.as_mut() {
                    Some(ArchivedHeadAndTail { head: _, tail }) => {
                        // if this was not the first allocated page, update the tail page's right link to the new page,
                        // the new page's left link to the prev tail page, and the tail page index to the new page
                        let prev_tail_page = self.pool.load((*tail).into()).await.unwrap();
                        view.set_left_link((*tail).into());
                        let mut prev_tail_guard = prev_tail_page.write().await;
                        let mut prev_tail_view = HeapViewMut::<T>::view_mut(&mut prev_tail_guard)?;
                        prev_tail_view.set_right_link(page_idx);
                        *tail = page_idx.into();
                    }
                    None => {
                        // if this was the first allocated page, update the first and last page indices
                        meta.head_and_tail = ArchivedOption::Some(
                            HeadAndTail { head: page_idx, tail: page_idx }.into(),
                        );
                    }
                }

                self.fsm.update(&guard, updated_free_space).await?;

                Ok(r)
            }
        }
    }
}

#[cfg(test)]
mod tests;
