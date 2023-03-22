use nsql_buffer::Pool;
use nsql_pager::PAGE_DATA_SIZE;

use crate::page::{InteriorPageViewMut, LeafPageViewMut, NodeMut, NodeViewMut};
use crate::Result;

#[test]
fn test_slotted_pages() -> Result<()> {
    nsql_test::start(async {
        let pool = nsql_test::mk_mem_buffer_pool!();
        let handle = pool.alloc().await?;
        let _page = handle.page().write();
        Ok(())
    })
}

#[test]
fn test_raw_bytes_mut() {
    let mut buf = [0u8; PAGE_DATA_SIZE];
    let mut view = InteriorPageViewMut::<u32>::initialize(&mut buf);
    let raw_bytes = *view.raw_bytes_mut();
    assert_eq!(raw_bytes, buf);

    let mut view = LeafPageViewMut::<u32, u64>::initialize(&mut buf);
    let raw_bytes = *view.raw_bytes_mut();
    assert_eq!(raw_bytes, buf);
}
