use nsql_buffer::Pool;

use super::view::HeapViewMut;

#[test]
fn test_heap_basic() -> nsql_buffer::Result<()> {
    nsql_test::start(async {
        let pool = nsql_test::mk_fast_mem_buffer_pool!();
        let handle = pool.alloc().await?;
        let mut guard = handle.write();
        let mut heap = HeapViewMut::initialize(&mut guard);

        const N: u32 = 500;
        for i in 0..N {
            let idx = heap.push(&i).unwrap();
            assert_eq!(heap[idx], i);
        }

        Ok(())
    })
}
