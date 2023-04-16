use nsql_transaction::TransactionManager;

use super::Heap;

#[test]
fn test_heap_page_append() -> nsql_buffer::Result<()> {
    nsql_test::start(async {
        let txm = TransactionManager::new();
        let tx = txm.begin().await;

        let pool = nsql_test::mk_fast_mem_buffer_pool!();
        let heap = Heap::initialize(pool).await?;

        const N: u32 = 3000;
        for i in 0..N {
            let id = heap.append(&tx, &i).await?;
            assert_eq!(heap.get(&tx, id).await?, i);
        }

        Ok(())
    })
}

#[test]
fn test_heap_scan() -> nsql_buffer::Result<()> {
    nsql_test::start(async {
        let txm = TransactionManager::new();
        let tx = txm.begin().await;

        let pool = nsql_test::mk_fast_mem_buffer_pool!();
        let heap = Heap::initialize(pool).await?;

        const N: u32 = 500;
        for i in 0..N {
            heap.append(&tx, &i).await?;
        }

        let values = heap.scan(&tx).await?;
        assert_eq!(values.len(), N as usize);
        assert_eq!(values, (0..N).collect::<Vec<_>>());

        Ok(())
    })
}
