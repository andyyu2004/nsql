use nsql_buffer::Pool;
use nsql_test::mk_fast_mem_buffer_pool;

use super::FreeSpaceMap;

#[test]
fn test_fsm() -> nsql_buffer::Result<()> {
    nsql_test::start(async {
        let pool = mk_fast_mem_buffer_pool!();
        let fsm = FreeSpaceMap::initialize(pool.clone()).await?;
        assert!(fsm.find(1).await?.is_none());

        let page = pool.alloc().await?;
        let guard = page.write().await;

        fsm.update(&guard, 2).await?;
        assert_eq!(fsm.find(1).await?, Some(guard.page_idx()));

        fsm.update(&guard, 0).await?;
        assert_eq!(fsm.find(1).await?, None);

        Ok(())
    })
}

#[test]
fn test_fsm_multiple_same_size() -> nsql_buffer::Result<()> {
    nsql_test::start(async {
        let pool = mk_fast_mem_buffer_pool!();
        let fsm = FreeSpaceMap::initialize(pool.clone()).await?;

        let a = pool.alloc().await?;
        let a = a.write().await;
        let b = pool.alloc().await?;
        let b = b.write().await;

        fsm.update(&a, 2).await?;
        fsm.update(&b, 2).await?;

        assert_eq!(fsm.find(1).await?, Some(a.page_idx()));
        fsm.update(&a, 0).await?;
        assert_eq!(fsm.find(1).await?, Some(b.page_idx()));

        Ok(())
    })
}

#[test]
#[tracing_test::traced_test]
fn test_fsm_update_removes_old_entries() -> nsql_buffer::Result<()> {
    nsql_test::start(async {
        let pool = mk_fast_mem_buffer_pool!();
        let fsm = FreeSpaceMap::initialize(pool.clone()).await?;

        let handle = pool.alloc().await?;
        let guard = handle.write().await;

        for i in (1..150).rev() {
            fsm.update(&guard, i).await?;
            assert_eq!(fsm.find(i).await?, Some(guard.page_idx()));
            assert_eq!(
                fsm.find(i + 1).await?,
                None,
                "the previous entry should have been removed, but found space for `{}`",
                i + 1
            );
        }

        fsm.update(&guard, 1).await?;
        assert_eq!(fsm.find(1).await?, Some(guard.page_idx()));
        assert_eq!(fsm.find(2).await?, None);

        Ok(())
    })
}
