use std::sync::Arc;

use nsql_buffer::Pool;
use nsql_test::mk_fast_mem_buffer_pool;
use test_strategy::proptest;

use super::FreeSpaceMap;

#[test]
fn test_fsm() -> nsql_buffer::Result<()> {
    nsql_test::start(async {
        let pool = mk_fast_mem_buffer_pool!();
        let pager = Arc::clone(pool.pager());
        let fsm = FreeSpaceMap::initialize(pool).await?;
        assert!(fsm.find(1).await?.is_none());

        let idx = pager.alloc_page().await?;

        fsm.update(idx, 2).await?;
        assert_eq!(fsm.find(1).await?, Some(idx));

        fsm.update(idx, 0).await?;
        assert_eq!(fsm.find(1).await?, None);

        Ok(())
    })
}

#[test]
fn test_fsm_multiple_same_size() -> nsql_buffer::Result<()> {
    nsql_test::start(async {
        let pool = mk_fast_mem_buffer_pool!();
        let pager = Arc::clone(pool.pager());
        let fsm = FreeSpaceMap::initialize(pool).await?;

        let a = pager.alloc_page().await?;
        let b = pager.alloc_page().await?;

        fsm.update(a, 2).await?;
        fsm.update(b, 2).await?;

        assert_eq!(fsm.find(1).await?, Some(a));
        fsm.update(a, 0).await?;
        assert_eq!(fsm.find(1).await?, Some(b));

        Ok(())
    })
}

#[proptest]
fn proptest_fsm() {
    nsql_test::start(async {})
}
