use nsql_test::mk_mem_buffer_pool;

use super::FreeSpaceMap;

#[test]
fn test_fsm() -> nsql_buffer::Result<()> {
    nsql_test::start(async {
        let pool = mk_mem_buffer_pool!();
        let fsm = FreeSpaceMap::create(pool.clone()).await?;
        let pager = pool.pager();

        let index = pager.alloc_page().await?;
        fsm.update(index, 10).await?;
        Ok(())
    })
}
