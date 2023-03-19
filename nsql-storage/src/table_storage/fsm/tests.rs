use std::sync::Arc;

use nsql_buffer::Pool;
use nsql_test::mk_mem_buffer_pool;

use super::FreeSpaceMap;

#[test]
fn test_fsm() -> nsql_buffer::Result<()> {
    nsql_test::start(async {
        let pool = mk_mem_buffer_pool!();
        let pager = Arc::clone(pool.pager());
        let fsm = FreeSpaceMap::create(pool).await?;

        let index = pager.alloc_page().await?;
        fsm.update(index, 10).await?;
        Ok(())
    })
}
