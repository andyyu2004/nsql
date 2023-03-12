#[test]
fn test_slotted_pages() -> nsql_serde::Result<()> {
    nsql_test::start(async {
        let pool = nsql_test::mk_mem_buffer_pool!();
        let handle = pool.alloc().await?;
        let _page = handle.page().data_mut();
        Ok(())
    })
}
