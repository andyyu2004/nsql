use nsql_btree::BTree;
use nsql_test::mk_mem_buffer_pool;

#[test]
fn test_btree_insert_and_get() -> nsql_serde::Result<()> {
    nsql_test::start(async {
        let pool = mk_mem_buffer_pool!();
        let btree = BTree::<u32, u64>::create(pool).await?;
        assert!(btree.insert(1, 2).await?.is_none());
        assert_eq!(btree.get(&1).await?, Some(2));
        Ok(())
    })
}

#[test]
fn test_btree_insert_many_and_get() -> nsql_serde::Result<()> {
    nsql_test::start(async {
        let pool = mk_mem_buffer_pool!();
        let btree = BTree::<u32, u64>::create(pool).await?;
        for i in 0..2 {
            assert!(btree.insert(i, i as u64).await?.is_none());
            assert_eq!(btree.get(&i).await?, Some(i as u64));
        }
        Ok(())
    })
}
