use nsql_btree::{BTree, Result};
use nsql_test::mk_fast_mem_buffer_pool;

#[test]
fn test_btree_remove_from_empty() -> Result<()> {
    nsql_test::start(async {
        let pool = mk_fast_mem_buffer_pool!();
        let btree = BTree::<u32, u64>::initialize(pool).await?;
        assert!(btree.remove(&1).await?.is_none());
        Ok(())
    })
}

#[test]
fn test_btree_get_after_removal_returns_none() -> Result<()> {
    nsql_test::start(async {
        let pool = mk_fast_mem_buffer_pool!();
        let btree = BTree::<u32, u64>::initialize(pool).await?;
        assert!(btree.insert(&1, &2).await?.is_none());
        assert_eq!(btree.get(&1).await?, Some(2));
        assert_eq!(btree.remove(&1).await?, Some(2));
        assert!(btree.get(&1).await?.is_none());
        Ok(())
    })
}
