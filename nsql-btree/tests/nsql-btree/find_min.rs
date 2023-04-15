use nsql_btree::{BTree, Result};
use nsql_test::mk_fast_mem_buffer_pool;

#[test]
#[tracing_test::traced_test]
fn test_btree_find_min() -> Result<()> {
    nsql_test::start(async {
        let pool = mk_fast_mem_buffer_pool!();
        let btree = BTree::<u32, u64>::initialize(pool).await?;
        assert!(btree.find_min(&1).await?.is_none());

        assert!(btree.insert(&9, &1).await?.is_none());
        assert_eq!(btree.find_min(&8).await?, Some(1));
        assert_eq!(btree.find_min(&9).await?, Some(1));
        assert_eq!(btree.find_min(&10).await?, None);

        Ok(())
    })
}

#[test]
#[tracing_test::traced_test]
fn test_btree_find_min_after_removal() -> Result<()> {
    nsql_test::start(async {
        let pool = mk_fast_mem_buffer_pool!();
        let btree = BTree::<u32, u64>::initialize(pool).await?;
        assert!(btree.find_min(&1).await?.is_none());

        assert!(btree.insert(&9, &1).await?.is_none());
        assert_eq!(btree.find_min(&9).await?, Some(1));
        assert_eq!(btree.remove(&9).await?, Some(1));
        assert_eq!(btree.find_min(&9).await?, None);
        assert_eq!(btree.find_min(&8).await?, None);
        Ok(())
    })
}
