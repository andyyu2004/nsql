use nsql_btree::{BTree, Result};
use nsql_test::mk_fast_mem_buffer_pool;
use test_strategy::proptest;

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
fn test_btree_remove_removed_item_returns_none() -> Result<()> {
    nsql_test::start(async {
        let pool = mk_fast_mem_buffer_pool!();
        let btree = BTree::<u32, u64>::initialize(pool).await?;
        assert!(btree.insert(&1, &2).await?.is_none());
        assert_eq!(btree.remove(&1).await?, Some(2));
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

#[test]
#[tracing_test::traced_test]
fn test_btree_get_after_removal_multipage() -> Result<()> {
    nsql_test::start(async {
        let pool = mk_fast_mem_buffer_pool!();
        let btree = BTree::initialize(pool).await?;

        const N: u32 = 340;

        for i in (0..N).rev() {
            assert!(btree.insert(&i, &i).await?.is_none());
            assert_eq!(btree.get(&i).await?, Some(i));
            assert_eq!(btree.remove(&i).await?, Some(i));
            assert!(btree.get(&i).await?.is_none(), "i={i}");
            // this assertion was triggered by a former bug
            assert!(btree.get(&(i + 1)).await?.is_none(), "i+1={}", i + 1);
        }

        Ok(())
    })
}

#[test]
#[tracing_test::traced_test]
fn test_btree_remove_many() -> Result<()> {
    nsql_test::start(async {
        let pool = mk_fast_mem_buffer_pool!();
        let btree = BTree::initialize(pool).await?;

        const N: u32 = 1000;

        for i in 0..N {
            assert!(btree.insert(&i, &(i + 1)).await?.is_none());
        }

        for i in 0..N {
            assert_eq!(btree.remove(&i).await?, Some(i + 1));
        }

        for i in 0..N {
            assert!(btree.get(&i).await?.is_none());
            assert!(btree.find_min(&i).await?.is_none());
        }

        Ok(())
    })
}

#[proptest]
fn proptest_btree_remove_then_get(insertions: Vec<(u32, u32)>, removals: Vec<u32>) {
    nsql_test::start(async {
        let pool = mk_fast_mem_buffer_pool!();
        let btree = BTree::initialize(pool).await?;

        for (k, v) in insertions {
            btree.insert(&k, &v).await?;
        }

        for k in removals {
            btree.remove(&k).await?;
            assert!(btree.get(&k).await?.is_none());
        }

        Ok::<_, nsql_pager::Error>(())
    })
    .unwrap()
}
