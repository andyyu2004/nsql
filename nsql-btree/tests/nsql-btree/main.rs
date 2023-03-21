use std::fmt;

use nsql_btree::{BTree, Min, Result};
use nsql_rkyv::DefaultSerializer;
use nsql_test::mk_fast_mem_buffer_pool;
use rkyv::{Archive, Deserialize, Serialize};
use test_strategy::proptest;

#[test]
fn test_btree_empty() -> Result<()> {
    nsql_test::start(async {
        let pool = mk_fast_mem_buffer_pool!();
        let btree = BTree::<u32, u64>::initialize(pool).await?;
        assert!(btree.get(&1).await?.is_none());
        Ok(())
    })
}

#[test]
fn test_btree_insert_and_get() -> Result<()> {
    nsql_test::start(async {
        let pool = mk_fast_mem_buffer_pool!();
        let btree = BTree::<u32, u64>::initialize(pool).await?;
        btree.insert(&42, &420).await?;
        assert_eq!(btree.get(&42).await?, Some(420));
        Ok(())
    })
}

#[test]
fn test_btree_insert_many_and_get() -> Result<()> {
    nsql_test::start(async {
        let pool = mk_fast_mem_buffer_pool!();
        let btree = BTree::<u32, u64>::initialize(pool).await?;
        for i in 1..50 {
            btree.insert(&i, &(i as u64)).await?;
            assert_eq!(btree.get(&i).await?, Some(i as u64));
        }
        Ok(())
    })
}

#[test]
fn test_btree_root_leaf_page_split() -> Result<()> {
    cov_mark::check!(root_leaf_split);
    run_serial_inserts::<300>()
}

#[test]
fn test_btree_leaf_page_split() -> Result<()> {
    cov_mark::check!(non_root_leaf_split);
    run_serial_inserts::<500>()
}

#[test]
fn test_btree_root_interior_page_split() -> Result<()> {
    cov_mark::check!(root_interior_split);
    run_serial_inserts::<34239>()
}

#[test]
fn test_btree_interior_page_split() -> Result<()> {
    cov_mark::check!(non_root_interior_split);
    run_serial_inserts::<60000>()
}

fn run_serial_inserts<const N: u32>() -> Result<()> {
    nsql_test::start(async {
        let pool = mk_fast_mem_buffer_pool!();
        let btree = BTree::<u32, u64>::initialize(pool).await?;
        for i in 0..=N {
            btree.insert(&i, &(i as u64)).await?;
        }

        for i in 0..=N {
            assert_eq!(btree.get(&i).await?, Some(i as u64));
        }

        Ok(())
    })
}

#[test]
fn test_out_of_order_insert() {
    let pairs = [(1, 0), (0, 0)];
    run_insertions(&pairs).unwrap()
}

#[proptest]
fn test_btree_insert_and_get_random(pairs: Vec<(u32, u16)>) {
    run_insertions(&pairs).unwrap()
}

fn run_insertions<K, V>(pairs: &[(K, V)]) -> Result<()>
where
    K: Min + Archive + Serialize<DefaultSerializer> + fmt::Debug,
    K::Archived: PartialOrd<K> + Clone + fmt::Debug + Ord,
    V: Archive + Eq + Serialize<DefaultSerializer> + Clone + fmt::Debug,
    V::Archived: Clone + Deserialize<V, rkyv::Infallible> + fmt::Debug,
{
    nsql_test::start(async {
        let pool = mk_fast_mem_buffer_pool!();
        let btree = BTree::<K, V>::initialize(pool).await?;
        for (key, value) in pairs {
            btree.insert(key, value).await?;
            assert_eq!(&btree.get(key).await?.unwrap(), value);
        }
        Ok(())
    })
}
