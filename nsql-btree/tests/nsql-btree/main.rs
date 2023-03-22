mod concurrent;

use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;

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
        assert!(btree.insert(&42, &420).await?.is_none());
        assert_eq!(btree.get(&42).await?, Some(420));
        Ok(())
    })
}

#[test]
fn test_btree_duplicate_key() -> Result<()> {
    cov_mark::check!(slotted_page_insert_duplicate);
    run_insertions(&[(1, 42), (1, 43)])
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
            assert!(
                btree.insert(&i, &(i as u64)).await?.is_none(),
                "there should be no previous value as all keys are serially unique"
            );
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
fn test_btree_insert_and_get_random(pairs: Box<[(u32, u16)]>) {
    run_insertions(&pairs).unwrap()
}

#[test]
fn test_insert_duplicate_reuse_slot() -> Result<()> {
    cov_mark::check!(slotted_page_insert_duplicate_reuse);
    let inputs = (0..10000).map(|i| (i % 7, i)).collect::<Vec<_>>();
    run_insertions(&inputs)
}

#[test]
fn test_insert_duplicate_into_full_page() -> Result<()> {
    // Inserting duplicates into a full page requires some special handling.
    // This testcase tests the easy case where we can reuse the evicted slot.
    cov_mark::check!(slotted_page_insert_duplicate_full_reuse);
    // We exercise the case by filling up a page and then inserting the same keys again.
    let inputs = (0..2).flat_map(|_| (0..580).map(|i| (i, i))).collect::<Vec<_>>();
    run_insertions(&inputs)
}

fn run_insertions<K, V>(pairs: &[(K, V)]) -> Result<()>
where
    K: Min + Archive + Serialize<DefaultSerializer> + Eq + Hash + fmt::Debug,
    K::Archived: Deserialize<K, rkyv::Infallible> + PartialOrd<K> + Clone + fmt::Debug + Ord,
    V: Archive + Eq + Serialize<DefaultSerializer> + Clone + fmt::Debug,
    V::Archived: Clone + Deserialize<V, rkyv::Infallible> + fmt::Debug,
{
    nsql_test::start(async {
        let pool = mk_fast_mem_buffer_pool!();
        let btree = BTree::<K, V>::initialize(pool).await?;
        let mut expected_pairs = HashMap::<&K, &V>::default();
        for (key, value) in pairs {
            let expected_prev = expected_pairs.insert(key, value);
            assert_eq!(btree.insert(key, value).await?.as_ref(), expected_prev);
            assert_eq!(&btree.get(key).await?.unwrap(), value);
        }

        // check that the all values are still there
        for (key, value) in expected_pairs {
            assert_eq!(&btree.get(key).await?.unwrap(), value);
        }

        Ok(())
    })
}
