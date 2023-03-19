use nsql_btree::BTree;
use nsql_test::mk_mem_buffer_pool;

#[test]
fn test_btree_empty() -> nsql_serde::Result<()> {
    nsql_test::start(async {
        let pool = mk_mem_buffer_pool!();
        let btree = BTree::<u32, u64>::init(pool).await?;
        assert!(btree.get(&1).await?.is_none());
        Ok(())
    })
}

#[test]
fn test_btree_insert_and_get() -> nsql_serde::Result<()> {
    nsql_test::start(async {
        let pool = mk_mem_buffer_pool!();
        let btree = BTree::<u32, u64>::init(pool).await?;
        btree.insert(&42, &420).await?;
        assert_eq!(btree.get(&42).await?, Some(420));
        Ok(())
    })
}

#[test]
fn test_btree_insert_many_and_get() -> nsql_serde::Result<()> {
    nsql_test::start(async {
        let pool = mk_mem_buffer_pool!();
        let btree = BTree::<u32, u64>::init(pool).await?;
        for i in 1..200 {
            btree.insert(&i, &(i as u64)).await?;
            assert_eq!(btree.get(&i).await?, Some(i as u64));
        }
        Ok(())
    })
}

#[test]
fn test_btree_root_page_full() -> nsql_serde::Result<()> {
    nsql_test::start(async {
        let pool = mk_mem_buffer_pool!();
        let btree = BTree::<u32, u64>::init(pool).await?;
        const N: u32 = 50000;
        for i in 0..=N {
            btree.insert(&i, &(i as u64)).await?;
            assert_eq!(btree.get(&i).await?, Some(i as u64));
        }

        for i in 0..=N {
            assert_eq!(btree.get(&i).await?, Some(i as u64));
        }

        Ok(())
    })
}

#[test]
fn test_btree_propogate_split() -> nsql_serde::Result<()> {
    nsql_test::start(async {
        let pool = mk_mem_buffer_pool!();
        let btree = BTree::<u32, u64>::init(pool).await?;
        const N: u32 = 100000;
        for i in 0..=N {
            btree.insert(&i, &(i as u64)).await?;
            assert_eq!(btree.get(&i).await?, Some(i as u64));
        }

        for i in 0..=N {
            assert_eq!(btree.get(&i).await?, Some(i as u64));
        }

        Ok(())
    })
}

// // FIXME write some visualization tools to help see what's going on in the btree
// #[bench]
// fn test_btree_propogate_split2() -> nsql_serde::Result<()> {
//     nsql_test::start(async {
//         let pool = mk_mem_buffer_pool!();
//         let btree = BTree::<u32, u64>::init(pool).await?;
//         const N: u32 = 1000000;
//         for i in 0..=N {
//             assert!(btree.insert(i, i as u64).await?.is_none());
//             assert_eq!(btree.get(&i).await?, Some(i as u64));
//         }

//         for i in 0..=N {
//             assert_eq!(btree.get(&i).await?, Some(i as u64));
//         }

//         Ok(())
//     })
// }
