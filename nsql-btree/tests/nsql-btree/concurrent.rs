use super::*;

#[test]
fn test_concurrent_inserts() {}

// fn run_concurrent_insertions<K, V>() -> Result<()>
// where
//     K: Min + Archive + Serialize<DefaultSerializer> + fmt::Debug,
//     K::Archived: PartialOrd<K> + Clone + fmt::Debug + Ord,
//     V: Archive + Eq + Serialize<DefaultSerializer> + Clone + fmt::Debug,
//     V::Archived: Clone + Deserialize<V, rkyv::Infallible> + fmt::Debug,
// {
//     nsql_test::start(async {
//         let pool = mk_fast_mem_buffer_pool!();
//         let btree = BTree::<K, V>::initialize(pool).await?;
//         // for (key, value) in pairs {
//         //     btree.insert(key, value).await?;
//         //     assert_eq!(&btree.get(key).await?.unwrap(), value);
//         // }
//         Ok(())
//     })
// }
