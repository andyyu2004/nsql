use tokio::task::JoinSet;

use super::*;

#[proptest]
fn test_concurrent_inserts(inputs: ConcurrentTestInputs<u32, u64>) {
    run_concurrent_insertions(inputs).unwrap();
}

type ConcurrentTestInputs<K, V> = Vec<Box<[(K, V)]>>;

fn run_concurrent_insertions<K, V>(inputs: ConcurrentTestInputs<K, V>) -> Result<()>
where
    K: Min + Archive + Serialize<DefaultSerializer> + fmt::Debug + 'static,
    K::Archived: PartialOrd<K> + Clone + fmt::Debug + Ord,
    V: Archive + Eq + Serialize<DefaultSerializer> + Clone + fmt::Debug + 'static,
    V::Archived: Clone + Deserialize<V, rkyv::Infallible> + fmt::Debug,
{
    nsql_test::start(async {
        let pool = mk_fast_mem_buffer_pool!();
        let btree = BTree::<K, V>::initialize(pool).await?;
        // let mut set = JoinSet::<Result<()>>::new();
        for input in inputs {
            // let btree = BTree::clone(&btree);
            // set.spawn_local(async move {
            for (key, value) in input.iter() {
                btree.insert(key, value).await?;
                assert_eq!(&btree.get(key).await?.unwrap(), value);
            }
            // Ok(())
            // });
        }

        // while let Some(res) = set.join_next().await {
        //     res.unwrap()?;
        // }

        Ok(())
    })
}
