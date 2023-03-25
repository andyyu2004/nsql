use tokio::task::JoinSet;

use super::*;

#[test]
fn test_concurrent_inserts_simple() {
    let inputs = (0..100).map(|_| (0..600).map(|i| (i, i)).collect()).collect::<Vec<_>>();
    run_concurrent_insertions(inputs).unwrap();
}

#[proptest]
fn test_concurrent_inserts_random(inputs: ConcurrentTestInputs<u8, u8>) {
    run_concurrent_insertions(inputs).unwrap();
}

type ConcurrentTestInputs<K, V> = Vec<Box<[(K, V)]>>;

fn run_concurrent_insertions<K, V>(inputs: ConcurrentTestInputs<K, V>) -> Result<()>
where
    K: Min + Archive + Serialize<DefaultSerializer> + fmt::Debug + Send + Sync + 'static,
    K::Archived: Deserialize<K, rkyv::Infallible> + PartialOrd<K> + fmt::Debug + Ord + Send + Sync,
    V: Archive + Eq + Serialize<DefaultSerializer> + fmt::Debug + Send + Sync + 'static,
    V::Archived: Deserialize<V, rkyv::Infallible> + fmt::Debug + Send + Sync,
{
    nsql_test::start(async {
        let pool = mk_fast_mem_buffer_pool!();
        let btree = BTree::<K, V>::initialize(pool).await?;
        let mut set = JoinSet::<Result<()>>::new();
        for input in inputs {
            let btree = BTree::clone(&btree);
            set.spawn(async move {
                for (key, value) in &input[..] {
                    btree.insert(key, value).await?;
                    assert_eq!(&btree.get(key).await?.unwrap(), value);
                }
                Ok(())
            });
        }

        while let Some(res) = set.join_next().await {
            res.unwrap()?;
        }

        Ok(())
    })
}
