// can't use `cov_mark` in this file as it uses thread locals but we're spinning up a bunch of
// tasks which get scheduled on alternative threads
// Instead, we do assertions based on log outputs to achieve a similar effect
use std::{fmt, io};

use nsql_rkyv::DefaultSerializer;
use nsql_test::mk_fast_mem_buffer_pool;
use rkyv::{Archive, Deserialize, Serialize};
use test_strategy::proptest;
use tokio::task::JoinSet;

use crate::{BTree, Min, Result};

#[tokio::test]
#[tracing_test::traced_test]
async fn test_concurrent_root_leaf_split() -> Result<()> {
    let inputs = (0..2).map(|_| (0..500).map(|i| (i, i)).collect()).collect::<Vec<_>>();
    run_concurrent_insertions(inputs).await?;
    assert!(logs_contain("splitting root kind=nsql_btree::page::leaf::LeafPageViewMut<i32, i32>"));
    assert!(!logs_contain("splitting non-root"));
    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn test_concurrent_non_root_leaf_split() -> Result<()> {
    let inputs = (0..2).map(|_| (0..750).map(|i| (i, i)).collect()).collect::<Vec<_>>();
    run_concurrent_insertions(inputs).await?;
    assert!(logs_contain("splitting root kind=nsql_btree::page::leaf::LeafPageViewMut<i32, i32>"));
    assert!(logs_contain(
        "splitting non-root kind=nsql_btree::page::leaf::LeafPageViewMut<i32, i32>"
    ));
    Ok(())
}

// FIXME fix other tests first
// #[proptest]
fn test_concurrent_inserts_random(inputs: ConcurrentTestInputs<u8, u8>) {
    nsql_test::start(async {
        run_concurrent_insertions(inputs).await.unwrap();
    })
}

type ConcurrentTestInputs<K, V> = Vec<Box<[(K, V)]>>;

async fn run_concurrent_insertions<K, V>(inputs: ConcurrentTestInputs<K, V>) -> Result<()>
where
    K: Min + Archive + Serialize<DefaultSerializer> + fmt::Debug + Send + Sync + 'static,
    K::Archived: Deserialize<K, rkyv::Infallible> + PartialOrd<K> + fmt::Debug + Ord + Send + Sync,
    V: Archive + Eq + Serialize<DefaultSerializer> + fmt::Debug + Send + Sync + 'static,
    V::Archived: Deserialize<V, rkyv::Infallible> + fmt::Debug + Send + Sync,
{
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
        match res {
            Ok(res) => res?,
            Err(err) => Err(io::Error::new(io::ErrorKind::Other, err))?,
        }
    }

    Ok(())
}
