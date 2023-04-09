use std::collections::HashMap;
// can't use `cov_mark` in this file as it uses thread locals but we're spinning up a bunch of
// tasks which get scheduled on alternative threads
// Instead, we do assertions based on log outputs to achieve a similar effect
use std::hash::Hash;
use std::sync::Arc;
use std::{fmt, io};

use dashmap::DashMap;
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
    assert!(logs_contain("detected concurrent leaf split"));
    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn test_concurrent_non_root_leaf_split_reverse() -> Result<()> {
    let inputs = (0..2).map(|_| (0..750).rev().map(|i| (i, i)).collect()).collect::<Vec<_>>();
    run_concurrent_insertions(inputs).await?;
    assert!(logs_contain("splitting root kind=nsql_btree::page::leaf::LeafPageViewMut<i32, i32>"));
    assert!(logs_contain(
        "splitting non-root kind=nsql_btree::page::leaf::LeafPageViewMut<i32, i32>"
    ));
    assert!(logs_contain("detected concurrent leaf split"));
    Ok(())
}

// #[tokio::test]
// #[tracing_test::traced_test]
// async fn test_concurrent_root_interior_split() -> Result<()> {
//     let inputs = (0..2).map(|_| (0..40000).map(|i| (i, i)).collect()).collect::<Vec<_>>();
//     run_concurrent_insertions(inputs).await?;
//     // assert!(logs_contain(
//     //     "splitting root kind=nsql_btree::page::leaf::InteriorPageViewMut<i32, i32>"
//     // ));
//     Ok(())
// }

// FIXME fix other tests first
// this is way too slow
#[proptest]
fn test_concurrent_inserts_random(inputs: ConcurrentTestInputs<u8, u8>) {
    nsql_test::start(async {
        run_concurrent_insertions(inputs).await.unwrap();
    })
}

type ConcurrentTestInputs<K, V> = Vec<Box<[(K, V)]>>;

// To make this test pass reliably we need to ensure that the same `K` always maps to the same `V`.
async fn run_concurrent_insertions<K, V>(inputs: ConcurrentTestInputs<K, V>) -> Result<()>
where
    K: Min
        + Archive
        + Serialize<DefaultSerializer>
        + fmt::Debug
        + Clone
        + Hash
        + Eq
        + Send
        + Sync
        + 'static,
    K::Archived:
        Deserialize<K, rkyv::Infallible> + PartialOrd<K> + fmt::Debug + Clone + Ord + Send + Sync,
    V: Archive + Eq + Clone + Serialize<DefaultSerializer> + fmt::Debug + Send + Sync + 'static,
    V::Archived: Deserialize<V, rkyv::Infallible> + fmt::Debug + Send + Sync,
{
    let pool = mk_fast_mem_buffer_pool!();
    let btree = BTree::<K, V>::initialize(pool).await?;
    let mut join_set = JoinSet::<Result<()>>::new();

    // mapping from key to the latest two values per key per thread
    // the last two values are both valid because we can't be sure which stage of the update we are at
    let values_map = Arc::<DashMap<K, HashMap<usize, Vec<V>>>>::default();

    for (thread_id, input) in inputs.into_iter().enumerate() {
        let btree = BTree::clone(&btree);
        let values_map = Arc::clone(&values_map);
        join_set.spawn(async move {
            for (key, value) in &input[..] {
                let mut valid_values_ref = values_map.entry(key.clone()).or_default();
                let thread_values = valid_values_ref.entry(thread_id).or_default();
                thread_values.insert(0, value.clone());
                thread_values.truncate(2);
                drop(valid_values_ref);

                let _prev = btree.insert(key, value).await?;

                let v = btree.get(key).await?.unwrap();

                let valid_values_ref = values_map.get(key).unwrap();
                let valid_values = valid_values_ref.iter().map(|(tid, vs)| (*tid, vs.clone())).collect::<Vec<(usize, Vec<V>)>>();
                assert!(
                    valid_values.iter().any(|(_k, xs)| xs.iter().any(|x| x == &v)) ,
                    "the value retrieved from the btree was not any one of the expected values (thread_id={:?}, key={:?}, value={:?}, expected_values={:?})",
                    thread_id,
                    key,
                    v,
                    valid_values,
                );
            }
            Ok(())
        });
    }

    while let Some(res) = join_set.join_next().await {
        match res {
            Ok(res) => res?,
            Err(err) => Err(io::Error::new(io::ErrorKind::Other, err))?,
        }
    }

    Ok(())
}
