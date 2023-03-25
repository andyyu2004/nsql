use criterion::async_executor::AsyncExecutor;
use criterion::{criterion_group, criterion_main, Criterion};
use nsql_btree::{BTree, Result};
use tokio::task::JoinSet;

criterion_group!(benches, bench_insertions);
criterion_main!(benches);

// struct Runtime(tokio_uring::Runtime);

// impl Default for Runtime {
//     fn default() -> Self {
//         Self(tokio_uring::Runtime::new(&tokio_uring::builder()).unwrap())
//     }
// }

// impl AsyncExecutor for Runtime {
//     fn block_on<T>(&self, future: impl Future<Output = T>) -> T {
//         self.0.block_on(future)
//     }
// }

fn new_runtime() -> impl AsyncExecutor {
    tokio::runtime::Runtime::new().unwrap()
}

fn bench_insertions(c: &mut Criterion) {
    c.bench_function("insertions", |b| b.to_async(new_runtime()).iter(insertions));
    c.bench_function("concurrent insertions", |b| {
        b.to_async(new_runtime()).iter(concurrent_insertions)
    });
}

async fn insertions() {
    let pool = nsql_test::mk_fast_mem_buffer_pool!();
    let btree = BTree::<u32, u64>::initialize(pool).await.unwrap();
    for i in 0..60000 {
        btree.insert(&i, &(i as u64)).await.unwrap();
    }
}

async fn concurrent_insertions() {
    let pool = nsql_test::mk_fast_mem_buffer_pool!();
    let btree = BTree::<u32, u64>::initialize(pool).await.unwrap();
    let mut set = JoinSet::<Result<()>>::new();
    for _ in 0..100 {
        let btree = BTree::clone(&btree);
        set.spawn(async move {
            for i in 0..600 {
                btree.insert(&i, &(i as u64)).await?;
            }
            Ok(())
        });
    }

    while let Some(res) = set.join_next().await {
        res.unwrap().unwrap();
    }
}
