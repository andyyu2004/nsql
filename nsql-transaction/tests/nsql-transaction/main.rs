use nsql_transaction::{Error, TransactionManager};

#[test]
fn test_concurrent_transaction_management() -> Result<(), Error> {
    nsql_test::start(async {
        let txm = TransactionManager::initialize();
        let mut set = tokio::task::JoinSet::new();

        for _ in 0..10 {
            let txm = txm.clone();
            set.spawn({
                async move {
                    for _ in 0..100 {
                        let tx = txm.begin();
                        tx.commit();
                    }
                }
            });
        }

        let tx = txm.begin();
        while let Some(res) = set.join_next().await {
            res.unwrap();
        }
        tx.commit();

        Ok(())
    })
}
