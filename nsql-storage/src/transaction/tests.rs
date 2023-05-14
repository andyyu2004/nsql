use super::{TransactionError, TransactionManager};

#[test]
fn test_concurrent_transaction_management() -> Result<(), TransactionError> {
    nsql_test::start(async {
        let txm = TransactionManager::initialize();
        let mut set = tokio::task::JoinSet::new();

        for _ in 0..10 {
            let txm = txm.clone();
            set.spawn({
                async move {
                    for _ in 0..100 {
                        let tx = txm.begin();
                        tx.commit().await.unwrap();
                    }
                }
            });
        }

        let tx = txm.begin();
        while let Some(res) = set.join_next().await {
            res.unwrap();
        }
        tx.commit().await.unwrap();

        Ok(())
    })
}
