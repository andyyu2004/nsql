use std::sync::Arc;

use nsql_pager::Pager;
use nsql_transaction::Transaction;

use crate::tuple::Tuple;

pub struct TableStorage<P: Pager> {
    pager: Arc<P>,
}

impl<P: Pager + Send + Sync> nsql_catalog::TableStorage for TableStorage<P> {}

impl<P: Pager> TableStorage<P> {
    pub fn new(pager: Arc<P>) -> Self {
        Self { pager }
    }

    pub async fn scan(tx: &Transaction) -> Vec<Tuple> {
        todo!()
    }
}
