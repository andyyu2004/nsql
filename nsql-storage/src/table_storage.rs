use std::sync::Arc;

use nsql_pager::Pager;
use nsql_transaction::Transaction;

use crate::tuple::Tuple;

pub struct TableStorage {
    pager: Arc<dyn Pager>,
}

impl TableStorage {
    pub fn new(pager: Arc<dyn Pager>) -> Self {
        Self { pager }
    }

    pub async fn append(&self, tx: &Transaction, tuple: Tuple) -> Result<(), ()> {
        todo!()
    }

    pub async fn scan(tx: &Transaction) -> Vec<Tuple> {
        todo!()
    }
}
