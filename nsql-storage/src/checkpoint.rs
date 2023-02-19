use nsql_catalog::Catalog;
use nsql_pager::{MetaPageWriter, Pager};
use nsql_transaction::Transaction;

use crate::Result;

pub struct Checkpointer<'a, P> {
    pager: &'a P,
}

impl<'a, P> Checkpointer<'a, P> {
    pub fn new(pager: &'a P) -> Self {
        Self { pager }
    }
}

impl<P: Pager> Checkpointer<'_, P> {
    pub async fn checkpoint(&self, tx: &Transaction, catalog: &Catalog) -> Result<()> {
        let page = self.pager.alloc_page().await?;
        let writer = MetaPageWriter::new(self.pager, page);
        Ok(())
    }
}
