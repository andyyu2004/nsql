use nsql_transaction::Transaction;

pub struct Catalog {}

impl Catalog {
    pub async fn create_schema(
        &self,
        tx: &Transaction,
        schema_name: &str,
    ) -> Result<(), std::io::Error> {
        Ok(())
    }
}
