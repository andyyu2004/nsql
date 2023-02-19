pub struct Catalog {}

impl Catalog {
    pub async fn create_schema(&self, name: &str) -> Result<(), std::io::Error> {
        Ok(())
    }
}
