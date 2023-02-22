use nsql_serde::Deserialize;

#[derive(Debug, Clone)]
pub enum Ty {
    Int,
}

impl Deserialize for Ty {
    type Error = std::io::Error;

    async fn deserialize(_de: &mut dyn nsql_serde::Deserializer<'_>) -> Result<Self, Self::Error> {
        todo!()
    }
}
