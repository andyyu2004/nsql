use crate::{CatalogEntity, EntryName};

#[derive(Clone)]
pub struct Schema {
    pub name: EntryName,
    pub is_internal: bool,
}

impl Schema {
    pub(crate) fn new(name: EntryName, is_internal: bool) -> Self {
        Self { name, is_internal }
    }
}

impl CatalogEntity for Schema {
    fn name(&self) -> &EntryName {
        &self.name
    }
}
