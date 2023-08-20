use super::*;

impl Namespace {
    pub(crate) const CATALOG: Oid<Self> = Oid::new(0);
    pub const MAIN: Oid<Self> = Oid::new(1);
}

pub(super) struct BootstrapNamespace {
    pub oid: Oid<Namespace>,
    pub name: &'static str,
}

pub(super) fn bootstrap_data() -> Box<[BootstrapNamespace]> {
    vec![
        BootstrapNamespace { oid: Namespace::CATALOG, name: "nsql_catalog" },
        BootstrapNamespace { oid: Namespace::MAIN, name: MAIN_SCHEMA },
    ]
    .into()
}
