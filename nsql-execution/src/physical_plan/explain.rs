use std::fmt;

use nsql_catalog::Catalog;
use nsql_transaction::Transaction;

use crate::{PhysicalNode, RootPipeline};

pub type Result<T = ()> = std::result::Result<T, fmt::Error>;

pub trait Explain {
    fn explain(&self, catalog: &Catalog, tx: &Transaction, f: &mut fmt::Formatter<'_>) -> Result;
}

pub(crate) fn explain_pipeline<'a>(
    catalog: &'a Catalog,
    tx: &'a Transaction,
    root: &'a RootPipeline,
) -> Explainer<'a> {
    todo!()
}

pub(crate) fn explain<'a>(
    catalog: &'a Catalog,
    tx: &'a Transaction,
    node: &'a dyn PhysicalNode,
) -> Explainer<'a> {
    Explainer { catalog, tx, node, indent: 0 }
}

pub struct Explainer<'a> {
    catalog: &'a Catalog,
    tx: &'a Transaction,
    node: &'a dyn PhysicalNode,
    indent: usize,
}

impl<'a> Explainer<'a> {
    fn explain_child(&self, node: &'a dyn PhysicalNode) -> Explainer<'_> {
        Explainer { catalog: self.catalog, tx: self.tx, node, indent: self.indent + 2 }
    }
}

impl fmt::Display for Explainer<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:indent$}", "", indent = self.indent)?;
        self.node.explain(self.catalog, self.tx, f)?;
        writeln!(f)?;

        for child in self.node.children() {
            self.explain_child(child.as_ref()).fmt(f)?;
        }

        Ok(())
    }
}
