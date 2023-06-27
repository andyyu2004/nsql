use nsql_core::UntypedOid;

use super::*;
use crate::Namespace;

mod builtins;

#[derive(Debug, Clone, PartialEq, Eq, Hash, FromTuple, IntoTuple)]
pub struct Function {
    pub(crate) oid: Oid<Self>,
    pub(crate) namespace: Oid<Namespace>,
    pub(crate) name: Name,
    pub(crate) args: Box<[LogicalType]>,
    pub(crate) ret: LogicalType,
}

impl<'env, S: StorageEngine> nsql_storage::eval::FunctionCatalog<'env, S> for Catalog<'env, S> {
    #[inline]
    fn get_function(
        &self,
        tx: &dyn Transaction<'env, S>,
        oid: UntypedOid,
    ) -> Result<Box<dyn nsql_storage::eval::Function>> {
        let f = self.get::<Function>(tx, oid.cast())?;
        Ok(Box::new(f))
    }
}

impl nsql_storage::eval::Function for Function {
    #[inline]
    fn call(&self, args: Box<[Value]>) -> Value {
        self.call(args)
    }

    #[inline]
    fn arity(&self) -> usize {
        self.args.len()
    }
}

impl Function {
    #[inline]
    pub fn name(&self) -> Name {
        Name::clone(&self.name)
    }

    #[inline]
    pub fn return_type(&self) -> LogicalType {
        self.ret.clone()
    }

    pub fn call(&self, args: Box<[Value]>) -> Value {
        if let Some(f) = builtins::get_builtin(self.oid) {
            return f(args);
        }

        // otherwise, we store the bytecode for non-builtin functions
        // let bytecode: Expr = todo!();

        panic!()
    }
}

impl SystemEntity for Function {
    type Parent = Namespace;

    type Key = Oid<Self>;

    type SearchKey = (Name, Box<[LogicalType]>);

    #[inline]
    fn key(&self) -> Self::Key {
        self.oid
    }

    #[inline]
    fn search_key(&self) -> Self::SearchKey {
        (self.name(), self.args.clone())
    }

    #[inline]
    fn name<'env, S: StorageEngine>(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn Transaction<'env, S>,
    ) -> Result<Name> {
        Ok(self.name())
    }

    #[inline]
    fn desc() -> &'static str {
        "function"
    }

    #[inline]
    fn parent_oid<'env, S: StorageEngine>(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn Transaction<'env, S>,
    ) -> Result<Option<Oid<Self::Parent>>> {
        Ok(Some(self.namespace))
    }

    fn bootstrap_table_storage_info() -> TableStorageInfo {
        TableStorageInfo::new(
            Table::FUNCTION.untyped(),
            vec![
                ColumnStorageInfo::new(LogicalType::Oid, true),
                ColumnStorageInfo::new(LogicalType::Oid, false),
                ColumnStorageInfo::new(LogicalType::Text, false),
                ColumnStorageInfo::new(LogicalType::array(LogicalType::Type), false),
                ColumnStorageInfo::new(LogicalType::Type, false),
            ],
        )
    }

    #[inline]
    fn table() -> Oid<Table> {
        Table::FUNCTION
    }
}
