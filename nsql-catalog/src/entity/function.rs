use std::{fmt, mem};

use nsql_core::UntypedOid;

use super::*;
use crate::Namespace;

mod builtins;

pub type ScalarFunction = fn(Box<[Value]>) -> Value;

pub trait AggregateFunctionInstance: fmt::Debug {
    fn update(&mut self, value: Value);
    fn finalize(self: Box<Self>) -> Value;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, FromTuple, IntoTuple)]
pub struct Function {
    pub(crate) oid: Oid<Self>,
    pub(crate) kind: FunctionKind,
    pub(crate) namespace: Oid<Namespace>,
    pub(crate) name: Name,
    pub(crate) args: Box<[LogicalType]>,
    pub(crate) ret: LogicalType,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum FunctionKind {
    Scalar,
    Aggregate, // note: make sure the assertion below is changed if variants are reordered
}

impl FromValue for FunctionKind {
    fn from_value(value: Value) -> Result<Self, CastError<Self>> {
        match value {
            Value::Byte(b) => {
                assert!(b <= FunctionKind::Aggregate as u8);
                Ok(unsafe { mem::transmute(b) })
            }
            _ => Err(CastError::new(value)),
        }
    }
}

impl From<FunctionKind> for Value {
    #[inline]
    fn from(value: FunctionKind) -> Self {
        Value::Byte(value as u8)
    }
}

impl<'env, S: StorageEngine> nsql_storage::eval::FunctionCatalog<'env, S> for Catalog<'env, S> {
    #[inline]
    fn get_function(
        &self,
        tx: &dyn Transaction<'env, S>,
        oid: UntypedOid,
    ) -> Result<Box<dyn nsql_storage::eval::ScalarFunction>> {
        let f = self.get::<Function>(tx, oid.cast())?;
        Ok(Box::new(f))
    }
}

impl nsql_storage::eval::ScalarFunction for Function {
    #[inline]
    fn call(&self, args: Box<[Value]>) -> Value {
        self.get_scalar_function()(args)
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
    pub fn kind(&self) -> FunctionKind {
        self.kind
    }

    #[inline]
    pub fn return_type(&self) -> LogicalType {
        self.ret.clone()
    }

    #[inline]
    pub fn get_scalar_function(&self) -> ScalarFunction {
        if let Some(f) = builtins::get_scalar_function(self.oid) {
            return f;
        }

        // otherwise, we store the bytecode for non-builtin functions
        // let bytecode: Expr = todo!();

        panic!("missing scalar function definition")
    }

    #[inline]
    pub fn get_aggregate_instance(&self) -> Box<dyn AggregateFunctionInstance> {
        if let Some(f) = builtins::get_aggregate_function(self.oid) {
            return f;
        }

        panic!("missing aggregate function")
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
                ColumnStorageInfo::new("oid", LogicalType::Oid, true),
                ColumnStorageInfo::new("kind", LogicalType::Byte, false),
                ColumnStorageInfo::new("namespace", LogicalType::Oid, false),
                ColumnStorageInfo::new("name", LogicalType::Text, false),
                ColumnStorageInfo::new("args", LogicalType::array(LogicalType::Type), true),
                ColumnStorageInfo::new("ret", LogicalType::Type, false),
            ],
        )
    }

    #[inline]
    fn table() -> Oid<Table> {
        Table::FUNCTION
    }
}
