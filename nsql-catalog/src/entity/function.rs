use std::{fmt, mem};

use nsql_core::UntypedOid;
use nsql_storage::expr::Expr;
use nsql_storage_engine::ExecutionMode;

use super::*;
use crate::expr::{ExecutableFunction, FunctionArgs};
use crate::{ColumnIdentity, FunctionCatalog, SystemEntityPrivate, TransactionContext};

mod builtins;

pub type ScalarFunctionPtr<'env, 'txn, S, M> = fn(
    Catalog<'env, S>,
    &dyn TransactionContext<'env, 'txn, S, M>,
    FunctionArgs<'_>,
) -> Result<Value>;

pub trait AggregateFunctionInstance: fmt::Debug {
    fn update(&mut self, value: Option<Value>);
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
    fn from_value(value: Value) -> Result<Self, CastError> {
        let b = value.cast::<u8>()?;
        assert!(b <= FunctionKind::Aggregate as u8);
        Ok(unsafe { mem::transmute(b) })
    }
}

impl From<FunctionKind> for Value {
    #[inline]
    fn from(value: FunctionKind) -> Self {
        Value::Byte(value as u8)
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    FunctionCatalog<'env, 'txn, S, M> for Catalog<'env, S>
{
    #[inline]
    fn storage(&self) -> &'env S {
        self.storage
    }

    #[inline]
    fn get_function(
        &self,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        oid: Oid<Function>,
    ) -> Result<ExecutableFunction<'env, 'txn, S, M>> {
        let f = self.get::<M, Function>(tx, oid)?;
        Ok(f.get_scalar_function())
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
    pub fn args(&self) -> &[LogicalType] {
        &self.args
    }

    #[inline]
    pub fn return_type(&self) -> LogicalType {
        self.ret.clone()
    }

    #[inline]
    pub fn get_scalar_function<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        &self,
    ) -> ScalarFunctionPtr<'env, 'txn, S, M> {
        assert!(matches!(self.kind, FunctionKind::Scalar));
        if let Some(f) = builtins::get_scalar_function::<S, M>(self.oid) {
            return f;
        }

        // otherwise, we store the bytecode for non-builtin functions
        // let bytecode: Expr = todo!();

        panic!("missing builtin scalar function definition for oid {}", self.oid)
    }

    #[inline]
    pub fn get_aggregate_instance(&self) -> Box<dyn AggregateFunctionInstance> {
        assert!(matches!(self.kind, FunctionKind::Aggregate));
        if let Some(f) = builtins::get_aggregate_function(self.oid) {
            return f;
        }

        panic!("missing builtin aggregate function definition for oid {}", self.oid)
    }

    #[inline]
    pub fn oid(&self) -> Oid<Function> {
        self.oid
    }
}

impl SystemEntity for Function {
    type Parent = Namespace;

    type Key = Oid<Self>;

    type SearchKey = Name;

    #[inline]
    fn key(&self) -> Self::Key {
        self.oid
    }

    #[inline]
    fn search_key(&self) -> Self::SearchKey {
        self.name()
    }

    #[inline]
    fn name<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<Name> {
        Ok(self.name())
    }

    #[inline]
    fn desc() -> &'static str {
        "function"
    }

    #[inline]
    fn parent_oid<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<Option<Oid<Self::Parent>>> {
        Ok(Some(self.namespace))
    }
}

impl SystemEntityPrivate for Function {
    const TABLE: Oid<Table> = Table::FUNCTION;

    fn bootstrap_column_info() -> Vec<BootstrapColumn> {
        vec![
            BootstrapColumn {
                ty: LogicalType::Oid,
                name: "oid",
                is_primary_key: true,
                identity: ColumnIdentity::None,
                default_expr: Expr::null(),
                seq: None,
            },
            BootstrapColumn {
                ty: LogicalType::Byte,
                name: "kind",
                is_primary_key: false,
                identity: ColumnIdentity::None,
                default_expr: Expr::null(),
                seq: None,
            },
            BootstrapColumn {
                ty: LogicalType::Oid,
                name: "namespace",
                is_primary_key: true,
                identity: ColumnIdentity::None,
                default_expr: Expr::null(),
                seq: None,
            },
            BootstrapColumn {
                ty: LogicalType::Text,
                name: "name",
                is_primary_key: false,
                identity: ColumnIdentity::None,
                default_expr: Expr::null(),
                seq: None,
            },
            BootstrapColumn {
                ty: LogicalType::array(LogicalType::Type),
                name: "args",
                is_primary_key: true,
                identity: ColumnIdentity::None,
                default_expr: Expr::null(),
                seq: None,
            },
            BootstrapColumn {
                ty: LogicalType::Type,
                name: "ret",
                is_primary_key: false,
                identity: ColumnIdentity::None,
                default_expr: Expr::null(),
                seq: None,
            },
        ]
    }
}
