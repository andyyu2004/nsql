use anyhow::Result;
use nsql_storage::eval::{Expr, FunctionCatalog, TupleExpr};
use nsql_storage_engine::{ExecutionMode, StorageEngine, Transaction};

pub trait TupleExprResolveExt {
    /// Prepare this tuple expression for evaluation.
    // This resolves any function oids and replaces them with the actual function.
    fn resolve<'env, S: StorageEngine, M: ExecutionMode<'env, S>, F>(
        self,
        catalog: &dyn FunctionCatalog<'env, S, M, F>,
        tx: &dyn Transaction<'env, S>,
    ) -> Result<TupleExpr<F>>;
}

impl TupleExprResolveExt for TupleExpr {
    #[inline]
    fn resolve<'env, S: StorageEngine, M: ExecutionMode<'env, S>, F>(
        self,
        catalog: &dyn FunctionCatalog<'env, S, M, F>,
        tx: &dyn Transaction<'env, S>,
    ) -> Result<TupleExpr<F>> {
        self.map(|oid| catalog.get_function(tx, oid))
    }
}

pub trait ExprResolveExt {
    /// Prepare this expression for evaluation.
    // This resolves any function oids and replaces them with what the catalog returns
    fn resolve<'env, S: StorageEngine, M: ExecutionMode<'env, S>, F>(
        self,
        catalog: &dyn FunctionCatalog<'env, S, M, F>,
        tx: &dyn Transaction<'env, S>,
    ) -> Result<Expr<F>>;
}

impl ExprResolveExt for Expr {
    #[inline]
    fn resolve<'env, S: StorageEngine, M: ExecutionMode<'env, S>, F>(
        self,
        catalog: &dyn FunctionCatalog<'env, S, M, F>,
        tx: &dyn Transaction<'env, S>,
    ) -> Result<Expr<F>> {
        self.map(|oid| catalog.get_function(tx, oid))
    }
}
