use crate::*;

pub(crate) struct SelectBinder<'a, 'env, S> {
    binder: &'a Binder<'env, S>,
    aggregates: Vec<(Function, Box<[ir::Expr]>)>,
}

impl<'a, 'env, S: StorageEngine> SelectBinder<'a, 'env, S> {
    pub fn new(binder: &'a Binder<'env, S>) -> Self {
        Self { binder, aggregates: Default::default() }
    }

    pub fn bind(
        mut self,
        tx: &dyn Transaction<'env, S>,
        scope: &Scope<S>,
        items: &[ast::SelectItem],
    ) -> Result<(Box<[(Function, Box<[ir::Expr]>)]>, Box<[ir::Expr]>)> {
        let items = items
            .iter()
            .map(|item| self.bind_select_item(tx, scope, item))
            .flatten_ok()
            .collect::<Result<Box<_>>>()?;
        Ok((self.aggregates.into_boxed_slice(), items))
    }

    fn bind_select_item(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        scope: &Scope<S>,
        item: &ast::SelectItem,
    ) -> Result<Vec<ir::Expr>> {
        let expr = match item {
            ast::SelectItem::UnnamedExpr(expr) => self.bind_select_expr(tx, scope, expr)?,
            ast::SelectItem::ExprWithAlias { expr, alias } => {
                self.bind_select_expr(tx, scope, expr)?.alias(&alias.value)
            }
            _ => return self.binder.bind_select_item(tx, scope, item),
        };

        Ok(vec![expr])
    }

    fn bind_select_expr(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        scope: &Scope<S>,
        expr: &ast::Expr,
    ) -> Result<ir::Expr> {
        match expr {
            ast::Expr::Function(f) => {
                let (function, args) = self.binder.bind_function(tx, scope, f)?;
                let ty = function.return_type();
                let kind = match function.kind() {
                    FunctionKind::Function => ir::ExprKind::FunctionCall { function, args },
                    FunctionKind::Aggregate => {
                        let idx = self.aggregates.len();
                        self.aggregates.push((function, args));
                        // FIXME need to add column to scope
                        // create a column reference to the aggregate column that will be added
                        ir::ExprKind::ColumnRef {
                            // FIXME check for alias
                            path: Path::unqualified(expr.to_string()),
                            index: TupleIndex::new(idx),
                        }
                    }
                };

                Ok(ir::Expr { ty, kind })
            }
            _ => self.binder.bind_expr(tx, scope, expr),
        }
    }
}
