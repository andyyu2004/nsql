use crate::*;

pub trait ExprFolder {
    /// Replace an expression, this must be type preserving
    fn fold_expr(&mut self, expr: &Expr) -> Expr;

    #[inline]
    fn fold_expr_boxed(&mut self, expr: &Expr) -> Box<Expr> {
        Box::new(self.fold_expr(expr))
    }
}

pub trait ExprFold: Sized {
    fn fold_with(&self, folder: &mut impl ExprFolder) -> Self;

    fn super_fold_with(&self, folder: &mut impl ExprFolder) -> Self {
        self.fold_with(folder)
    }
}

impl ExprFold for OrderExpr {
    #[inline]
    fn fold_with(&self, folder: &mut impl ExprFolder) -> Self {
        Self { expr: folder.fold_expr(&self.expr), asc: self.asc }
    }
}

impl ExprFold for Expr {
    #[inline]
    fn super_fold_with(&self, folder: &mut impl ExprFolder) -> Self {
        folder.fold_expr(self)
    }

    fn fold_with(&self, folder: &mut impl ExprFolder) -> Self {
        let kind = match &self.kind {
            ExprKind::Literal(lit) => ExprKind::Literal(lit.clone()),
            ExprKind::Array(exprs) => {
                ExprKind::Array(exprs.iter().map(|expr| folder.fold_expr(expr)).collect())
            }
            ExprKind::Alias { alias, expr } => {
                ExprKind::Alias { alias: alias.clone(), expr: folder.fold_expr_boxed(expr) }
            }
            ExprKind::UnaryOp { op, expr } => {
                ExprKind::UnaryOp { op: *op, expr: folder.fold_expr_boxed(expr) }
            }
            ExprKind::BinOp { op, lhs, rhs } => ExprKind::BinOp {
                op: *op,
                lhs: folder.fold_expr_boxed(lhs),
                rhs: folder.fold_expr_boxed(rhs),
            },
            ExprKind::ColumnRef { qpath, index } => {
                ExprKind::ColumnRef { qpath: qpath.clone(), index: *index }
            }
            ExprKind::FunctionCall { function, args } => ExprKind::FunctionCall {
                function: function.clone(),
                args: args.iter().map(|expr| folder.fold_expr(expr)).collect(),
            },
            ExprKind::Case { scrutinee, cases, else_result } => ExprKind::Case {
                scrutinee: folder.fold_expr_boxed(scrutinee),
                cases: cases
                    .iter()
                    .map(|case| Case {
                        when: folder.fold_expr(&case.when),
                        then: folder.fold_expr(&case.then),
                    })
                    .collect(),
                else_result: else_result.as_ref().map(|expr| folder.fold_expr_boxed(expr)),
            },
        };

        Expr { ty: self.ty.clone(), kind }
    }
}
