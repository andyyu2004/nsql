use core::fmt;
use std::ops::ControlFlow;

use anyhow::anyhow;

use crate::visit::Visitor;
use crate::{ColumnRef, Expr, ExprKind, QueryPlan};

#[derive(Default)]
struct Validator {
    errors: Vec<anyhow::Error>,
}

#[derive(Debug)]
pub struct ValidationError(Box<[anyhow::Error]>);

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for error in self.0.iter() {
            writeln!(f, "{error}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ValidationError {}

impl Visitor for Validator {
    fn visit_expr(&mut self, plan: &QueryPlan, expr: &Expr) -> ControlFlow<()> {
        match &expr.kind {
            ExprKind::ColumnRef(ColumnRef { qpath, index, level: 0 }) => {
                if index.as_usize() >= plan.schema().len() {
                    self.errors.push(anyhow!(
                        "column reference out of bounds: {index} >= {} (path: {qpath}) (plan:\n{plan:#})",
                        plan.schema().len(),
                    ));
                }
                ControlFlow::Continue(())
            }
            _ => self.walk_expr(plan, expr),
        }
    }
}

impl QueryPlan {
    pub fn validate(&self) -> Result<(), ValidationError> {
        let mut validator = Validator::default();
        validator.visit_query_plan(self);
        if validator.errors.is_empty() {
            Ok(())
        } else {
            Err(ValidationError(validator.errors.into_boxed_slice()))
        }
    }
}
