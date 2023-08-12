use core::fmt;
use std::ops::ControlFlow;

use anyhow::anyhow;

use crate::visit::Visitor;
use crate::{Expr, ExprKind, Plan};

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
    fn visit_expr(&mut self, plan: &Plan, expr: &Expr) -> ControlFlow<()> {
        match &expr.kind {
            ExprKind::ColumnRef { qpath, index } => {
                if index.as_usize() >= plan.schema().len() {
                    self.errors.push(anyhow!(
                        "column reference out of bounds: {index} >= {} (path: {})",
                        plan.schema().len(),
                        qpath,
                    ));
                }
                ControlFlow::Continue(())
            }
            _ => self.walk_expr(plan, expr),
        }
    }
}

impl Plan {
    pub fn validate(&self) -> Result<(), ValidationError> {
        let mut validator = Validator::default();
        validator.visit_plan(self);
        if validator.errors.is_empty() {
            Ok(())
        } else {
            Err(ValidationError(validator.errors.into_boxed_slice()))
        }
    }
}
