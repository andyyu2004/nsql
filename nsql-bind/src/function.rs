use nsql_catalog::{Function, Operator};
use nsql_core::ty::{TypeFold, TypeFolder, Zip, ZipError, ZipResult, Zipper};
use nsql_core::LogicalType;
use nsql_storage_engine::{FallibleIterator, StorageEngine, Transaction};

use crate::{Binder, Error, Result};

impl<'env, S: StorageEngine> Binder<'env, S> {
    pub(crate) fn resolve_candidate_operators(
        &self,
        tx: &dyn Transaction<'env, S>,
        mut candidates: impl FallibleIterator<Item = Operator, Error = Error>,
        arg_types: &[LogicalType],
    ) -> Result<Option<ir::MonoOperator>> {
        let functions_table = self.catalog.functions(tx)?;
        candidates.find_map(|op| {
            let f = functions_table.get(op.function())?;
            Ok(try {
                let (_args, ret) =
                    Unifier::default().try_unify_function(arg_types, f.args(), f.return_type())?;
                ir::MonoOperator::new(op, ir::MonoFunction::new(f, ret))
            })
        })
    }

    pub(crate) fn resolve_candidate_functions(
        &self,
        mut candidates: impl FallibleIterator<Item = Function, Error = Error>,
        arg_types: &[LogicalType],
    ) -> Result<Option<ir::MonoFunction>> {
        candidates.find_map(|f| {
            Ok(try {
                let (_args, ret) =
                    Unifier::default().try_unify_function(arg_types, f.args(), f.return_type())?;
                ir::MonoFunction::new(f, ret)
            })
        })
    }
}

// we treat the ANY type like a very limited form of an implicit type parameter T.
// All ANY's must instantiate to the same type.
#[derive(Debug, Default)]
struct Unifier {
    /// the mapping for the `ANY` type if present
    subst: Option<LogicalType>,
}

impl Unifier {
    fn try_unify_function(
        mut self,
        args: &[LogicalType],
        targets: &[LogicalType],
        return_type: LogicalType,
    ) -> Option<(Box<[LogicalType]>, LogicalType)> {
        // we try to unify the function type with the provided argument types
        if Zip::zip_with(&mut self, args, targets).is_err() {
            // if there is no solution we return `None`
            return None;
        }

        // there was a solution, substitute out the ANY with the substitution we found
        // unwraps will never fire here, as the folder impl is infallible
        let args = args.to_vec().fold_with(&mut self).unwrap().into_boxed_slice();
        let ret = return_type.super_fold_with(&mut self).unwrap();
        Some((args, ret))
    }
}

impl Zipper for Unifier {
    fn zip_tys(&mut self, arg: &LogicalType, target: &LogicalType) -> ZipResult<()> {
        match (arg, target) {
            // a null argument can be passed anywhere
            (LogicalType::Null, _) => Ok(()),
            // the ANY type can match any type
            (ty, LogicalType::Any) => match &self.subst {
                // if we already have a substituion (ANY -> ty) and we now require a different one, fail
                Some(subst) if subst != ty => Err(ZipError),
                _ => {
                    self.subst = Some(ty.clone());
                    Ok(())
                }
            },
            // any other new recursive cases need to be added here
            (LogicalType::Array(a), LogicalType::Array(b)) => self.zip_tys(a, b),
            (t, u) if t == u => Ok(()),
            _ => Err(ZipError),
        }
    }
}

impl TypeFolder for Unifier {
    type Error = std::convert::Infallible;

    fn fold_ty(&mut self, ty: LogicalType) -> Result<LogicalType, Self::Error> {
        match ty {
            LogicalType::Any => Ok(self.subst.clone().expect("there was an ANY type present ")),
            _ => ty.fold_with(self),
        }
    }
}
