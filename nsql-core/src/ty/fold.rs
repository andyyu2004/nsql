use super::*;

pub trait TypeFolder {
    type Error;

    fn fold_ty(&mut self, ty: LogicalType) -> Result<LogicalType, Self::Error>;
}

pub trait TypeFold: Sized {
    fn fold_with<F: TypeFolder>(self, folder: &mut F) -> Result<Self, F::Error>;

    fn super_fold_with<F: TypeFolder>(self, folder: &mut F) -> Result<Self, F::Error> {
        self.fold_with(folder)
    }
}

impl<T: TypeFold> TypeFold for Vec<T> {
    #[inline]
    fn fold_with<F: TypeFolder>(self, folder: &mut F) -> Result<Self, F::Error> {
        self.into_iter().map(|t| t.super_fold_with(folder)).collect()
    }
}

impl TypeFold for LogicalType {
    fn fold_with<F: TypeFolder>(self, folder: &mut F) -> Result<Self, F::Error> {
        match self {
            LogicalType::Null => Ok(LogicalType::Null),
            LogicalType::Byte => Ok(LogicalType::Byte),
            LogicalType::Bool => Ok(LogicalType::Bool),
            LogicalType::Int64 => Ok(LogicalType::Int64),
            LogicalType::Float64 => Ok(LogicalType::Float64),
            LogicalType::Decimal => Ok(LogicalType::Decimal),
            LogicalType::Text => Ok(LogicalType::Text),
            LogicalType::Oid => Ok(LogicalType::Oid),
            LogicalType::Bytea => Ok(LogicalType::Bytea),
            LogicalType::Type => Ok(LogicalType::Type),
            LogicalType::Expr => Ok(LogicalType::Expr),
            LogicalType::TupleExpr => Ok(LogicalType::TupleExpr),
            LogicalType::Any => Ok(LogicalType::Any),
            LogicalType::Array(elem) => {
                elem.super_fold_with(folder).map(Box::new).map(LogicalType::Array)
            }
        }
    }

    #[inline]
    fn super_fold_with<F: TypeFolder>(self, folder: &mut F) -> Result<Self, F::Error> {
        folder.fold_ty(self)
    }
}
