mod aggregates;

use nsql_core::Oid;
use nsql_storage::value::{Decimal, Value};
use nsql_storage_engine::ReadWriteExecutionMode;

use super::*;
use crate::{Sequence, SequenceData};

macro_rules! cast_to {
    ($to:ty) => {
        |_catalog, _tx, mut args| {
            assert_eq!(args.len(), 2);
            // ensure the necessary `FromValue` cases are there for this cast to succeed
            // in particular, the rust level `to` type's `FromValue` impl needs a case for the `from` type.
            let casted: Option<$to> = args[0].take().cast().unwrap();
            let dummy: Option<$to> = args[1].take().cast().unwrap();
            assert!(dummy.is_none(), "non-null value was passed as dummy cast argument");
            Ok(Value::from(casted))
        }
    };
}

macro_rules! comparison {
    ($op:tt: $ty:ty) => {
        |_catalog, _tx, mut args| {
            assert_eq!(args.len(), 2);
            let a = args[0].take();
            let b = args[1].take();
            debug_assert!(a.is_compat_with(&b), "cannot compare `{a}` and `{b}` (this should have been a type error)");
            let a: Option<$ty> = a.cast().unwrap();
            let b: Option<$ty> = b.cast().unwrap();
            match (a, b) {
                (Some(a), Some(b)) => {
                    Ok(Value::Bool(a $op b))
                }
                _  => Ok(Value::Null),
            }
        }
    };
}

macro_rules! method {
    ($method:ident: $ty:ty) => {
        |_catalog, _tx, mut args| {
            assert_eq!(args.len(), 1);
            let x: Option<$ty> = args[0].take().cast().unwrap();
            match x {
                Some(x) => Ok(Value::from(x.$method())),
                _ => Ok(Value::Null),
            }
        }
    };
}

macro_rules! comparison_include_null {
    ($op:tt: $ty:ty) => {
        |_catalog, _tx, mut args| {
            assert_eq!(args.len(), 2);
            let a = args[0].take();
            let b = args[1].take();
            debug_assert!(a.is_compat_with(&b), "cannot compare `{a}` and `{b}` (this should have been a type error)");
            let a: Option<$ty> = a.cast().unwrap();
            let b: Option<$ty> = b.cast().unwrap();
            Ok(Value::Bool(a $op b))
        }
    };
}

macro_rules! arbitary_binary_op {
    ($ty:ty, |$x:ident, $y:ident| $expr:expr) => {
        |_catalog, _tx, mut args| {
            assert_eq!(args.len(), 2);
            let a = args[0].take();
            let b = args[1].take();
            debug_assert!(
                a.is_compat_with(&b),
                "cannot perform binary operation with `{a}` and `{b}` (this should have been a type error)"
            );

            let a: Option<$ty> = a.cast().unwrap();
            let b: Option<$ty> = b.cast().unwrap();
            let f = |$x, $y| $expr;
            Ok(f(a, b))
        }
    };
}

macro_rules! infix_op {
    ($op:tt: $ty:ty) => {
        |_catalog, _tx, mut args| {
            assert_eq!(args.len(), 2);
            let a: Option<$ty> = args[0].take().cast().unwrap();
            let b: Option<$ty> = args[1].take().cast().unwrap();
            match (a, b) {
                (Some(a), Some(b)) => Ok(Value::from(a $op b)),
                _  => Ok(Value::Null),
            }
        }
    };
}

macro_rules! prefix_op {
    ($op:tt: $ty:ty) => {
        |_catalog, _tx, mut args| {
            assert_eq!(args.len(), 1);
            let x: Option<$ty> = args[0].take().cast().unwrap();
            match x {
                Some(x) => Ok(Value::from($op x)),
                _  => Ok(Value::Null),
            }
        }
    };
}

#[rustfmt::skip]
pub(crate) fn get_scalar_function<S: StorageEngine>(oid: Oid<Function>) -> Option<ScalarFunction<S>> {
    Some(match oid {
        _ if oid == Function::NEG_INT     => prefix_op!(- : i64),
        _ if oid == Function::NEG_FLOAT   => prefix_op!(- : f64),
        _ if oid == Function::NEG_DEC     => prefix_op!(- : Decimal),
        _ if oid == Function::NOT_BOOL    => prefix_op!(! : bool),
        _ if oid == Function::ADD_INT     => infix_op!(+ : i64),
        _ if oid == Function::ADD_FLOAT   => infix_op!(+ : f64),
        _ if oid == Function::ADD_DEC     => infix_op!(+ : Decimal),
        _ if oid == Function::SUB_INT     => infix_op!(- : i64),
        _ if oid == Function::SUB_FLOAT   => infix_op!(- : f64),
        _ if oid == Function::SUB_DEC     => infix_op!(- : Decimal),
        _ if oid == Function::MUL_INT     => infix_op!(* : i64),
        _ if oid == Function::MUL_FLOAT   => infix_op!(* : f64),
        _ if oid == Function::MUL_DEC     => infix_op!(* : Decimal),
        _ if oid == Function::DIV_INT     => infix_op!(/ : i64),
        _ if oid == Function::DIV_FLOAT   => infix_op!(/ : f64),
        _ if oid == Function::DIV_DEC     => infix_op!(/ : Decimal),
        _ if oid == Function::BETWEEN_ANY => between,
        _ if oid == Function::EQ_ANY      => comparison!(== : Value),
        _ if oid == Function::NEQ_ANY     => comparison!(!= : Value),
        _ if oid == Function::LT_ANY      => comparison!(<  : Value),
        _ if oid == Function::LTE_ANY     => comparison!(<= : Value),
        _ if oid == Function::GTE_ANY     => comparison!(>= : Value),
        _ if oid == Function::GT_ANY      => comparison!(>  : Value),
        _ if oid == Function::OR_BOOL     => arbitary_binary_op!(bool, |a, b| match (a, b) {
            (Some(a), Some(b)) => Value::Bool(a || b),
            (Some(true), _) | (_, Some(true)) => Value::Bool(true),
            _ => Value::Null,
        }),
        _ if oid == Function::AND_BOOL    => arbitary_binary_op!(bool, |a, b| match (a, b) {
            (Some(a), Some(b)) => Value::Bool(a && b),
            (Some(false), _) | (_, Some(false)) => Value::Bool(false),
            _ => Value::Null,
        }),
        _ if oid == Function::IS_DISTINCT_FROM_ANY => comparison_include_null!(!= : Value),
        _ if oid == Function::IS_NOT_DISTINCT_FROM_ANY => comparison_include_null!(== : Value),
        _ if oid == Function::ABS_INT   => method!(abs: i64),
        _ if oid == Function::ABS_FLOAT => method!(abs: f64),
        _ if oid == Function::ABS_DEC   => method!(abs: Decimal),
        // casts
        _ if oid == Function::CAST_SELF         => cast_to!(Value),
        _ if oid == Function::CAST_INT_TO_DEC   => cast_to!(Decimal),
        _ if oid == Function::CAST_INT_TO_FLOAT => cast_to!(f64),
        _ if oid == Function::CAST_INT_TO_OID   => cast_to!(UntypedOid),
        _ if oid == Function::NEXTVAL => nextval,
        _ if oid == Function::NEXTVAL_OID => nextval_oid,
        _ if oid == Function::MK_NEXTVAL_EXPR => mk_nextval_expr,
        // misc
        _ if oid == Function::RANGE2 => range2,
        _ if oid == Function::ARRAY_ELEMENT => array_element,
        _ if oid == Function::ARRAY_POSITION => array_position,
        _ if oid == Function::ARRAY_CONTAINS => array_contains,

        _ => return None,
    })
}

#[allow(clippy::boxed_local)]
fn between<'env, S: StorageEngine>(
    _catalog: Catalog<'env, S>,
    _tx: &dyn Transaction<'env, S>,
    mut args: Box<[Value]>,
) -> Result<Value> {
    assert_eq!(args.len(), 3);
    let target = args[0].take();
    let lower = args[1].take();
    let upper = args[2].take();
    debug_assert!(
        target.is_compat_with(&lower)
            && target.is_compat_with(&upper)
            && lower.is_compat_with(&upper),
        "cannot compare `{target}` and `{lower}` and `{upper}` (this should have been a type error)"
    );

    let target: Option<Value> = target.cast().unwrap();
    let lower: Option<Value> = lower.cast().unwrap();
    let upper: Option<Value> = upper.cast().unwrap();
    match (lower, target, upper) {
        (Some(lower), Some(target), _) if target < lower => Ok(Value::Bool(false)),
        (_, Some(target), Some(upper)) if target > upper => Ok(Value::Bool(false)),
        (Some(lower), Some(target), Some(upper)) => {
            Ok(Value::Bool(lower <= target && target <= upper))
        }
        _ => Ok(Value::Null),
    }
}

pub(crate) fn get_aggregate_function(
    oid: Oid<Function>,
) -> Option<Box<dyn AggregateFunctionInstance>> {
    use self::aggregates::*;
    Some(match oid {
        _ if oid == Function::SUM_INT => Box::<SumInt>::default(),
        _ if oid == Function::PRODUCT_INT => Box::<ProductInt>::default(),
        _ if oid == Function::AVG_INT => Box::<AverageInt>::default(),
        _ if oid == Function::FIRST => Box::<First>::default(),
        _ if oid == Function::COUNT => Box::<Count>::default(),
        _ if oid == Function::COUNT_STAR => Box::<CountStar>::default(),
        _ if oid == Function::MIN_ANY => Box::<Min>::default(),
        _ if oid == Function::MAX_ANY => Box::<Max>::default(),
        _ => return None,
    })
}

#[allow(clippy::boxed_local)]
fn range2<'env, S: StorageEngine>(
    _catalog: Catalog<'env, S>,
    _tx: &dyn Transaction<'env, S>,
    mut args: Box<[Value]>,
) -> Result<Value> {
    assert_eq!(args.len(), 2);
    let start: Option<i64> = args[0].take().cast().unwrap();
    let end: Option<i64> = args[1].take().cast().unwrap();
    match (start, end) {
        (Some(start), Some(end)) => Ok(Value::Array((start..end).map(Value::Int64).collect())),
        _ => Ok(Value::Null),
    }
}

#[allow(clippy::boxed_local)]
fn array_element<'env, S: StorageEngine>(
    _catalog: Catalog<'env, S>,
    _tx: &dyn Transaction<'env, S>,
    mut args: Box<[Value]>,
) -> Result<Value> {
    assert_eq!(args.len(), 2);
    let array = match args[0].take() {
        Value::Array(xs) => xs,
        _ => panic!("expected array"),
    };

    // one-indexed
    let index: Option<i64> = args[1].take().cast().unwrap();
    match index {
        None => Ok(Value::Null),
        Some(index) => {
            if index <= 0 || index as usize > array.len() {
                return Ok(Value::Null);
            }

            Ok(array[index as usize - 1].clone())
        }
    }
}

#[allow(clippy::boxed_local)]
fn array_position<'env, S: StorageEngine>(
    _catalog: Catalog<'env, S>,
    _tx: &dyn Transaction<'env, S>,
    mut args: Box<[Value]>,
) -> Result<Value> {
    assert_eq!(args.len(), 2);
    let array = match args[0].take() {
        Value::Array(xs) => xs,
        Value::Null => return Ok(Value::Null),
        _ => panic!("expected array"),
    };

    let target = args[1].take();
    match array.iter().position(|v| v.is_not_null() && v == &target) {
        // one-indexed
        Some(index) => Ok(Value::Int64(index as i64 + 1)),
        None => Ok(Value::Null),
    }
}

#[allow(clippy::boxed_local)]
fn array_contains<'env, S: StorageEngine>(
    _catalog: Catalog<'env, S>,
    _tx: &dyn Transaction<'env, S>,
    mut args: Box<[Value]>,
) -> Result<Value> {
    assert_eq!(args.len(), 2);
    let array = match args[0].take() {
        Value::Array(xs) => xs,
        Value::Null => return Ok(Value::Null),
        _ => panic!("expected array"),
    };

    let target = match args[1].take() {
        Value::Null => return Ok(Value::Null),
        target => target,
    };

    Ok(array.iter().any(|v| v.is_not_null() && v == &target).into())
}

#[allow(clippy::boxed_local)]
fn nextval<'env, S: StorageEngine>(
    catalog: Catalog<'env, S>,
    tx: &dyn Transaction<'env, S>,
    mut args: Box<[Value]>,
) -> Result<Value> {
    assert_eq!(args.len(), 1);
    let tx = tx.try_as_write().expect("nextval should be passed a write transaction");
    let oid: Oid<Table> = args[0].take().cast().unwrap();
    let sequence = catalog.system_table::<Sequence>(tx)?.get(oid)?;
    let seq_table = catalog.system_table_write::<Table>(tx)?.get(oid)?;
    let mut storage = seq_table.storage::<S, ReadWriteExecutionMode>(catalog, tx)?;

    let current = match storage.get(Value::from(SequenceData::KEY))? {
        Some(seq) => {
            let current = SequenceData::from_tuple(seq)?.value;
            storage.update(&SequenceData::new(current + sequence.step).into_tuple())?;
            current
        }
        None => {
            let current = sequence.start;
            storage
                .insert(&catalog, tx, &SequenceData::new(current + sequence.step).into_tuple())?
                .expect("insert shouldn't conflict");
            current
        }
    };

    Ok(Value::Int64(current))
}

fn nextval_oid<'env, S: StorageEngine>(
    catalog: Catalog<'env, S>,
    tx: &dyn Transaction<'env, S>,
    args: Box<[Value]>,
) -> Result<Value> {
    let next = nextval(catalog, tx, args)?;
    Ok(Value::Oid(next.cast().unwrap()))
}

/// A function that returns an expression that evaluates to the next value of the given sequence.
/// This is used to create the `default_expr` value for a column with a generated identity.
#[allow(clippy::boxed_local)]
fn mk_nextval_expr<'env, S: StorageEngine>(
    _catalog: Catalog<'env, S>,
    _tx: &dyn Transaction<'env, S>,
    mut args: Box<[Value]>,
) -> Result<Value> {
    let oid: UntypedOid = args[0].take().cast().unwrap();
    Ok(Value::Expr(Expr::call(Function::NEXTVAL.untyped(), [oid.into()])))
}
