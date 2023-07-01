mod range;

use nsql_core::Oid;
use nsql_storage::value::Value;

use super::*;

pub(crate) fn get_scalar_function(oid: Oid<Function>) -> Option<ScalarFunction> {
    Some(match oid {
        Function::RANGE2 => |mut args| {
            assert_eq!(args.len(), 2);
            let start: i32 = args[0].take().cast_non_null().unwrap();
            let end: i32 = args[1].take().cast_non_null().unwrap();
            Value::Array((start..end).map(Value::Int32).collect())
        },
        _ => return None,
    })
}

pub(crate) fn get_aggregate_function(oid: Oid<Function>) -> Option<AggregateFunction> {
    Some(match oid {
        Function::SUM_INT => AggregateFunction {
            state: Value::Int32(0),
            update: |state, next| match next {
                Value::Int32(n) => Value::Int32(state.cast_non_null::<i32>().unwrap() + n),
                _ => panic!(),
            },
        },
        Function::PRODUCT_INT => AggregateFunction {
            state: Value::Int32(1),
            update: |state, next| match next {
                Value::Int32(n) => Value::Int32(state.cast_non_null::<i32>().unwrap() * n),
                _ => panic!(),
            },
        },
        _ => return None,
    })
}
