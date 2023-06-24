mod range;

use nsql_core::Oid;
use nsql_storage::value::Value;

use crate::Function;

type FunctionType = fn(Box<[Value]>) -> Value;

pub(crate) fn get_builtin(oid: Oid<Function>) -> Option<FunctionType> {
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
