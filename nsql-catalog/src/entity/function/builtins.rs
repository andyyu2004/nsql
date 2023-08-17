use nsql_core::Oid;
use nsql_storage::value::{Decimal, Value};

use super::*;

macro_rules! comparison {
    ($op:tt: $ty:ty) => {
        |mut args| {
            assert_eq!(args.len(), 2);
            // FIXME need to handle nulls
            let a: $ty = args[0].take().cast_non_null().unwrap();
            let b: $ty = args[1].take().cast_non_null().unwrap();
            Value::Bool(a $op b)
        }
    };

}

pub(crate) fn get_scalar_function(oid: Oid<Function>) -> Option<ScalarFunction> {
    Some(match oid {
        Function::RANGE2 => |mut args| {
            assert_eq!(args.len(), 2);
            let start: i64 = args[0].take().cast_non_null().unwrap();
            let end: i64 = args[1].take().cast_non_null().unwrap();
            Value::Array((start..end).map(Value::Int64).collect())
        },
        Function::GT_INT => comparison!(> : i64),
        Function::GT_FLOAT => comparison!(> : f64),
        Function::GT_DEC => comparison!(> : Decimal),
        Function::ARRAY_ELEMENT => |mut args| {
            assert_eq!(args.len(), 2);
            let array = match args[0].take() {
                Value::Array(xs) => xs,
                _ => panic!("expected array"),
            };

            // one-indexed
            let index: Option<i64> = args[1].take().cast().unwrap();
            match index {
                None => Value::Null,
                Some(index) => {
                    if index <= 0 || index as usize > array.len() {
                        return Value::Null;
                    }

                    array[index as usize - 1].clone()
                }
            }
        },
        Function::ARRAY_POSITION => |mut args| {
            assert_eq!(args.len(), 2);
            let array = match args[0].take() {
                Value::Array(xs) => xs,
                _ => panic!("expected array"),
            };

            // one-indexed
            let target = args[1].take();
            match array.iter().position(|v| v == &target) {
                Some(index) => Value::Int64(index as i64 + 1),
                None => Value::Null,
            }
        },
        _ => return None,
    })
}

pub(crate) fn get_aggregate_function(
    oid: Oid<Function>,
) -> Option<Box<dyn AggregateFunctionInstance>> {
    Some(match oid {
        Function::SUM_INT => Box::<SumInt>::default(),
        Function::PRODUCT_INT => Box::<ProductInt>::default(),
        Function::AVG_INT => Box::<AverageInt>::default(),
        _ => return None,
    })
}

#[derive(Debug, Default)]
struct SumInt {
    state: i64,
}

impl AggregateFunctionInstance for SumInt {
    #[inline]
    fn update(&mut self, value: Value) {
        match value {
            Value::Int64(n) => self.state += n,
            _ => panic!(),
        }
    }

    #[inline]
    fn finalize(self: Box<Self>) -> Value {
        Value::Int64(self.state)
    }
}

#[derive(Debug)]
struct ProductInt {
    state: i64,
}

impl Default for ProductInt {
    fn default() -> Self {
        Self { state: 1 }
    }
}

impl AggregateFunctionInstance for ProductInt {
    #[inline]
    fn update(&mut self, value: Value) {
        match value {
            Value::Int64(i) => self.state *= i,
            _ => panic!(),
        }
    }

    #[inline]
    fn finalize(self: Box<Self>) -> Value {
        Value::Int64(self.state)
    }
}

#[derive(Debug, Default)]
struct AverageInt {
    value: i64,
    count: i64,
}

impl AggregateFunctionInstance for AverageInt {
    #[inline]
    fn update(&mut self, value: Value) {
        match value {
            Value::Int64(n) => {
                self.value += n;
                self.count += 1;
            }
            _ => panic!(),
        }
    }

    #[inline]
    fn finalize(self: Box<Self>) -> Value {
        if self.count == 0 {
            return Value::Null;
        }

        let f = self.value as f64 / self.count as f64;
        Value::Float64(f.to_bits())
    }
}
