use nsql_core::Oid;
use nsql_storage::value::{Decimal, Value};

use super::*;

macro_rules! cast {
    ($to:ty) => {
        |mut args| {
            assert_eq!(args.len(), 2);
            let casted: Option<$to> = args[0].take().cast().unwrap();
            let dummy: Option<$to> = args[0].take().cast().unwrap();
            assert!(dummy.is_none(), "non-null value was passed as dummy cast argument");
            Value::from(casted)
        }
    };
}

macro_rules! comparison {
    ($op:tt: $ty:ty) => {
        |mut args| {
            assert_eq!(args.len(), 2);
            let a: Option<$ty> = args[0].take().cast().unwrap();
            let b: Option<$ty> = args[1].take().cast().unwrap();
            match (a, b) {
                (Some(a), Some(b)) => Value::Bool(a $op b),
                _  => Value::Null,
            }
        }
    };
}

macro_rules! infix_op {
    ($op:tt: $ty:ty) => {
        |mut args| {
            assert_eq!(args.len(), 2);
            let a: Option<$ty> = args[0].take().cast().unwrap();
            let b: Option<$ty> = args[1].take().cast().unwrap();
            match (a, b) {
                (Some(a), Some(b)) => Value::from(a $op b),
                _  => Value::Null,
            }
        }
    };
}

macro_rules! prefix_op {
    ($op:tt: $ty:ty) => {
        |mut args| {
            assert_eq!(args.len(), 1);
            let x: Option<$ty> = args[0].take().cast().unwrap();
            match x {
                Some(x) => Value::from($op x),
                _  => Value::Null,
            }
        }
    };
}

#[rustfmt::skip]
pub(crate) fn get_scalar_function(oid: Oid<Function>) -> Option<ScalarFunction> {
    Some(match oid {
        Function::RANGE2 => |mut args| {
            assert_eq!(args.len(), 2);
            let start: Option<i64> = args[0].take().cast().unwrap();
            let end: Option<i64> = args[1].take().cast().unwrap();
            match (start, end) {
                (Some(start), Some(end)) => Value::Array((start..end).map(Value::Int64).collect()),
                _ => Value::Null,
            }
        },
        Function::NEG_INT   => prefix_op!(- : i64),
        Function::NOT_BOOL  => prefix_op!(! : bool),
        Function::ADD_INT   => infix_op!(+ : i64),
        Function::EQ        => comparison!(== : Value),
        Function::LT        => comparison!(<  : Value),
        Function::LTE       => comparison!(<= : Value),
        Function::GTE       => comparison!(>= : Value),
        Function::GT        => comparison!(>  : Value),
        Function::CAST_INT_TO_DEC => cast!(Decimal),
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
                Value::Null => return Value::Null,
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
