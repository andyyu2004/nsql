use nsql_core::Oid;
use nsql_storage::value::{Decimal, Value};

use super::*;

macro_rules! cast {
    ($to:ty) => {
        |mut args| {
            assert_eq!(args.len(), 2);
            // ensure the necessary `FromValue` cases are there for this cast to succeed
            // in particular, the rust level `to` types `FromValue` impl needs a case for the `from` type.
            let casted: Option<$to> = args[0].take().cast().unwrap();
            let dummy: Option<$to> = args[1].take().cast().unwrap();
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
        Function::NEG_INT   => prefix_op!(- : i64),
        Function::NOT_BOOL  => prefix_op!(! : bool),
        Function::ADD_INT   => infix_op!(+ : i64),
        Function::ADD_FLOAT => infix_op!(+ : f64),
        Function::ADD_DEC   => infix_op!(+ : Decimal),
        Function::SUB_INT   => infix_op!(- : i64),
        Function::SUB_FLOAT => infix_op!(- : f64),
        Function::SUB_DEC   => infix_op!(- : Decimal),
        Function::MUL_INT   => infix_op!(* : i64),
        Function::MUL_FLOAT => infix_op!(* : f64),
        Function::MUL_DEC   => infix_op!(* : Decimal),
        Function::DIV_INT   => infix_op!(/ : i64),
        Function::DIV_FLOAT => infix_op!(/ : f64),
        Function::DIV_DEC   => infix_op!(/ : Decimal),
        Function::EQ        => comparison!(== : Value),
        Function::LT        => comparison!(<  : Value),
        Function::LTE       => comparison!(<= : Value),
        Function::GTE       => comparison!(>= : Value),
        Function::GT        => comparison!(>  : Value),
        Function::OR_BOOL   => comparison!(|| : bool),
        Function::AND_BOOL  => comparison!(&& : bool),
        // casts
        Function::CAST_SELF         => cast!(Value),
        Function::CAST_INT_TO_DEC   => cast!(Decimal),
        Function::CAST_INT_TO_FLOAT => cast!(f64),
        // misc
        Function::RANGE2 => |mut args| {
            assert_eq!(args.len(), 2);
            let start: Option<i64> = args[0].take().cast().unwrap();
            let end: Option<i64> = args[1].take().cast().unwrap();
            match (start, end) {
                (Some(start), Some(end)) => Value::Array((start..end).map(Value::Int64).collect()),
                _ => Value::Null,
            }
        },
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
        Function::COUNT => Box::<Count>::default(),
        Function::COUNT_STAR => Box::<CountStar>::default(),
        _ => return None,
    })
}

#[derive(Debug, Default)]
struct SumInt {
    state: i64,
}

impl AggregateFunctionInstance for SumInt {
    #[inline]
    fn update(&mut self, value: Option<Value>) {
        match value.expect("sum should be passed an argument") {
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
    fn update(&mut self, value: Option<Value>) {
        match value.expect("product should be passed an argument") {
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
    fn update(&mut self, value: Option<Value>) {
        match value.expect("avg should be passed an argument") {
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

#[derive(Debug, Default)]
struct Count {
    count: usize,
}

impl AggregateFunctionInstance for Count {
    #[inline]
    fn update(&mut self, value: Option<Value>) {
        match value.expect("count should be passed an argument") {
            Value::Null => {}
            _ => self.count += 1,
        }
    }

    #[inline]
    fn finalize(self: Box<Self>) -> Value {
        Value::Int64(self.count as i64)
    }
}

#[derive(Debug, Default)]
struct CountStar {
    count: usize,
}

impl AggregateFunctionInstance for CountStar {
    #[inline]
    fn update(&mut self, value: Option<Value>) {
        debug_assert!(value.is_none(), "count(*) should not be passed an arg");
        self.count += 1
    }

    #[inline]
    fn finalize(self: Box<Self>) -> Value {
        Value::Int64(self.count as i64)
    }
}
