use super::*;

#[derive(Debug, Default)]
pub(super) struct SumInt {
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
pub(super) struct ProductInt {
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
pub(super) struct AverageInt {
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
pub(super) struct First {
    value: Value,
}

impl AggregateFunctionInstance for First {
    #[inline]
    fn update(&mut self, value: Option<Value>) {
        match value.expect("first should be passed an argument") {
            v if self.value.is_null() => self.value = v,
            _ => (),
        }
    }

    #[inline]
    fn finalize(self: Box<Self>) -> Value {
        self.value
    }
}

#[derive(Debug, Default)]
pub(super) struct Count {
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
pub(super) struct CountStar {
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

#[derive(Debug, Default)]
pub(super) struct Min {
    value: Value,
}

impl AggregateFunctionInstance for Min {
    #[inline]
    fn update(&mut self, value: Option<Value>) {
        self.value = match (self.value.take(), value.expect("min should be passed an argument")) {
            (current, Value::Null) => current,
            (Value::Null, v) => v,
            (current, v) => current.min(v),
        }
    }

    #[inline]
    fn finalize(self: Box<Self>) -> Value {
        self.value
    }
}

#[derive(Debug, Default)]
pub(super) struct Max {
    value: Value,
}

impl AggregateFunctionInstance for Max {
    #[inline]
    fn update(&mut self, value: Option<Value>) {
        self.value = self.value.take().max(value.expect("max should be passed an argument"))
    }

    #[inline]
    fn finalize(self: Box<Self>) -> Value {
        self.value
    }
}
