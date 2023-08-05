use std::fmt;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU8, Ordering};

pub trait AtomicEnumType: Copy + From<u8> + Into<u8> {}

impl<T> AtomicEnumType for T where T: Copy + From<u8> + Into<u8> {}

pub struct AtomicEnum<T> {
    inner: AtomicU8,
    _marker: PhantomData<T>,
}

impl<T: fmt::Debug + AtomicEnumType> fmt::Debug for AtomicEnum<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.load(Ordering::Relaxed))
    }
}

impl<T: AtomicEnumType + Default> Default for AtomicEnum<T> {
    #[inline]
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: AtomicEnumType> AtomicEnum<T> {
    #[inline]
    pub fn new(value: T) -> Self {
        Self { inner: AtomicU8::new(value.into()), _marker: PhantomData }
    }

    pub fn into_inner(self) -> T {
        self.inner.into_inner().into()
    }

    #[inline]
    pub fn load(&self, order: Ordering) -> T {
        self.inner.load(order).into()
    }

    #[inline]
    pub fn store(&self, value: T, order: Ordering) {
        self.inner.store(value.into(), order);
    }
}
