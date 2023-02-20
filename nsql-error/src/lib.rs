use std::error::Error;
use std::fmt::Display;
use std::io;
use std::ops::Deref;

pub type Result<T, E> = std::result::Result<T, Report<E>>;

#[repr(transparent)]
#[derive(Debug)]
pub struct Report<C>(error_stack::Report<C>);

impl<C> Display for Report<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<C> Deref for Report<C> {
    type Target = error_stack::Report<C>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<E: Error + Send + Sync + 'static> From<E> for Report<E> {
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

impl From<Report<io::Error>> for io::Error {
    fn from(Report(err): Report<io::Error>) -> Self {
        io::Error::new(err.current_context().kind(), err.into_error())
    }
}
