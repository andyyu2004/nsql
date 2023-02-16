use std::error::Error;
use std::io;

pub type Result<T, E> = std::result::Result<T, Report<E>>;

#[repr(transparent)]
#[derive(Debug)]
pub struct Report<C>(error_stack::Report<C>);

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

