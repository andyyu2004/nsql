mod reader;
mod writer;

pub use reader::MetaPageReader;
pub use writer::MetaPageWriter;

macro_rules! try_io {
    ($e:expr) => {
        $e.map_err(|err| std::io::Error::new(err.current_context().kind(), err))?
    };
}

use try_io;

#[cfg(test)]
mod tests;
