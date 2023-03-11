mod reader;
mod writer;

use nsql_util::static_assert_eq;
pub use reader::MetaPageReader;
pub use writer::MetaPageWriter;

use crate::PageIndex;

macro_rules! try_io {
    ($e:expr) => {
        $e.map_err(|err| std::io::Error::new(err.current_context().kind(), err))?
    };
}

use try_io;

const PAGE_IDX_SIZE: usize = std::mem::size_of::<PageIndex>();
// if this changes, then we have to change the reader and writer to expect a different
// number of bytes to represent the next block pointer
static_assert_eq!(PAGE_IDX_SIZE, 4);

#[cfg(test)]
mod tests;
