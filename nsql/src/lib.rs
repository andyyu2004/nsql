use std::io;
use std::path::Path;

pub struct Nsql {}

impl Nsql {
    pub fn open(_path: impl AsRef<Path>) -> io::Result<Self> {
        Ok(Self {})
    }
}

