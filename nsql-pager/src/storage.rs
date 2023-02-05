use std::fs::File;
pub(crate) use std::os::unix::prelude::FileExt;

pub trait Storage: FileExt {}

impl Storage for File {}
